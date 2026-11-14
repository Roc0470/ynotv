use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::engine::AiSubtitleEngine;
use super::models::Pcm16kChunk;
use crate::audio_capture::PcmAudioPacket;

const PCM_FRAME_BYTES: usize = 3_200;
const PCM_FRAME_SAMPLES: usize = 1_600;
const FRAME_DURATION_MS: u32 = 100;
const SAMPLE_RATE_HZ: u32 = 16_000;
const MIN_SEGMENT_MS: u32 = 2_500;
const MAX_SEGMENT_MS: u32 = 3_500;
const VAD_RMS_THRESHOLD: f32 = 0.008;

pub type SharedSubtitleEngine = Arc<Mutex<Box<dyn AiSubtitleEngine>>>;

pub struct StandaloneScout {
    pub first_frame: oneshot::Receiver<Result<(), String>>,
    pub task: JoinHandle<Result<(), String>>,
}

/// Resolves the proxy FFmpeg should tunnel through.
///
/// mpv is started with `--http-proxy=$ALL_PROXY`, so playback may depend on that
/// proxy. FFmpeg never reads `ALL_PROXY`, and its HTTP protocol only speaks HTTP
/// CONNECT, so SOCKS URLs are mapped onto the same host/port (Clash/mihomo expose
/// both protocols on one mixed port). Without this the scout connects directly and
/// providers that only serve proxied IPs answer 403 Forbidden.
fn proxy_for_ffmpeg() -> Option<String> {
    const NAMES: [&str; 6] = [
        "https_proxy",
        "HTTPS_PROXY",
        "http_proxy",
        "HTTP_PROXY",
        "all_proxy",
        "ALL_PROXY",
    ];
    let raw = NAMES
        .iter()
        .find_map(|name| std::env::var(name).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())?;

    for prefix in ["socks5h://", "socks5://", "socks4://"] {
        if let Some(rest) = raw.strip_prefix(prefix) {
            return Some(format!("http://{rest}"));
        }
    }
    Some(raw)
}

pub fn start_standalone(
    stream_url: String,
    headers: BTreeMap<String, String>,
    engine: SharedSubtitleEngine,
    decoded_audio_ms: Arc<AtomicU64>,
    cancellation: CancellationToken,
) -> Result<StandaloneScout, String> {
    if stream_url.trim().is_empty() {
        return Err("直播流地址不能为空".to_string());
    }

    let ffmpeg_path = crate::stream_probe::find_ffmpeg()
        .ok_or_else(|| "找不到 FFmpeg，请确认应用目录中的 FFmpeg sidecar 可用".to_string())?;
    let mut command = Command::new(ffmpeg_path);
    command
        .arg("-hide_banner")
        .arg("-nostdin")
        .arg("-loglevel")
        .arg("warning")
        .arg("-reconnect")
        .arg("1")
        .arg("-reconnect_streamed")
        .arg("1")
        .arg("-reconnect_delay_max")
        .arg("5")
        // Segment URLs on this provider have no file extension
        // (`.../manifest/<hash>/0/36114812`), which FFmpeg's HLS demuxer rejects
        // by default with "not in allowed_segment_extensions". mpv's bundled
        // FFmpeg allows it, which is why playback works but the scout does not.
        .arg("-extension_picky")
        .arg("0");

    let mut user_agent = None;
    let mut header_lines = Vec::new();
    for (name, value) in headers {
        if name.contains(['\r', '\n']) || value.contains(['\r', '\n']) {
            return Err("HTTP 请求头不能包含换行符".to_string());
        }
        // FFmpeg always emits its own `User-Agent` line; passing another one through
        // `-headers` makes it send the header twice, which some CDNs answer with 403.
        if name.eq_ignore_ascii_case("user-agent") {
            user_agent = Some(value);
            continue;
        }
        header_lines.push(format!("{name}: {value}\r\n"));
    }
    if let Some(user_agent) = user_agent.as_deref().map(str::trim).filter(|ua| !ua.is_empty()) {
        command.arg("-user_agent").arg(user_agent);
        log::warn!("[AiAudioScout/FFmpeg] Scout User-Agent: {user_agent}");
    }
    // mpv is launched with `--http-proxy=$ALL_PROXY`, but FFmpeg only understands
    // `http_proxy`/`https_proxy`. Without this, the scout connects directly while the
    // player goes through the proxy, and providers that only serve proxied IPs answer 403.
    if let Some(proxy) = proxy_for_ffmpeg() {
        command.arg("-http_proxy").arg(&proxy);
        log::warn!("[AiAudioScout/FFmpeg] Scout proxy: {proxy}");
    }
    if !header_lines.is_empty() {
        command.arg("-headers").arg(header_lines.concat());
    }

    command
        .arg("-i")
        .arg(stream_url)
        .arg("-vn")
        .arg("-sn")
        .arg("-dn")
        .arg("-ac")
        .arg("1")
        .arg("-ar")
        .arg(SAMPLE_RATE_HZ.to_string())
        .arg("-f")
        .arg("s16le")
        .arg("-acodec")
        .arg("pcm_s16le")
        .arg("pipe:1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);

    #[cfg(target_os = "windows")]
    {
        command.creation_flags(0x0800_0000);
    }

    let mut child = command
        .spawn()
        .map_err(|error| format!("启动 FFmpeg 音频探路器失败：{error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "无法读取 FFmpeg 音频输出".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "无法读取 FFmpeg 错误输出".to_string())?;
    let stderr_error = Arc::new(std::sync::Mutex::new(None));
    let stderr_error_reader = Arc::clone(&stderr_error);
    let stderr_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        // The scout only runs for a few seconds before it either produces audio or is
        // abandoned, so keep the first lines at warn level: debug output is filtered out
        // of the app log and a silent 8s timeout is impossible to diagnose without them.
        let mut logged_lines = 0_u32;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.contains("403") || line.to_ascii_lowercase().contains("forbidden") {
                if let Ok(mut error) = stderr_error_reader.lock() {
                    *error = Some(format!("FFmpeg 连接直播流被拒绝：{line}"));
                }
            }
            if logged_lines < 40 {
                logged_lines += 1;
                log::warn!("[AiAudioScout/FFmpeg] {line}");
            } else {
                log::debug!("[AiAudioScout/FFmpeg] {line}");
            }
        }
    });

    let (first_frame_sender, first_frame) = oneshot::channel();
    let task = tokio::spawn(run_standalone_process(
        child,
        stdout,
        engine,
        decoded_audio_ms,
        cancellation,
        first_frame_sender,
        stderr_error,
        stderr_task,
    ));

    Ok(StandaloneScout { first_frame, task })
}

pub fn start_direct_tap(
    receiver: mpsc::Receiver<PcmAudioPacket>,
    engine: SharedSubtitleEngine,
    decoded_audio_ms: Arc<AtomicU64>,
    start_pts_ms: u64,
    cancellation: CancellationToken,
) -> (
    oneshot::Receiver<Result<(), String>>,
    JoinHandle<Result<(), String>>,
) {
    decoded_audio_ms.store(start_pts_ms, Ordering::SeqCst);
    let (first_frame_sender, first_frame) = oneshot::channel();
    let task = tokio::spawn(run_direct_tap(
        receiver,
        engine,
        decoded_audio_ms,
        cancellation,
        first_frame_sender,
    ));
    (first_frame, task)
}

struct EnginePipeline {
    engine: SharedSubtitleEngine,
    wants_audio_frames: bool,
    segment_sender: mpsc::Sender<Pcm16kChunk>,
    errors: mpsc::UnboundedReceiver<String>,
    worker: JoinHandle<()>,
}

impl EnginePipeline {
    async fn new(engine: SharedSubtitleEngine, cancellation: CancellationToken) -> Self {
        let wants_audio_frames = engine.lock().await.wants_audio_frames();
        let (segment_sender, mut segment_receiver) = mpsc::channel::<Pcm16kChunk>(8);
        let (error_sender, errors) = mpsc::unbounded_channel();
        let worker_engine = Arc::clone(&engine);
        let worker = tokio::spawn(async move {
            loop {
                let segment = tokio::select! {
                    _ = cancellation.cancelled() => break,
                    segment = segment_receiver.recv() => match segment {
                        Some(segment) => segment,
                        None => break,
                    }
                };
                let result = {
                    let mut engine = worker_engine.lock().await;
                    tokio::select! {
                        _ = cancellation.cancelled() => break,
                        result = engine.on_segment_pcm(segment) => result,
                    }
                };
                if let Err(error) = result {
                    let _ = error_sender.send(error);
                    break;
                }
            }
        });
        Self {
            engine,
            wants_audio_frames,
            segment_sender,
            errors,
            worker,
        }
    }

    async fn push_frame(
        &mut self,
        frame: Pcm16kChunk,
        assembler: &mut SegmentAssembler,
    ) -> Result<(), String> {
        if self.wants_audio_frames {
            self.engine.lock().await.on_frame_pcm(frame.clone()).await?;
        }
        if let Some(segment) = assembler.push(frame) {
            self.segment_sender
                .try_send(segment)
                .map_err(|error| format!("AI 字幕引擎处理速度落后于音频输入：{error}"))?;
        }
        if let Ok(error) = self.errors.try_recv() {
            return Err(error);
        }
        Ok(())
    }

    async fn close(self) {
        drop(self.segment_sender);
        self.worker.abort();
        let _ = self.worker.await;
    }
}

async fn run_standalone_process(
    mut child: Child,
    mut stdout: tokio::process::ChildStdout,
    engine: SharedSubtitleEngine,
    decoded_audio_ms: Arc<AtomicU64>,
    cancellation: CancellationToken,
    first_frame_sender: oneshot::Sender<Result<(), String>>,
    stderr_error: Arc<std::sync::Mutex<Option<String>>>,
    stderr_task: JoinHandle<()>,
) -> Result<(), String> {
    let mut first_frame_sender = Some(first_frame_sender);
    let mut assembler = SegmentAssembler::default();
    let mut pending_bytes = Vec::with_capacity(PCM_FRAME_BYTES * 3);
    let mut read_buffer = [0_u8; PCM_FRAME_BYTES * 2];
    let mut pipeline = EnginePipeline::new(Arc::clone(&engine), cancellation.clone()).await;

    let result = 'read: loop {
        tokio::select! {
            _ = cancellation.cancelled() => break Ok(()),
            error = pipeline.errors.recv() => {
                if let Some(error) = error {
                    break Err(error);
                } else {
                    break Err("AI 字幕引擎处理任务已意外结束".to_string());
                }
            }
            read_result = stdout.read(&mut read_buffer) => {
                let bytes_read = match read_result {
                    Ok(bytes_read) => bytes_read,
                    Err(error) => break Err(format!("读取 FFmpeg 音频失败：{error}")),
                };
                if bytes_read == 0 {
                    let error = stderr_error
                        .lock()
                        .ok()
                        .and_then(|error| error.clone())
                        .unwrap_or_else(|| "FFmpeg 音频流提前结束".to_string());
                    break Err(error);
                }
                pending_bytes.extend_from_slice(&read_buffer[..bytes_read]);
                while pending_bytes.len() >= PCM_FRAME_BYTES {
                    let frame_bytes: Vec<u8> = pending_bytes.drain(..PCM_FRAME_BYTES).collect();
                    let samples = frame_bytes
                        .chunks_exact(2)
                        .map(|pair| i16::from_le_bytes([pair[0], pair[1]]))
                        .collect::<Vec<_>>();
                    let frame = make_frame(samples, &decoded_audio_ms);
                    if let Some(sender) = first_frame_sender.take() {
                        let _ = sender.send(Ok(()));
                    }
                    if let Err(error) = pipeline.push_frame(frame, &mut assembler).await {
                        break 'read Err(error);
                    }
                }
            }
        }
        if cancellation.is_cancelled() {
            break Ok(());
        }
    };

    pipeline.close().await;
    if result.is_err() || cancellation.is_cancelled() {
        let _ = child.start_kill();
    }
    let wait_result = child
        .wait()
        .await
        .map_err(|error| format!("等待 FFmpeg 音频探路器退出失败：{error}"));
    let _ = stderr_task.await;

    if let Some(sender) = first_frame_sender {
        let message = result
            .as_ref()
            .err()
            .cloned()
            .unwrap_or_else(|| "FFmpeg 尚未产出首帧音频".to_string());
        let _ = sender.send(Err(message));
    }

    match (result, wait_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) if !cancellation.is_cancelled() => Err(error),
        (Ok(()), Ok(status)) if !status.success() && !cancellation.is_cancelled() => {
            Err(format!("FFmpeg 音频探路器退出码异常：{status}"))
        }
        (Ok(()), _) => Ok(()),
    }
}

async fn run_direct_tap(
    mut receiver: mpsc::Receiver<PcmAudioPacket>,
    engine: SharedSubtitleEngine,
    decoded_audio_ms: Arc<AtomicU64>,
    cancellation: CancellationToken,
    first_frame_sender: oneshot::Sender<Result<(), String>>,
) -> Result<(), String> {
    let mut first_frame_sender = Some(first_frame_sender);
    let mut resampler = LinearResampler::default();
    let mut pending_samples = Vec::with_capacity(PCM_FRAME_SAMPLES * 2);
    let mut assembler = SegmentAssembler::default();
    let mut pipeline = EnginePipeline::new(Arc::clone(&engine), cancellation.clone()).await;

    let result = 'capture: loop {
        let packet = tokio::select! {
            _ = cancellation.cancelled() => break Ok(()),
            error = pipeline.errors.recv() => {
                if let Some(error) = error {
                    break Err(error);
                } else {
                    break Err("AI 字幕引擎处理任务已意外结束".to_string());
                }
            }
            packet = receiver.recv() => match packet {
                Some(packet) => packet,
                None => break Err("WASAPI PCM 订阅已关闭".to_string()),
            },
        };
        let samples = match resampler.resample(&packet.mono_samples, packet.sample_rate_hz) {
            Ok(samples) => samples,
            Err(error) => break Err(error),
        };
        pending_samples.extend(samples);

        while pending_samples.len() >= PCM_FRAME_SAMPLES {
            let frame_samples: Vec<i16> = pending_samples.drain(..PCM_FRAME_SAMPLES).collect();
            let frame = make_frame(frame_samples, &decoded_audio_ms);
            if let Some(sender) = first_frame_sender.take() {
                let _ = sender.send(Ok(()));
            }
            if let Err(error) = pipeline.push_frame(frame, &mut assembler).await {
                break 'capture Err(error);
            }
        }
    };

    pipeline.close().await;
    if let Some(sender) = first_frame_sender {
        let message = result
            .as_ref()
            .err()
            .cloned()
            .unwrap_or_else(|| "WASAPI 尚未产出首帧音频".to_string());
        let _ = sender.send(Err(message));
    }
    result
}

fn make_frame(samples: Vec<i16>, decoded_audio_ms: &AtomicU64) -> Pcm16kChunk {
    let pts_ms = decoded_audio_ms.fetch_add(FRAME_DURATION_MS as u64, Ordering::SeqCst);
    let rms_energy = rms_energy(&samples);
    Pcm16kChunk {
        pts_ms,
        duration_ms: FRAME_DURATION_MS,
        samples,
        rms_energy,
    }
}

fn rms_energy(samples: &[i16]) -> f32 {
    if samples.is_empty() {
        return 0.0;
    }
    let sum_squares = samples
        .iter()
        .map(|sample| {
            let normalized = *sample as f64 / i16::MAX as f64;
            normalized * normalized
        })
        .sum::<f64>();
    (sum_squares / samples.len() as f64).sqrt() as f32
}

#[derive(Default)]
struct SegmentAssembler {
    samples: Vec<i16>,
    start_pts_ms: Option<u64>,
    sum_squares: f64,
}

impl SegmentAssembler {
    fn push(&mut self, frame: Pcm16kChunk) -> Option<Pcm16kChunk> {
        if self.samples.is_empty() {
            self.start_pts_ms = Some(frame.pts_ms);
        }
        self.sum_squares += frame
            .samples
            .iter()
            .map(|sample| {
                let normalized = *sample as f64 / i16::MAX as f64;
                normalized * normalized
            })
            .sum::<f64>();
        self.samples.extend(frame.samples);

        let duration_ms = (self.samples.len() / 16) as u32;
        if duration_ms < MIN_SEGMENT_MS
            || (frame.rms_energy >= VAD_RMS_THRESHOLD && duration_ms < MAX_SEGMENT_MS)
        {
            return None;
        }

        let samples = std::mem::take(&mut self.samples);
        let rms_energy = (self.sum_squares / samples.len() as f64).sqrt() as f32;
        self.sum_squares = 0.0;
        Some(Pcm16kChunk {
            pts_ms: self.start_pts_ms.take()?,
            duration_ms,
            samples,
            rms_energy,
        })
    }
}

#[derive(Default)]
struct LinearResampler {
    source_rate_hz: Option<u32>,
}

impl LinearResampler {
    fn resample(&mut self, samples: &[f32], source_rate_hz: u32) -> Result<Vec<i16>, String> {
        if source_rate_hz == 0 {
            return Err("WASAPI 返回无效采样率".to_string());
        }
        if self
            .source_rate_hz
            .is_some_and(|previous_rate| previous_rate != source_rate_hz)
        {
            return Err("WASAPI 采样率在采集中发生变化".to_string());
        }
        self.source_rate_hz = Some(source_rate_hz);

        let output_len = ((samples.len() as f64 * SAMPLE_RATE_HZ as f64) / source_rate_hz as f64)
            .round() as usize;
        let ratio = source_rate_hz as f64 / SAMPLE_RATE_HZ as f64;
        let mut output = Vec::with_capacity(output_len);
        for output_index in 0..output_len {
            let source_position = output_index as f64 * ratio;
            let left_index = source_position.floor() as usize;
            let right_index = (left_index + 1).min(samples.len().saturating_sub(1));
            let fraction = (source_position - left_index as f64) as f32;
            let left = samples.get(left_index).copied().unwrap_or(0.0);
            let right = samples.get(right_index).copied().unwrap_or(left);
            let sample = (left + (right - left) * fraction).clamp(-1.0, 1.0);
            output.push((sample * i16::MAX as f32).round() as i16);
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::{make_frame, rms_energy, LinearResampler, SegmentAssembler, FRAME_DURATION_MS};
    use std::sync::atomic::AtomicU64;

    #[test]
    fn decoded_audio_pts_advances_exactly_one_hundred_ms_per_pcm_frame() {
        let pts = AtomicU64::new(0);
        let first = make_frame(vec![0; 1_600], &pts);
        let second = make_frame(vec![0; 1_600], &pts);

        assert_eq!(first.pts_ms, 0);
        assert_eq!(second.pts_ms, FRAME_DURATION_MS as u64);
        assert_eq!(pts.load(std::sync::atomic::Ordering::SeqCst), 200);
    }

    #[test]
    fn direct_tap_resamples_48khz_mono_to_16khz_pcm() {
        let mut resampler = LinearResampler::default();
        let samples = vec![0.5; 480];

        let converted = resampler.resample(&samples, 48_000).unwrap();

        assert_eq!(converted.len(), 160);
        assert!(converted.iter().all(|sample| *sample > 16_000));
    }

    #[test]
    fn segment_assembler_cuts_after_minimum_duration_at_silence() {
        let pts = AtomicU64::new(0);
        let mut assembler = SegmentAssembler::default();
        let mut completed = None;
        for _ in 0..25 {
            let frame = make_frame(vec![0; 1_600], &pts);
            completed = assembler.push(frame);
        }

        let segment = completed.unwrap();
        assert_eq!(segment.duration_ms, 2_500);
        assert_eq!(segment.samples.len(), 40_000);
        assert_eq!(rms_energy(&segment.samples), 0.0);
    }
}
