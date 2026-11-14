use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use reqwest::multipart::{Form, Part};
use reqwest::{Client, Response, StatusCode};
use serde_json::{json, Value};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use url::Url;
use uuid::Uuid;

use super::models::{
    AiSubtitleConfig, AiSubtitleCue, AiSubtitleProvider, AiSubtitleRoute, Pcm16kChunk,
};

const DEFAULT_BAILIAN_BASE_URL: &str = "https://dashscope.aliyuncs.com/compatible-mode/v1";
pub(crate) const DEFAULT_BAILIAN_REALTIME_MODEL: &str = "qwen3.8-livetranslate-flash-realtime";
const ASR_MODEL: &str = "paraformer-v2";
const MIN_SEGMENT_SAMPLES: usize = 40_000;
const MAX_SEGMENT_SAMPLES: usize = 56_000;
const VAD_RMS_THRESHOLD: f32 = 0.008;
const MAX_CONTEXT_SENTENCES: usize = 2;
const REALTIME_CUE_TAIL_MS: u64 = 3_000;
/// Live captions release one sentence at a time. These limits stop a single cue
/// from growing into a wall of text when the model never marks the turn as done.
/// They are loose enough to let the translation finish a natural sentence.
const REALTIME_CUE_MAX_DURATION_MS: u64 = 4_500;
const REALTIME_CUE_MAX_TRANSLATED_CHARS: usize = 38;
const REALTIME_CUE_MAX_SOURCE_CHARS: usize = 130;
/// A sentence terminator only ends a cue once the translation has at least this
/// many characters, so short interjections do not become their own caption.
const REALTIME_MIN_SENTENCE_CHARS: usize = 8;
/// A long sentence may also break at a clause comma once it reaches this size,
/// which keeps the break on a natural boundary instead of mid-word.
const REALTIME_CLAUSE_BREAK_CHARS: usize = 18;
/// A hard cap (duration/size) may only force a split once the translation is at
/// least this long; below it the cue stays partial so the caption is never cut
/// at 8-9 characters while the translation is still streaming.
const REALTIME_FORCED_RELEASE_MIN_CHARS: usize = 20;
/// `response.text.done` means the server finished the whole turn, so even a
/// short polished translation may be released.
const REALTIME_DONE_RELEASE_MIN_CHARS: usize = 4;
/// Temporary diagnostics counter for live subtitle troubleshooting.
static DIAG_LINES: AtomicU64 = AtomicU64::new(0);
/// Translation text arrives about this long after the audio that produced it.
/// Compensating keeps the next caption's window aligned with what the viewer hears.
const REALTIME_TRANSLATION_LAG_MS: u64 = 1_500;
const REALTIME_SPEECH_END_SILENCE_MS: u32 = 450;
const MAX_PENDING_REALTIME_SPEECH_STARTS: usize = 16;
const UNANCHORED_REALTIME_CUE_LOOKBACK_MS: u64 = 1_000;
const REALTIME_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const REALTIME_SESSION_TIMEOUT: Duration = Duration::from_secs(15);
const REALTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
type RealtimeSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct RealtimeSession {
    outgoing: mpsc::Sender<Message>,
    task: JoinHandle<Result<(), String>>,
}

pub struct BailianRealtimeEngine {
    config: AiSubtitleConfig,
    model: String,
    api_key: String,
    cue_sender: UnboundedSender<AiSubtitleCue>,
    latest_audio_pts_ms: Arc<AtomicU64>,
    speech_tracker: Arc<Mutex<RealtimeSpeechTracker>>,
    session: Option<RealtimeSession>,
}

impl BailianRealtimeEngine {
    pub fn new(
        config: AiSubtitleConfig,
        cue_sender: UnboundedSender<AiSubtitleCue>,
    ) -> Result<Self, String> {
        if config.provider != AiSubtitleProvider::DashScope {
            return Err("百炼 Realtime 引擎只支持 DashScope".to_string());
        }
        let api_key = config
            .api_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "请先配置百炼 API Key".to_string())?
            .to_string();
        let model = if config.model.to_ascii_lowercase().contains("realtime") {
            config.model.trim().to_string()
        } else {
            DEFAULT_BAILIAN_REALTIME_MODEL.to_string()
        };

        Ok(Self {
            config,
            model,
            api_key,
            cue_sender,
            latest_audio_pts_ms: Arc::new(AtomicU64::new(0)),
            speech_tracker: Arc::new(Mutex::new(RealtimeSpeechTracker::default())),
            session: None,
        })
    }

    async fn stop_session(&mut self) -> Result<(), String> {
        let Some(session) = self.session.take() else {
            return Ok(());
        };
        let RealtimeSession { outgoing, mut task } = session;
        let finish_result = timeout(
            REALTIME_SHUTDOWN_TIMEOUT,
            outgoing.send(Message::Text(
                json!({ "type": "session.finish" }).to_string().into(),
            )),
        )
        .await;
        match finish_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                task.abort();
                let _ = task.await;
                return Err(format!("发送百炼 Realtime session.finish 失败：{error}"));
            }
            Err(_) => {
                task.abort();
                let _ = task.await;
                return Err("发送百炼 Realtime session.finish 超时".to_string());
            }
        }

        let result = match timeout(REALTIME_SHUTDOWN_TIMEOUT, &mut task).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(format!("百炼 Realtime 接收任务失败：{error}")),
            Err(_) => {
                task.abort();
                Err("等待百炼 Realtime session.finished 超时".to_string())
            }
        };
        drop(outgoing);
        result
    }
}

impl AiSubtitleEngine for BailianRealtimeEngine {
    fn wants_audio_frames(&self) -> bool {
        true
    }

    fn start(&mut self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            if self.session.is_some() {
                return Err("百炼 Realtime 引擎已经启动".to_string());
            }
            let request = build_realtime_request(
                self.config.base_url.as_deref(),
                &self.model,
                &self.api_key,
            )?;
            let (mut socket, _) = timeout(REALTIME_CONNECT_TIMEOUT, connect_async(request))
                .await
                .map_err(|_| "百炼 Realtime WebSocket 握手超时（15 秒）".to_string())?
                .map_err(|error| describe_realtime_connect_error(&error))?;
            wait_for_realtime_event(&mut socket, "session.created").await?;
            let session_update = build_realtime_session_update(&self.config);
            socket
                .send(Message::Text(session_update.to_string().into()))
                .await
                .map_err(|error| format!("发送百炼 Realtime session.update 失败：{error}"))?;
            wait_for_realtime_event(&mut socket, "session.updated").await?;

            let (outgoing, mut outgoing_receiver) = mpsc::channel(16);
            let cue_sender = self.cue_sender.clone();
            let latest_audio_pts_ms = Arc::clone(&self.latest_audio_pts_ms);
            let speech_tracker = Arc::clone(&self.speech_tracker);
            let task = tokio::spawn(async move {
                let mut cue_assembler = RealtimeCueAssembler::default();
                loop {
                    tokio::select! {
                        message = outgoing_receiver.recv() => {
                            let Some(message) = message else {
                                socket.close(None).await
                                    .map_err(|error| format!("关闭百炼 Realtime WebSocket 失败：{error}"))?;
                                return Ok(());
                            };
                            if matches!(message, Message::Close(_)) {
                                socket.close(None).await
                                    .map_err(|error| format!("关闭百炼 Realtime WebSocket 失败：{error}"))?;
                                return Ok(());
                            }
                            socket.send(message).await
                                .map_err(|error| format!("发送百炼 Realtime 音频帧失败：{error}"))?;
                        }
                        incoming = socket.next() => {
                            match incoming {
                                Some(Ok(Message::Text(text))) => {
                                    if serde_json::from_str::<Value>(text.as_ref())
                                        .ok()
                                        .and_then(|event| event.get("type").and_then(Value::as_str).map(str::to_string))
                                        .as_deref()
                                        == Some("session.finished")
                                    {
                                        socket.close(None).await
                                            .map_err(|error| format!("关闭百炼 Realtime WebSocket 失败：{error}"))?;
                                        return Ok(());
                                    }
                                    cue_assembler.handle_event(
                                        text.as_ref(),
                                        latest_audio_pts_ms.load(Ordering::SeqCst),
                                        &cue_sender,
                                        &speech_tracker,
                                    )?;
                                }
                                Some(Ok(Message::Ping(payload))) => {
                                    socket.send(Message::Pong(payload)).await
                                        .map_err(|error| format!("回复百炼 Realtime Ping 失败：{error}"))?;
                                }
                                Some(Ok(Message::Close(_))) => {
                                    return Ok(());
                                }
                                Some(Ok(_)) => {}
                                Some(Err(error)) => {
                                    return Err(format!("接收百炼 Realtime 事件失败：{error}"));
                                }
                                None => return Err("百炼 Realtime WebSocket 连接已结束".to_string()),
                            }
                        }
                    }
                }
            });
            self.session = Some(RealtimeSession { outgoing, task });
            Ok(())
        })
    }

    fn on_frame_pcm(&mut self, chunk: Pcm16kChunk) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let session = self
                .session
                .as_ref()
                .ok_or_else(|| "百炼 Realtime 引擎尚未启动".to_string())?;
            self.latest_audio_pts_ms.store(
                chunk.pts_ms.saturating_add(chunk.duration_ms as u64),
                Ordering::SeqCst,
            );
            if let Ok(mut tracker) = self.speech_tracker.lock() {
                tracker.observe(&chunk);
            }
            let mut pcm_bytes = Vec::with_capacity(chunk.samples.len() * 2);
            for sample in chunk.samples {
                pcm_bytes.extend_from_slice(&sample.to_le_bytes());
            }
            let event = json!({
                "type": "input_audio_buffer.append",
                "audio": base64::engine::general_purpose::STANDARD.encode(pcm_bytes),
            });
            session
                .outgoing
                .send(Message::Text(event.to_string().into()))
                .await
                .map_err(|error| format!("发送百炼 Realtime 音频数据失败：{error}"))
        })
    }

    fn on_segment_pcm(&mut self, _chunk: Pcm16kChunk) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn stop(&mut self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move { self.stop_session().await })
    }
}

pub(crate) fn build_realtime_request(
    base_url: Option<&str>,
    model: &str,
    api_key: &str,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, String> {
    let api_key = api_key.trim();
    if api_key.is_empty() {
        return Err("请先填写百炼 API Key".to_string());
    }
    if model.trim().is_empty() {
        return Err("请先配置 Realtime 模型".to_string());
    }
    let base_url = base_url
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_BAILIAN_BASE_URL);
    let mut url =
        Url::parse(base_url).map_err(|error| format!("Realtime Base URL 无效：{error}"))?;
    match url.scheme() {
        "https" => url
            .set_scheme("wss")
            .map_err(|_| "无法将 Realtime URL 转换为 WSS".to_string())?,
        "http" => url
            .set_scheme("ws")
            .map_err(|_| "无法将 Realtime URL 转换为 WS".to_string())?,
        "ws" | "wss" => {}
        _ => return Err("Realtime Base URL 必须使用 HTTP(S) 或 WS(S)".to_string()),
    }

    url.set_path("/api-ws/v1/realtime");
    url.set_query(None);
    url.query_pairs_mut().append_pair("model", model.trim());

    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|error| format!("构造百炼 Realtime 握手请求失败：{error}"))?;
    let authorization = HeaderValue::from_str(&format!("Bearer {api_key}"))
        .map_err(|error| format!("API Key 无效：{error}"))?;
    request.headers_mut().insert(AUTHORIZATION, authorization);
    Ok(request)
}

fn build_realtime_session_update(config: &AiSubtitleConfig) -> Value {
    json!({
        "type": "session.update",
        "session": {
            "output_modalities": ["text"],
            "audio": {
                "input": {
                    "format": {
                        "type": "pcm",
                        "sample_rate": 16000
                    },
                    "turn_detection": {
                        "type": "speaker_detection",
                        "threshold": 0.5
                    }
                }
            },
            "translation": {
                "language": config.target_language
            }
        }
    })
}

async fn wait_for_realtime_event(
    socket: &mut RealtimeSocket,
    expected_type: &str,
) -> Result<(), String> {
    timeout(REALTIME_SESSION_TIMEOUT, async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) => {
                    let event: Value = serde_json::from_str(text.as_ref())
                        .map_err(|error| format!("解析百炼 Realtime 会话事件失败：{error}"))?;
                    let event_type = event
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if event_type == "error" {
                        let message = event
                            .pointer("/error/message")
                            .and_then(Value::as_str)
                            .unwrap_or("未知服务端错误");
                        return Err(format!("百炼 Realtime 返回错误：{message}"));
                    }
                    if event_type == expected_type {
                        return Ok(());
                    }
                }
                Some(Ok(Message::Ping(payload))) => socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|error| format!("回复百炼 Realtime Ping 失败：{error}"))?,
                Some(Ok(Message::Close(_))) | None => {
                    return Err("百炼 Realtime WebSocket 在会话初始化期间关闭".to_string());
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => {
                    return Err(format!("接收百炼 Realtime 会话事件失败：{error}"));
                }
            }
        }
    })
    .await
    .map_err(|_| format!("等待百炼 Realtime {expected_type} 事件超时（15 秒）"))?
}

#[derive(Default)]
struct RealtimeCueAssembler {
    cue: Option<AiSubtitleCue>,
    next_start_pts_ms: Option<u64>,
    source_item_id: Option<String>,
    source_buffer: String,
    source_offset: usize,
    translation_response_id: Option<String>,
    translation_buffer: String,
    translation_offset: usize,
    translation_received: bool,
    last_released_start_pts_ms: Option<u64>,
}

#[derive(Default)]
struct RealtimeSpeechTracker {
    in_speech: bool,
    silence_ms: u32,
    pending_starts: VecDeque<u64>,
}

impl RealtimeSpeechTracker {
    fn observe(&mut self, chunk: &Pcm16kChunk) {
        if chunk.rms_energy >= VAD_RMS_THRESHOLD {
            if !self.in_speech {
                self.pending_starts.push_back(chunk.pts_ms);
                if self.pending_starts.len() > MAX_PENDING_REALTIME_SPEECH_STARTS {
                    self.pending_starts.pop_front();
                }
            }
            self.in_speech = true;
            self.silence_ms = 0;
        } else if self.in_speech {
            self.silence_ms = self.silence_ms.saturating_add(chunk.duration_ms);
            if self.silence_ms >= REALTIME_SPEECH_END_SILENCE_MS {
                self.in_speech = false;
                self.silence_ms = 0;
            }
        }
    }

    fn take_start(&mut self) -> Option<u64> {
        self.pending_starts.pop_front()
    }
}

impl RealtimeCueAssembler {
    fn handle_event(
        &mut self,
        payload: &str,
        latest_audio_pts_ms: u64,
        cue_sender: &UnboundedSender<AiSubtitleCue>,
        speech_tracker: &Mutex<RealtimeSpeechTracker>,
    ) -> Result<(), String> {
        let event: Value = serde_json::from_str(payload)
            .map_err(|error| format!("解析百炼 Realtime 事件失败：{error}"))?;
        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if event_type == "error" {
            let message = event
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("未知服务端错误");
            return Err(format!("百炼 Realtime 返回错误：{message}"));
        }
        // Temporary diagnostics for live subtitle troubleshooting.
        if matches!(
            event_type,
            "response.text.delta"
                | "response.text.done"
                | "response.audio_transcript.delta"
                | "response.audio_transcript.done"
                | "conversation.item.input_audio_transcription.delta"
                | "conversation.item.input_audio_transcription.completed"
                | "response.done"
        ) {
            let sequence = DIAG_LINES.fetch_add(1, Ordering::Relaxed);
            if sequence < 1_500 {
                let text = event
                    .get("delta")
                    .or_else(|| event.get("text"))
                    .or_else(|| event.get("transcript"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let preview: String = text.chars().take(24).collect();
                log::warn!(
                    "[AiSubtitleDiag] evt={event_type} latest={latest_audio_pts_ms} item={:?} resp={:?} len={} preview={preview:?}",
                    event.get("item_id").and_then(Value::as_str),
                    event.get("response_id").and_then(Value::as_str),
                    text.chars().count(),
                );
            }
        }
        if event_type == "input_audio_buffer.speech_started" {
            let local_start_pts_ms = speech_tracker
                .lock()
                .ok()
                .and_then(|mut tracker| tracker.take_start());
            self.next_start_pts_ms = event.get("audio_start_ms").and_then(Value::as_u64);
            self.next_start_pts_ms = local_start_pts_ms.or(self.next_start_pts_ms).or(Some(
                latest_audio_pts_ms.saturating_sub(UNANCHORED_REALTIME_CUE_LOOKBACK_MS),
            ));
            return Ok(());
        }
        if event_type == "response.done" {
            // Some deployments only send the finished text on response.done.
            if !self.translation_received {
                if let Some(text) = response_output_text(&event) {
                    self.reset_translation_stream();
                    self.push_translation_update(
                        None,
                        &text,
                        true,
                        latest_audio_pts_ms,
                        cue_sender,
                        speech_tracker,
                    )?;
                }
            }
            self.finish_current_cue(
                latest_audio_pts_ms,
                cue_sender,
                REALTIME_DONE_RELEASE_MIN_CHARS,
            )?;
            self.reset_translation_stream();
            return Ok(());
        }

        let item_id = event.get("item_id").and_then(Value::as_str);
        let response_id = event.get("response_id").and_then(Value::as_str);
        match event_type {
            "conversation.item.input_audio_transcription.delta" => {
                if let Some(text) = event.get("delta").and_then(Value::as_str) {
                    self.push_source_update(
                        item_id,
                        text,
                        false,
                        latest_audio_pts_ms,
                        cue_sender,
                        speech_tracker,
                    )?;
                }
            }
            "conversation.item.input_audio_transcription.completed" => {
                if let Some(text) = event.get("transcript").and_then(Value::as_str) {
                    self.push_source_update(
                        item_id,
                        text,
                        true,
                        latest_audio_pts_ms,
                        cue_sender,
                        speech_tracker,
                    )?;
                }
            }
            "response.text.delta" | "response.audio_transcript.delta" => {
                if let Some(text) = event.get("delta").and_then(Value::as_str) {
                    self.push_translation_update(
                        response_id,
                        text,
                        false,
                        latest_audio_pts_ms,
                        cue_sender,
                        speech_tracker,
                    )?;
                }
            }
            "response.text.done" | "response.audio_transcript.done" => {
                if let Some(text) = event.get("text").and_then(Value::as_str) {
                    self.push_translation_update(
                        response_id,
                        text,
                        true,
                        latest_audio_pts_ms,
                        cue_sender,
                        speech_tracker,
                    )?;
                }
                // The server has finished polishing this sentence's translation,
                // so release it now instead of waiting for a full stop or a
                // size limit that may never arrive.
                let translated_enough = self.cue.as_ref().is_some_and(|cue| {
                    cue.translated_text.trim().chars().count() >= REALTIME_DONE_RELEASE_MIN_CHARS
                });
                if translated_enough
                    && self.finish_current_cue(
                        latest_audio_pts_ms,
                        cue_sender,
                        REALTIME_DONE_RELEASE_MIN_CHARS,
                    )?
                {
                    self.next_start_pts_ms =
                        Some(self.next_cue_start_after_release(latest_audio_pts_ms));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn push_source_update(
        &mut self,
        item_id: Option<&str>,
        text: &str,
        replace: bool,
        latest_audio_pts_ms: u64,
        cue_sender: &UnboundedSender<AiSubtitleCue>,
        speech_tracker: &Mutex<RealtimeSpeechTracker>,
    ) -> Result<(), String> {
        if let Some(item_id) = item_id {
            if self.source_item_id.as_deref() != Some(item_id) {
                self.source_item_id = Some(item_id.to_string());
                self.source_buffer.clear();
                self.source_offset = 0;
            }
        }
        if replace {
            self.source_buffer = text.to_string();
        } else {
            self.source_buffer.push_str(text);
        }
        let pending: String = self
            .source_buffer
            .chars()
            .skip(self.source_offset)
            .collect();
        if pending.is_empty() {
            return Ok(());
        }
        self.source_offset += pending.chars().count();
        self.append_source_text(&pending, latest_audio_pts_ms, speech_tracker);
        self.enforce_size_limits(latest_audio_pts_ms, cue_sender)?;
        self.emit_partial(cue_sender)
    }

    fn push_translation_update(
        &mut self,
        response_id: Option<&str>,
        text: &str,
        replace: bool,
        latest_audio_pts_ms: u64,
        cue_sender: &UnboundedSender<AiSubtitleCue>,
        speech_tracker: &Mutex<RealtimeSpeechTracker>,
    ) -> Result<(), String> {
        if let Some(response_id) = response_id {
            if self.translation_response_id.as_deref() != Some(response_id) {
                self.reset_translation_stream();
                self.translation_response_id = Some(response_id.to_string());
            }
        }
        if replace {
            self.translation_buffer = text.to_string();
        } else {
            self.translation_buffer.push_str(text);
        }
        if !text.is_empty() {
            self.translation_received = true;
        }
        let pending: String = self
            .translation_buffer
            .chars()
            .skip(self.translation_offset)
            .collect();
        if pending.is_empty() {
            return Ok(());
        }
        self.translation_offset += pending.chars().count();
        self.append_translation_text(
            &pending,
            latest_audio_pts_ms,
            cue_sender,
            speech_tracker,
        )?;
        self.emit_partial(cue_sender)
    }

    fn append_source_text(
        &mut self,
        text: &str,
        latest_audio_pts_ms: u64,
        speech_tracker: &Mutex<RealtimeSpeechTracker>,
    ) {
        let cue = self.ensure_cue(latest_audio_pts_ms, speech_tracker);
        cue.source_text.push_str(text);
        cue.end_pts_ms = cue
            .end_pts_ms
            .max(latest_audio_pts_ms.saturating_add(REALTIME_CUE_TAIL_MS))
            .max(cue.start_pts_ms.saturating_add(1));
    }

    fn append_translation_text(
        &mut self,
        text: &str,
        latest_audio_pts_ms: u64,
        cue_sender: &UnboundedSender<AiSubtitleCue>,
        speech_tracker: &Mutex<RealtimeSpeechTracker>,
    ) -> Result<(), String> {
        for character in text.chars() {
            {
                let cue = self.ensure_cue(latest_audio_pts_ms, speech_tracker);
                cue.translated_text.push(character);
                cue.end_pts_ms = cue
                    .end_pts_ms
                    .max(latest_audio_pts_ms.saturating_add(REALTIME_CUE_TAIL_MS))
                    .max(cue.start_pts_ms.saturating_add(1));
            }
            if self.should_release_translation(character)
                && self.finish_current_cue(
                    latest_audio_pts_ms,
                    cue_sender,
                    REALTIME_MIN_SENTENCE_CHARS,
                )?
            {
                self.next_start_pts_ms = Some(self.next_cue_start_after_release(latest_audio_pts_ms));
            }
        }
        // Apply the size/duration caps once per chunk so a long sentence is cut
        // at the cap instead of running until the model ends the turn.
        self.enforce_size_limits(latest_audio_pts_ms, cue_sender)?;
        Ok(())
    }

    fn should_release_translation(&self, last_character: char) -> bool {
        let Some(cue) = self.cue.as_ref() else {
            return false;
        };
        let translated_chars = cue.translated_text.trim().chars().count();
        if is_sentence_terminator(last_character) {
            return translated_chars >= REALTIME_MIN_SENTENCE_CHARS;
        }
        is_clause_separator(last_character) && translated_chars >= REALTIME_CLAUSE_BREAK_CHARS
    }

    fn enforce_size_limits(
        &mut self,
        latest_audio_pts_ms: u64,
        cue_sender: &UnboundedSender<AiSubtitleCue>,
    ) -> Result<(), String> {
        let should_release = self.cue.as_ref().is_some_and(|cue| {
            let translated_chars = cue.translated_text.trim().chars().count();
            let source_chars = cue.source_text.chars().count();
            // Never publish a half-translated caption: a forced split only
            // happens once the translation itself is substantial enough.
            translated_chars >= REALTIME_FORCED_RELEASE_MIN_CHARS
                && (source_chars >= REALTIME_CUE_MAX_SOURCE_CHARS
                    || translated_chars >= REALTIME_CUE_MAX_TRANSLATED_CHARS
                    || latest_audio_pts_ms.saturating_sub(cue.start_pts_ms)
                        >= REALTIME_CUE_MAX_DURATION_MS)
        });
        if should_release
            && self.finish_current_cue(
                latest_audio_pts_ms,
                cue_sender,
                REALTIME_FORCED_RELEASE_MIN_CHARS,
            )?
        {
            self.next_start_pts_ms = Some(self.next_cue_start_after_release(latest_audio_pts_ms));
        }
        Ok(())
    }

    fn ensure_cue(
        &mut self,
        latest_audio_pts_ms: u64,
        speech_tracker: &Mutex<RealtimeSpeechTracker>,
    ) -> &mut AiSubtitleCue {
        if self.cue.is_none() {
            let local_start_pts_ms = speech_tracker
                .lock()
                .ok()
                .and_then(|mut tracker| tracker.take_start());
            let start_pts_ms = self
                .next_start_pts_ms
                .take()
                .or(local_start_pts_ms)
                .unwrap_or_else(|| {
                    latest_audio_pts_ms.saturating_sub(UNANCHORED_REALTIME_CUE_LOOKBACK_MS)
                });
            self.cue = Some(AiSubtitleCue {
                id: Uuid::new_v4().to_string(),
                source_text: String::new(),
                translated_text: String::new(),
                start_pts_ms,
                end_pts_ms: latest_audio_pts_ms.saturating_add(REALTIME_CUE_TAIL_MS),
                is_partial: true,
            });
        }
        self.cue
            .as_mut()
            .expect("cue is created when it is missing")
    }

    fn finish_current_cue(
        &mut self,
        latest_audio_pts_ms: u64,
        cue_sender: &UnboundedSender<AiSubtitleCue>,
        min_chars: usize,
    ) -> Result<bool, String> {
        let Some(cue) = self.cue.as_ref() else {
            return Ok(false);
        };
        if cue.source_text.trim().is_empty() && cue.translated_text.trim().is_empty() {
            self.cue = None;
            return Ok(false);
        }
        // A caption without a substantial translation is an English-only
        // fragment. Keep the cue partial and wait for the translation instead
        // of publishing it, even when the size or duration limit is reached.
        let translated_chars = cue.translated_text.trim().chars().count();
        if translated_chars < min_chars {
            return Ok(false);
        }
        let Some(mut cue) = self.cue.take() else {
            return Ok(false);
        };
        cue.end_pts_ms = cue
            .end_pts_ms
            .max(latest_audio_pts_ms.saturating_add(REALTIME_CUE_TAIL_MS))
            .max(cue.start_pts_ms.saturating_add(1));
        cue.is_partial = false;
        self.last_released_start_pts_ms = Some(cue.start_pts_ms);
        cue_sender
            .send(cue)
            .map_err(|_| "AI 字幕事件接收端已关闭".to_string())?;
        Ok(true)
    }

    fn next_cue_start_after_release(&self, latest_audio_pts_ms: u64) -> u64 {
        let compensated = latest_audio_pts_ms.saturating_sub(REALTIME_TRANSLATION_LAG_MS);
        self.last_released_start_pts_ms
            .map_or(compensated, |start| compensated.max(start.saturating_add(1)))
    }

    fn emit_partial(&self, cue_sender: &UnboundedSender<AiSubtitleCue>) -> Result<(), String> {
        if let Some(cue) = &self.cue {
            if !cue.source_text.is_empty() || !cue.translated_text.is_empty() {
                cue_sender
                    .send(cue.clone())
                    .map_err(|_| "AI 字幕事件接收端已关闭".to_string())?;
            }
        }
        Ok(())
    }

    fn reset_translation_stream(&mut self) {
        self.translation_response_id = None;
        self.translation_buffer.clear();
        self.translation_offset = 0;
        self.translation_received = false;
    }
}

fn is_sentence_terminator(character: char) -> bool {
    matches!(
        character,
        '。' | '！' | '？' | '；' | '…' | '!' | '?' | ';'
    )
}

/// Clause-level punctuation that can end a long caption on a natural boundary
/// once the translation is long enough.
fn is_clause_separator(character: char) -> bool {
    matches!(character, '，' | '、' | ',')
}

fn response_output_text(event: &Value) -> Option<String> {
    event
        .pointer("/response/output")
        .and_then(Value::as_array)?
        .iter()
        .flat_map(|item| {
            item.get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|content| content.get("text").and_then(Value::as_str))
        .map(str::to_string)
        .reduce(|mut output, part| {
            output.push_str(&part);
            output
        })
}

pub(crate) fn describe_realtime_connect_error(
    error: &tokio_tungstenite::tungstenite::Error,
) -> String {
    match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => {
            let status = response.status();
            let detail = response
                .body()
                .as_ref()
                .map(|body| String::from_utf8_lossy(body).trim().to_string())
                .filter(|body| !body.is_empty())
                .map(|body| format!("：{body}"))
                .unwrap_or_default();
            format!("百炼 Realtime WebSocket 握手失败（HTTP {status}）{detail}")
        }
        _ => format!("连接百炼 Realtime WebSocket 失败：{error}"),
    }
}

pub trait AiSubtitleEngine: Send {
    fn wants_audio_frames(&self) -> bool {
        true
    }

    fn start(&mut self) -> BoxFuture<'_, Result<(), String>>;
    fn on_frame_pcm(&mut self, chunk: Pcm16kChunk) -> BoxFuture<'_, Result<(), String>>;
    fn on_segment_pcm(&mut self, chunk: Pcm16kChunk) -> BoxFuture<'_, Result<(), String>>;
    fn stop(&mut self) -> BoxFuture<'_, Result<(), String>>;
}

struct TranslationContext {
    source_text: String,
    translated_text: String,
}

pub struct BailianTwoStageEngine {
    client: Client,
    config: AiSubtitleConfig,
    api_key: String,
    cue_sender: UnboundedSender<AiSubtitleCue>,
    context: VecDeque<TranslationContext>,
    started: bool,
}

impl BailianTwoStageEngine {
    pub fn new(
        config: AiSubtitleConfig,
        cue_sender: UnboundedSender<AiSubtitleCue>,
    ) -> Result<Self, String> {
        if config.provider != AiSubtitleProvider::DashScope
            || config.route != AiSubtitleRoute::TwoStage
        {
            return Err("百炼两步走引擎只支持 DashScope 的 two_stage 路线".to_string());
        }
        // 支持任意百炼模型，包括 qwen-plus, qwen-turbo, qwen3.8-livetranslate 系列
        let model = config.model.trim();
        if model.is_empty() {
            return Err("请先配置翻译模型".to_string());
        }
        let api_key = config
            .api_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "请先配置百炼 API Key".to_string())?
            .to_string();

        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| format!("创建百炼 HTTP 客户端失败：{error}"))?;

        Ok(Self {
            client,
            config,
            api_key,
            cue_sender,
            context: VecDeque::with_capacity(MAX_CONTEXT_SENTENCES),
            started: false,
        })
    }

    fn base_url(&self) -> &str {
        self.config
            .base_url
            .as_deref()
            .map(str::trim)
            .filter(|base_url| !base_url.is_empty())
            .unwrap_or(DEFAULT_BAILIAN_BASE_URL)
            .trim_end_matches('/')
    }

    async fn recognize(&self, wav: Vec<u8>) -> Result<String, String> {
        let file = Part::bytes(wav)
            .file_name("segment.wav")
            .mime_str("audio/wav")
            .map_err(|error| format!("创建 ASR 音频数据失败：{error}"))?;
        let form = Form::new().text("model", ASR_MODEL).part("file", file);
        let response = self
            .client
            .post(format!("{}/audio/transcriptions", self.base_url()))
            .bearer_auth(&self.api_key)
            .multipart(form)
            .send()
            .await
            .map_err(|error| format!("百炼语音识别请求失败：{error}"))?;
        let response = ensure_success(response, "百炼语音识别").await?;
        let body: Value = response
            .json()
            .await
            .map_err(|error| format!("解析百炼语音识别响应失败：{error}"))?;
        body.get("text")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
            .ok_or_else(|| "百炼语音识别没有返回有效文本".to_string())
    }

    async fn translate(&self, source_text: &str) -> Result<String, String> {
        let context = self
            .context
            .iter()
            .map(|sentence| {
                format!(
                    "原文：{}\n译文：{}",
                    sentence.source_text, sentence.translated_text
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let user_prompt = if context.is_empty() {
            format!("请翻译这句话：\n{source_text}")
        } else {
            format!("前文（仅供术语和语境参考）：\n{context}\n\n请翻译当前这句话：\n{source_text}")
        };

        let response = self
            .client
            .post(format!("{}/chat/completions", self.base_url()))
            .bearer_auth(&self.api_key)
            .json(&json!({
                "model": self.config.model,
                "messages": [
                    {
                        "role": "system",
                        "content": "将输入的外语口语翻译为简体中文电视字幕。译文简洁、自然、忠实原意，只输出译文，不添加解释。"
                    },
                    { "role": "user", "content": user_prompt }
                ],
                "temperature": 0.2,
                "max_tokens": 256
            }))
            .send()
            .await
            .map_err(|error| format!("百炼字幕翻译请求失败：{error}"))?;
        let response = ensure_success(response, "百炼字幕翻译").await?;
        let body: Value = response
            .json()
            .await
            .map_err(|error| format!("解析百炼字幕翻译响应失败：{error}"))?;
        extract_completion_text(&body)
            .filter(|text| !text.is_empty())
            .ok_or_else(|| "百炼翻译接口没有返回有效译文".to_string())
    }
}

impl AiSubtitleEngine for BailianTwoStageEngine {
    fn wants_audio_frames(&self) -> bool {
        false
    }

    fn start(&mut self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.started = true;
            Ok(())
        })
    }

    fn on_frame_pcm(&mut self, _chunk: Pcm16kChunk) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            if !self.started {
                return Err("AI 字幕引擎尚未启动".to_string());
            }
            Ok(())
        })
    }

    fn on_segment_pcm(&mut self, chunk: Pcm16kChunk) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            if !self.started {
                return Err("AI 字幕引擎尚未启动".to_string());
            }
            if is_silent(&chunk) {
                return Ok(());
            }
            validate_segment(&chunk)?;

            let wav = pcm16_mono_to_wav(&chunk.samples)?;
            let source_text = self.recognize(wav).await?;
            let end_pts_ms = chunk.pts_ms.saturating_add(chunk.duration_ms as u64);
            let cue_id = Uuid::new_v4().to_string();
            let partial_cue = AiSubtitleCue {
                id: cue_id.clone(),
                source_text: source_text.clone(),
                translated_text: String::new(),
                start_pts_ms: chunk.pts_ms,
                end_pts_ms,
                is_partial: true,
            };
            self.cue_sender
                .send(partial_cue)
                .map_err(|_| "AI 字幕事件接收端已关闭".to_string())?;

            let translated_text = self.translate(&source_text).await?;
            let complete_cue = AiSubtitleCue {
                id: cue_id,
                source_text: source_text.clone(),
                translated_text: translated_text.clone(),
                start_pts_ms: chunk.pts_ms,
                end_pts_ms,
                is_partial: false,
            };
            self.cue_sender
                .send(complete_cue)
                .map_err(|_| "AI 字幕事件接收端已关闭".to_string())?;

            self.context.push_back(TranslationContext {
                source_text,
                translated_text,
            });
            while self.context.len() > MAX_CONTEXT_SENTENCES {
                self.context.pop_front();
            }
            Ok(())
        })
    }

    fn stop(&mut self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.started = false;
            self.context.clear();
            Ok(())
        })
    }
}

fn is_silent(chunk: &Pcm16kChunk) -> bool {
    chunk.rms_energy < VAD_RMS_THRESHOLD
}

fn validate_segment(chunk: &Pcm16kChunk) -> Result<(), String> {
    if !(MIN_SEGMENT_SAMPLES..=MAX_SEGMENT_SAMPLES).contains(&chunk.samples.len()) {
        return Err("百炼两步走只接受 2.5 到 3.5 秒的 16 kHz PCM 音频切片".to_string());
    }
    Ok(())
}

pub fn create_ai_subtitle_engine(
    config: AiSubtitleConfig,
    cue_sender: UnboundedSender<AiSubtitleCue>,
) -> Result<Box<dyn AiSubtitleEngine>, String> {
    let is_realtime_model = config.model.to_ascii_lowercase().contains("realtime");
    if config.provider == AiSubtitleProvider::DashScope
        && (config.route == AiSubtitleRoute::Realtime || is_realtime_model)
    {
        return Ok(Box::new(BailianRealtimeEngine::new(config, cue_sender)?));
    }

    if config.provider == AiSubtitleProvider::DashScope && config.route == AiSubtitleRoute::TwoStage
    {
        return Ok(Box::new(BailianTwoStageEngine::new(config, cue_sender)?));
    }
    Err("当前 AI 字幕服务商或路线尚未实现".to_string())
}

fn pcm16_mono_to_wav(samples: &[i16]) -> Result<Vec<u8>, String> {
    let data_len = samples
        .len()
        .checked_mul(std::mem::size_of::<i16>())
        .and_then(|length| u32::try_from(length).ok())
        .ok_or_else(|| "PCM 音频切片过大".to_string())?;
    let riff_len = data_len
        .checked_add(36)
        .ok_or_else(|| "PCM 音频切片过大".to_string())?;
    let mut wav = Vec::with_capacity(data_len as usize + 44);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&riff_len.to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16_u32.to_le_bytes());
    wav.extend_from_slice(&1_u16.to_le_bytes());
    wav.extend_from_slice(&1_u16.to_le_bytes());
    wav.extend_from_slice(&16_000_u32.to_le_bytes());
    wav.extend_from_slice(&32_000_u32.to_le_bytes());
    wav.extend_from_slice(&2_u16.to_le_bytes());
    wav.extend_from_slice(&16_u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_len.to_le_bytes());
    for sample in samples {
        wav.extend_from_slice(&sample.to_le_bytes());
    }
    Ok(wav)
}

async fn ensure_success(response: Response, operation: &str) -> Result<Response, String> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }

    let body = response
        .text()
        .await
        .map_err(|error| format!("{operation}失败，读取错误响应时出错：{error}"))?;
    let detail = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .or_else(|| value.get("message"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .filter(|message| !message.is_empty())
        .unwrap_or_else(|| body.chars().take(240).collect());
    Err(format!(
        "{operation}失败：{}{}",
        describe_status(status),
        if detail.is_empty() {
            String::new()
        } else {
            format!("（{detail}）")
        }
    ))
}

fn describe_status(status: StatusCode) -> &'static str {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => "API Key 无效或无权限",
        StatusCode::NOT_FOUND => "API 地址或模型不存在",
        StatusCode::TOO_MANY_REQUESTS => "请求频率超限",
        _ => "服务请求错误",
    }
}

fn extract_completion_text(body: &Value) -> Option<String> {
    let content = body.pointer("/choices/0/message/content")?;
    if let Some(text) = content.as_str() {
        return Some(text.trim().to_string());
    }
    content.as_array().map(|parts| {
        parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("")
            .trim()
            .to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::{
        build_realtime_request, build_realtime_session_update, create_ai_subtitle_engine,
        extract_completion_text, is_silent, pcm16_mono_to_wav, validate_segment, AiSubtitleEngine,
        RealtimeCueAssembler, RealtimeSpeechTracker, DEFAULT_BAILIAN_REALTIME_MODEL,
        REALTIME_CUE_MAX_SOURCE_CHARS, REALTIME_CUE_MAX_TRANSLATED_CHARS,
    };
    use crate::ai_subtitle::models::{AiSubtitleConfig, AiSubtitleRoute, Pcm16kChunk};
    use axum::extract::State;
    use axum::routing::post;
    use axum::{Json, Router};
    use base64::Engine as _;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{json, Value};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio_tungstenite::tungstenite::Message;

    fn chunk(sample_count: usize, rms_energy: f32) -> Pcm16kChunk {
        Pcm16kChunk {
            pts_ms: 0,
            duration_ms: (sample_count / 16) as u32,
            samples: vec![0; sample_count],
            rms_energy,
        }
    }

    #[test]
    fn vad_skips_energy_below_threshold_and_accepts_threshold_value() {
        assert!(is_silent(&chunk(40_000, 0.007_999)));
        assert!(!is_silent(&chunk(40_000, 0.008)));
    }

    #[test]
    fn segment_duration_must_be_between_two_point_five_and_three_point_five_seconds() {
        assert!(validate_segment(&chunk(40_000, 0.01)).is_ok());
        assert!(validate_segment(&chunk(56_000, 0.01)).is_ok());
        assert!(validate_segment(&chunk(39_999, 0.01)).is_err());
        assert!(validate_segment(&chunk(56_001, 0.01)).is_err());
    }

    #[test]
    fn pcm_is_encoded_as_16khz_mono_pcm_wav() {
        let wav = pcm16_mono_to_wav(&[1, -2]).unwrap();

        assert_eq!(&wav[..4], b"RIFF");
        assert_eq!(&wav[8..12], b"WAVE");
        assert_eq!(wav.len(), 48);
        assert_eq!(&wav[24..28], &16_000_u32.to_le_bytes());
        assert_eq!(&wav[44..], &[1, 0, 254, 255]);
    }

    #[test]
    fn completion_text_supports_string_and_content_parts() {
        assert_eq!(
            extract_completion_text(&json!({
                "choices": [{ "message": { "content": "  你好。 " } }]
            })),
            Some("你好。".to_string())
        );
        assert_eq!(
            extract_completion_text(&json!({
                "choices": [{
                    "message": {
                        "content": [{ "text": "你" }, { "text": "好" }]
                    }
                }]
            })),
            Some("你好".to_string())
        );
    }

    #[test]
    fn realtime_handshake_uses_wss_model_query_and_bearer_auth() {
        let request = build_realtime_request(
            Some("https://dashscope.aliyuncs.com/compatible-mode/v1"),
            DEFAULT_BAILIAN_REALTIME_MODEL,
            "test-token",
        )
        .unwrap();

        assert_eq!(
            request.uri().to_string(),
            format!(
                "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model={DEFAULT_BAILIAN_REALTIME_MODEL}"
            )
        );
        assert_eq!(request.headers()["authorization"], "Bearer test-token");
    }

    #[test]
    fn realtime_session_update_sets_pcm_format_and_language_instructions() {
        let config = AiSubtitleConfig {
            source_language: "en".to_string(),
            target_language: "zh".to_string(),
            ..AiSubtitleConfig::default()
        };
        let update = build_realtime_session_update(&config);

        assert_eq!(update["type"], "session.update");
        assert_eq!(update["session"]["output_modalities"], json!(["text"]));
        assert_eq!(update["session"]["audio"]["input"]["format"]["type"], "pcm");
        assert_eq!(
            update["session"]["audio"]["input"]["format"]["sample_rate"],
            16_000
        );
        assert_eq!(
            update["session"]["audio"]["input"]["turn_detection"]["type"],
            "speaker_detection"
        );
        assert_eq!(update["session"]["translation"]["language"], "zh");
    }

    #[test]
    fn realtime_events_emit_updating_partial_and_final_cues() {
        let (cue_sender, mut cue_receiver) = unbounded_channel();
        let mut assembler = RealtimeCueAssembler::default();
        let speech_tracker = Mutex::new(RealtimeSpeechTracker::default());

        assembler
            .handle_event(
                r#"{"type":"conversation.item.input_audio_transcription.completed","transcript":"Hello"}"#,
                4_000,
                &cue_sender,
                &speech_tracker,
            )
            .unwrap();
        let source_partial = cue_receiver.try_recv().unwrap();
        assert_eq!(source_partial.source_text, "Hello");
        assert!(source_partial.is_partial);

        assembler
            .handle_event(
                r#"{"type":"response.text.delta","delta":"你好世界"}"#,
                4_200,
                &cue_sender,
                &speech_tracker,
            )
            .unwrap();
        let translation_partial = cue_receiver.try_recv().unwrap();
        assert_eq!(translation_partial.id, source_partial.id);
        assert_eq!(translation_partial.translated_text, "你好世界");
        assert!(translation_partial.is_partial);

        assembler
            .handle_event(
                r#"{"type":"response.done"}"#,
                4_300,
                &cue_sender,
                &speech_tracker,
            )
            .unwrap();
        let final_cue = cue_receiver.try_recv().unwrap();
        assert_eq!(final_cue.id, source_partial.id);
        assert_eq!(final_cue.source_text, "Hello");
        assert_eq!(final_cue.translated_text, "你好世界");
        assert!(!final_cue.is_partial);
    }

    #[test]
    fn realtime_cue_uses_local_speech_onset_when_server_onset_is_missing() {
        let (cue_sender, mut cue_receiver) = unbounded_channel();
        let mut assembler = RealtimeCueAssembler::default();
        let speech_tracker = Mutex::new(RealtimeSpeechTracker::default());
        speech_tracker.lock().unwrap().observe(&Pcm16kChunk {
            pts_ms: 1_000,
            duration_ms: 100,
            samples: vec![],
            rms_energy: 0.02,
        });

        assembler
            .handle_event(
                r#"{"type":"response.text.delta","delta":"你好"}"#,
                4_000,
                &cue_sender,
                &speech_tracker,
            )
            .unwrap();

        assert_eq!(cue_receiver.try_recv().unwrap().start_pts_ms, 1_000);
    }

    #[test]
    fn realtime_cue_prefers_the_local_timeline_anchor() {
        let (cue_sender, mut cue_receiver) = unbounded_channel();
        let mut assembler = RealtimeCueAssembler::default();
        let speech_tracker = Mutex::new(RealtimeSpeechTracker::default());
        speech_tracker.lock().unwrap().observe(&Pcm16kChunk {
            pts_ms: 1_000,
            duration_ms: 100,
            samples: vec![],
            rms_energy: 0.02,
        });

        assembler
            .handle_event(
                r#"{"type":"input_audio_buffer.speech_started","audio_start_ms":700}"#,
                1_100,
                &cue_sender,
                &speech_tracker,
            )
            .unwrap();
        assembler
            .handle_event(
                r#"{"type":"response.text.delta","delta":"你好"}"#,
                4_000,
                &cue_sender,
                &speech_tracker,
            )
            .unwrap();

        assert_eq!(cue_receiver.try_recv().unwrap().start_pts_ms, 1_000);
    }

    #[test]
    fn realtime_sentence_punctuation_releases_one_cue_per_sentence() {
        let (cue_sender, mut cue_receiver) = unbounded_channel();
        let mut assembler = RealtimeCueAssembler::default();
        let speech_tracker = Mutex::new(RealtimeSpeechTracker::default());

        assembler
            .handle_event(
                r#"{"type":"response.text.delta","delta":"你好世界，欢迎回来。"}"#,
                4_000,
                &cue_sender,
                &speech_tracker,
            )
            .unwrap();
        let first = cue_receiver.try_recv().unwrap();
        assert_eq!(first.translated_text, "你好世界，欢迎回来。");
        assert!(!first.is_partial);

        assembler
            .handle_event(
                r#"{"type":"response.text.delta","delta":"第二句也完成了。"}"#,
                5_000,
                &cue_sender,
                &speech_tracker,
            )
            .unwrap();
        let second = cue_receiver.try_recv().unwrap();
        assert_eq!(second.translated_text, "第二句也完成了。");
        assert!(!second.is_partial);
        assert_ne!(second.id, first.id);
        assert!(second.start_pts_ms > first.start_pts_ms);
    }

    #[test]
    fn realtime_long_sentence_breaks_at_a_clause_comma() {
        let (cue_sender, mut cue_receiver) = unbounded_channel();
        let mut assembler = RealtimeCueAssembler::default();
        let speech_tracker = Mutex::new(RealtimeSpeechTracker::default());
        let text = "这是一个刚好十八个字的完整句子内容，";

        assembler
            .handle_event(
                &json!({ "type": "response.text.delta", "delta": text }).to_string(),
                4_000,
                &cue_sender,
                &speech_tracker,
            )
            .unwrap();

        let cue = cue_receiver.try_recv().unwrap();
        assert!(!cue.is_partial);
        assert_eq!(cue.translated_text, text);
    }

    #[test]
    fn realtime_forced_split_waits_for_twenty_translated_characters() {
        let (cue_sender, mut cue_receiver) = unbounded_channel();
        let mut assembler = RealtimeCueAssembler::default();
        let speech_tracker = Mutex::new(RealtimeSpeechTracker::default());

        // The source already hits the source-character cap, but only six
        // Chinese characters exist, so the forced split must stay partial.
        let payload = json!({
            "type": "conversation.item.input_audio_transcription.completed",
            "item_id": "item_1",
            "transcript": "a".repeat(REALTIME_CUE_MAX_SOURCE_CHARS)
        })
        .to_string();
        assembler
            .handle_event(&payload, 10_000, &cue_sender, &speech_tracker)
            .unwrap();
        assembler
            .handle_event(
                r#"{"type":"response.text.delta","delta":"只有六个汉字"}"#,
                10_000,
                &cue_sender,
                &speech_tracker,
            )
            .unwrap();

        let mut saw_partial = false;
        while let Ok(cue) = cue_receiver.try_recv() {
            assert!(cue.is_partial);
            saw_partial = true;
        }
        assert!(saw_partial);
    }

    #[test]
    fn realtime_long_translation_without_punctuation_is_force_released() {
        let (cue_sender, mut cue_receiver) = unbounded_channel();
        let mut assembler = RealtimeCueAssembler::default();
        let speech_tracker = Mutex::new(RealtimeSpeechTracker::default());
        let text = "一".repeat(REALTIME_CUE_MAX_TRANSLATED_CHARS);
        let payload = json!({ "type": "response.text.delta", "delta": text }).to_string();

        assembler
            .handle_event(&payload, 4_000, &cue_sender, &speech_tracker)
            .unwrap();

        let cue = cue_receiver.try_recv().unwrap();
        assert!(!cue.is_partial);
        assert_eq!(
            cue.translated_text.chars().count(),
            REALTIME_CUE_MAX_TRANSLATED_CHARS
        );
        assert!(cue_receiver.try_recv().is_err());
    }

    #[test]
    fn realtime_done_text_does_not_repeat_already_released_sentences() {
        let (cue_sender, mut cue_receiver) = unbounded_channel();
        let mut assembler = RealtimeCueAssembler::default();
        let speech_tracker = Mutex::new(RealtimeSpeechTracker::default());

        assembler
            .handle_event(
                r#"{"type":"response.text.delta","delta":"你好世界，欢迎回来。"}"#,
                4_000,
                &cue_sender,
                &speech_tracker,
            )
            .unwrap();
        let first = cue_receiver.try_recv().unwrap();
        assert_eq!(first.translated_text, "你好世界，欢迎回来。");

        assembler
            .handle_event(
                r#"{"type":"response.text.done","text":"你好世界，欢迎回来。第二句也完成了。"}"#,
                5_000,
                &cue_sender,
                &speech_tracker,
            )
            .unwrap();
        let second = cue_receiver.try_recv().unwrap();
        assert_eq!(second.translated_text, "第二句也完成了。");
        assert!(!second.is_partial);
        assert_ne!(second.id, first.id);
    }

    #[test]
    fn realtime_source_items_pair_with_their_released_translations() {
        let (cue_sender, mut cue_receiver) = unbounded_channel();
        let mut assembler = RealtimeCueAssembler::default();
        let speech_tracker = Mutex::new(RealtimeSpeechTracker::default());

        for (payload, latest_audio_pts_ms) in [
            (
                json!({
                    "type": "conversation.item.input_audio_transcription.completed",
                    "item_id": "item_1",
                    "transcript": "Hello world."
                }),
                4_000,
            ),
            (
                json!({ "type": "response.text.delta", "delta": "你好世界，欢迎回来。" }),
                4_200,
            ),
            (
                json!({
                    "type": "conversation.item.input_audio_transcription.completed",
                    "item_id": "item_2",
                    "transcript": "Second sentence."
                }),
                6_000,
            ),
            (
                json!({ "type": "response.text.delta", "delta": "第二句也完成了。" }),
                6_200,
            ),
        ] {
            assembler
                .handle_event(
                    &payload.to_string(),
                    latest_audio_pts_ms,
                    &cue_sender,
                    &speech_tracker,
                )
                .unwrap();
        }

        let mut final_cues = Vec::new();
        while let Ok(cue) = cue_receiver.try_recv() {
            if !cue.is_partial {
                final_cues.push(cue);
            }
        }
        assert_eq!(final_cues.len(), 2);
        let first = &final_cues[0];
        let second = &final_cues[1];
        assert_eq!(first.source_text, "Hello world.");
        assert_eq!(first.translated_text, "你好世界，欢迎回来。");
        assert_eq!(second.source_text, "Second sentence.");
        assert_eq!(second.translated_text, "第二句也完成了。");
        assert_ne!(first.id, second.id);
    }

    #[test]
    fn realtime_model_selects_frame_oriented_bailian_engine() {
        let config = AiSubtitleConfig {
            api_key: Some("test-token".to_string()),
            model: DEFAULT_BAILIAN_REALTIME_MODEL.to_string(),
            route: AiSubtitleRoute::TwoStage,
            ..AiSubtitleConfig::default()
        };
        let (cue_sender, _) = unbounded_channel();
        let engine = create_ai_subtitle_engine(config, cue_sender).unwrap();

        assert!(engine.wants_audio_frames());
    }

    #[test]
    fn realtime_route_selects_realtime_engine_without_realtime_model_name() {
        let config = AiSubtitleConfig {
            api_key: Some("test-token".to_string()),
            route: AiSubtitleRoute::Realtime,
            ..AiSubtitleConfig::default()
        };
        let (cue_sender, _) = unbounded_channel();
        let engine = create_ai_subtitle_engine(config, cue_sender).unwrap();

        assert!(engine.wants_audio_frames());
    }

    #[tokio::test]
    async fn realtime_engine_sends_audio_and_emits_streamed_cues() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            socket
                .send(Message::Text(
                    json!({ "type": "session.created" }).to_string().into(),
                ))
                .await
                .unwrap();

            let update = socket.next().await.unwrap().unwrap();
            let Message::Text(update) = update else {
                panic!("expected session.update text event");
            };
            let update: Value = serde_json::from_str(update.as_ref()).unwrap();
            assert_eq!(update["type"], "session.update");
            assert_eq!(update["session"]["output_modalities"], json!(["text"]));
            assert_eq!(
                update["session"]["audio"]["input"]["format"]["sample_rate"],
                16_000
            );
            socket
                .send(Message::Text(
                    json!({ "type": "session.updated" }).to_string().into(),
                ))
                .await
                .unwrap();

            let audio = socket.next().await.unwrap().unwrap();
            let Message::Text(audio) = audio else {
                panic!("expected input_audio_buffer.append text event");
            };
            let audio: Value = serde_json::from_str(audio.as_ref()).unwrap();
            assert_eq!(audio["type"], "input_audio_buffer.append");
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(audio["audio"].as_str().unwrap())
                .unwrap();
            assert_eq!(decoded.len(), 3_200);

            for event in [
                json!({
                    "type": "conversation.item.input_audio_transcription.delta",
                    "delta": "Hello"
                }),
                json!({ "type": "response.text.delta", "delta": "你好世界" }),
                json!({ "type": "response.done" }),
            ] {
                socket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .unwrap();
            }

            let finish = socket.next().await.unwrap().unwrap();
            let Message::Text(finish) = finish else {
                panic!("expected session.finish text event");
            };
            let finish: Value = serde_json::from_str(finish.as_ref()).unwrap();
            assert_eq!(finish["type"], "session.finish");
            socket
                .send(Message::Text(
                    json!({ "type": "session.finished" }).to_string().into(),
                ))
                .await
                .unwrap();
            let _ = socket.next().await;
        });

        let (cue_sender, mut cue_receiver) = unbounded_channel();
        let config = AiSubtitleConfig {
            api_key: Some("test-token".to_string()),
            base_url: Some(format!("http://{address}/compatible-mode/v1")),
            model: DEFAULT_BAILIAN_REALTIME_MODEL.to_string(),
            ..AiSubtitleConfig::default()
        };
        let mut engine = super::BailianRealtimeEngine::new(config, cue_sender).unwrap();
        engine.start().await.unwrap();
        engine
            .on_frame_pcm(Pcm16kChunk {
                pts_ms: 0,
                duration_ms: 100,
                samples: vec![100; 1_600],
                rms_energy: 0.01,
            })
            .await
            .unwrap();

        let source_partial = tokio::time::timeout(Duration::from_secs(2), cue_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let translation_partial = tokio::time::timeout(Duration::from_secs(2), cue_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let final_cue = tokio::time::timeout(Duration::from_secs(2), cue_receiver.recv())
            .await
            .unwrap()
            .unwrap();

        assert!(source_partial.is_partial);
        assert_eq!(source_partial.source_text, "Hello");
        assert_eq!(translation_partial.id, source_partial.id);
        assert_eq!(translation_partial.translated_text, "你好世界");
        assert!(!final_cue.is_partial);
        assert_eq!(final_cue.id, source_partial.id);

        engine.stop().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn two_stage_emits_partial_then_translation_and_uses_recent_context() {
        async fn recognize() -> Json<Value> {
            Json(json!({ "text": "Hello world" }))
        }

        async fn translate(
            State(requests): State<Arc<Mutex<Vec<Value>>>>,
            Json(request): Json<Value>,
        ) -> Json<Value> {
            requests.lock().unwrap().push(request);
            Json(json!({
                "choices": [{ "message": { "content": "你好" } }]
            }))
        }

        let requests = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/v1/audio/transcriptions", post(recognize))
            .route(
                "/v1/chat/completions",
                post(translate).with_state(requests.clone()),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let (cue_sender, mut cue_receiver) = unbounded_channel();
        let config = AiSubtitleConfig {
            api_key: Some("test-token".to_string()),
            base_url: Some(format!("http://{address}/v1")),
            ..AiSubtitleConfig::default()
        };
        let mut engine = super::BailianTwoStageEngine::new(config, cue_sender).unwrap();
        engine.start().await.unwrap();

        for pts_ms in [0, 3_000] {
            engine
                .on_segment_pcm(Pcm16kChunk {
                    pts_ms,
                    duration_ms: 3_000,
                    samples: vec![100; 48_000],
                    rms_energy: 0.01,
                })
                .await
                .unwrap();
            let partial = cue_receiver.recv().await.unwrap();
            let complete = cue_receiver.recv().await.unwrap();
            assert!(partial.is_partial);
            assert!(!complete.is_partial);
            assert_eq!(partial.id, complete.id);
            assert_eq!(partial.source_text, "Hello world");
            assert_eq!(complete.translated_text, "你好");
        }

        let requests = requests.lock().unwrap();
        let second_prompt = requests[1]["messages"][1]["content"].as_str().unwrap();
        assert!(second_prompt.contains("原文：Hello world"));
        assert!(second_prompt.contains("译文：你好"));
        drop(requests);

        engine.stop().await.unwrap();
        server.abort();
    }
}
