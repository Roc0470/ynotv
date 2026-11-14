use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tauri::{AppHandle, Emitter, Listener, Manager, State};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::audio_scout::{self, SharedSubtitleEngine};
use super::engine::create_ai_subtitle_engine;
use super::models::{
    AiSubtitleAudioSource, AiSubtitleConfig, AiSubtitleCue, AiSubtitleRoute, AiSubtitleState,
};

// 海外代理直播源建连与首帧探测耗时较长，放宽至 12 秒避免误判超时并回退到直采。
const PREPARATION_TIMEOUT: Duration = Duration::from_secs(12);
const FIRST_TRANSLATION_WAIT: Duration = Duration::from_secs(3);
const MAX_SCOUT_ATTEMPTS: u32 = 3;
const SCOUT_RETRY_DELAY: Duration = Duration::from_millis(1_500);
const MAX_CUE_HISTORY_MS: u64 = 60_000;
const LATE_CUE_GRACE_MS: i128 = 4_000;
/// 每条字幕最短保持可读时长（毫秒）。
const MIN_CUE_DISPLAY_MS: u64 = 2_000;
/// 每条字幕最长自动延展可读时长（毫秒）。
const MAX_CUE_DISPLAY_MS: u64 = 6_000;
/// Temporary diagnostics counters for live subtitle troubleshooting.
static DIAG_STATUS_LINES: AtomicU64 = AtomicU64::new(0);
static DIAG_ACCEPT_LINES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct AiSubtitleController {
    inner: Arc<Mutex<ControllerInner>>,
}

struct ControllerInner {
    generation: u64,
    active: bool,
    phase_ready: bool,
    playback_advancing: bool,
    /// 主播放器恢复播放后实际推进的播放时长（毫秒），与探路器 0ms 起步时间轴 1:1 对齐。
    main_played_pts_ms: u64,
    /// 共享给 FFmpeg 音频探路器的只读播放进度原子时钟，用于动态控速追赶。
    shared_played_pts_ms: Arc<AtomicU64>,
    last_player_position_ms: Option<u64>,
    sync_offset_ms: i32,
    state: AiSubtitleState,
    cues: VecDeque<AiSubtitleCue>,
    last_emitted_cue: Option<AiSubtitleCue>,
    cancellation: Option<CancellationToken>,
    engine: Option<SharedSubtitleEngine>,
    workers: Vec<JoinHandle<()>>,
    listener_id: Option<tauri::EventId>,
    paused_by_controller: bool,
    stream_url: Option<String>,
    headers: BTreeMap<String, String>,
    /// 当 FFmpeg 探路不可用并回退到 WASAPI 直采时为 true，字幕到达即直接渲染。
    direct_tap_mode: bool,
}

impl Default for AiSubtitleController {
    fn default() -> Self {
        Self::new()
    }
}

impl AiSubtitleController {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ControllerInner {
                generation: 0,
                active: false,
                phase_ready: false,
                playback_advancing: false,
                main_played_pts_ms: 0,
                shared_played_pts_ms: Arc::new(AtomicU64::new(0)),
                last_player_position_ms: None,
                sync_offset_ms: 0,
                state: AiSubtitleState::Disabled,
                cues: VecDeque::new(),
                last_emitted_cue: None,
                cancellation: None,
                engine: None,
                workers: Vec::new(),
                listener_id: None,
                paused_by_controller: false,
                stream_url: None,
                headers: BTreeMap::new(),
                direct_tap_mode: false,
            })),
        }
    }

    pub async fn start(
        &self,
        app: AppHandle,
        stream_url: String,
        headers: HashMap<String, String>,
    ) -> Result<(), String> {
        self.start_with_options(app, stream_url, headers, true).await
    }

    async fn start_with_options(
        &self,
        app: AppHandle,
        stream_url: String,
        headers: HashMap<String, String>,
        reload_player: bool,
    ) -> Result<(), String> {
        if stream_url.trim().is_empty() {
            return Err("直播流地址不能为空".to_string());
        }
        let mut config = super::config::get_current_config(&app)?;
        config.enabled = true;
        // 新流启动时重置手动补偿偏移量为 0，保证 1:1 绝对时间轴开箱对齐。
        config.sync_offset_ms = 0;
        let (cue_sender, cue_receiver) = mpsc::unbounded_channel();
        let mut engine_config = config.clone();
        if engine_config
            .model
            .to_ascii_lowercase()
            .contains("realtime")
        {
            engine_config.route = AiSubtitleRoute::Realtime;
        }
        let engine = create_ai_subtitle_engine(engine_config, cue_sender)?;

        // 停止旧会话时的任何非致命异常都不应中断新频道的字幕启动流程。
        let _ = self.stop_internal(&app, false).await;
        // 必须在拉起探路器前先暂停主播放器（若中途手动开启还需重载流清空旧缓存），防止初始化耗时或缓存错位导致字幕抢跑。
        let prepare_player_result = if reload_player {
            match reload_main_player(&app, stream_url.clone()).await {
                Ok(()) => pause_main_player(&app).await,
                Err(error) => Err(error),
            }
        } else {
            pause_main_player(&app).await
        };
        if let Err(error) = prepare_player_result {
            if let Ok(mut fallback_config) = super::config::get_current_config(&app) {
                fallback_config.enabled = false;
                if let Err(save_error) = super::config::update_config(&app, fallback_config) {
                    log::error!(
                        "[AiSubtitle] 暂停主播放器失败后禁用配置也失败: {save_error}"
                    );
                }
            }
            return Err(error);
        }
        config = match super::config::update_config(&app, config) {
            Ok(config) => config,
            Err(error) => {
                if let Err(resume_error) = resume_main_player(&app).await {
                    log::error!(
                        "[AiSubtitle] 持久化配置失败后恢复主播放器也失败: {resume_error}"
                    );
                }
                return Err(error);
            }
        };
        let engine: SharedSubtitleEngine = Arc::new(tokio::sync::Mutex::new(engine));
        let cancellation = CancellationToken::new();
        let sorted_headers: BTreeMap<String, String> = headers.into_iter().collect();

        let (generation, player_played_ms) = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|error| format!("AI 字幕控制器锁不可用：{error}"))?;
            inner.generation = inner.generation.wrapping_add(1);
            inner.active = true;
            inner.phase_ready = false;
            inner.playback_advancing = false;
            inner.main_played_pts_ms = 0;
            inner.shared_played_pts_ms.store(0, Ordering::SeqCst);
            inner.last_player_position_ms = None;
            inner.sync_offset_ms = config.sync_offset_ms;
            inner.state = AiSubtitleState::Preparing {
                seconds_left: config.lead_seconds as u32,
                lead_seconds: config.lead_seconds as u32,
                waiting_for_translation: false,
            };
            inner.cues.clear();
            inner.last_emitted_cue = None;
            inner.cancellation = Some(cancellation.clone());
            inner.engine = Some(Arc::clone(&engine));
            inner.paused_by_controller = true;
            inner.stream_url = Some(stream_url.clone());
            inner.headers = sorted_headers.clone();
            inner.direct_tap_mode = false;
            (inner.generation, Arc::clone(&inner.shared_played_pts_ms))
        };
        let _ = app.emit("ai-subtitle:sync-offset", config.sync_offset_ms);
        emit_state(
            &app,
            &AiSubtitleState::Preparing {
                seconds_left: config.lead_seconds as u32,
                lead_seconds: config.lead_seconds as u32,
                waiting_for_translation: false,
            },
        );

        let controller = Arc::clone(&self.inner);
        let app_for_status = app.clone();
        let listener_id = app.listen("mpv-status", move |event| {
            update_playback_status(&app_for_status, &controller, generation, event.payload());
        });
        if let Ok(mut inner) = self.inner.lock() {
            inner.listener_id = Some(listener_id);
        }

        let cue_controller = Arc::clone(&self.inner);
        let cue_app = app.clone();
        let cue_cancel = cancellation.clone();
        let cue_task = tokio::spawn(async move {
            let mut receiver = cue_receiver;
            loop {
                tokio::select! {
                    _ = cue_cancel.cancelled() => break,
                    cue = receiver.recv() => match cue {
                        Some(cue) => accept_cue(&cue_app, &cue_controller, generation, cue),
                        None => break,
                    }
                }
            }
        });

        let session_app = app.clone();
        let session_inner = Arc::clone(&self.inner);
        let session_cancel = cancellation.clone();
        let session_url = stream_url;
        let session_headers = sorted_headers;
        // 蓄水时长即为探路器提前于播放器起跑的提前量，保证 1:1 绝对时间轴对齐。
        let lead_ms = (config.lead_seconds as u64).saturating_mul(1_000);
        let session_task = tokio::spawn(async move {
            run_session(
                session_app,
                session_inner,
                generation,
                session_url,
                session_headers,
                engine,
                lead_ms,
                config.audio_source,
                player_played_ms,
                session_cancel,
            )
            .await;
        });
        if let Ok(mut inner) = self.inner.lock() {
            if inner.generation == generation {
                inner.workers.push(cue_task);
                inner.workers.push(session_task);
            } else {
                cue_task.abort();
                session_task.abort();
            }
        }
        Ok(())
    }

    pub async fn stop(&self, app: &AppHandle) -> Result<(), String> {
        self.stop_internal(app, true).await
    }

    async fn stop_internal(&self, app: &AppHandle, resume_player: bool) -> Result<(), String> {
        let (cancellation, engine, workers, listener_id, should_resume, was_active) = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|error| format!("AI 字幕控制器锁不可用：{error}"))?;
            let was_active = inner.active || inner.state != AiSubtitleState::Disabled;
            inner.generation = inner.generation.wrapping_add(1);
            inner.active = false;
            inner.phase_ready = false;
            inner.playback_advancing = false;
            inner.direct_tap_mode = false;
            inner.cues.clear();
            inner.last_emitted_cue = None;
            inner.state = AiSubtitleState::Disabled;
            (
                inner.cancellation.take(),
                inner.engine.take(),
                std::mem::take(&mut inner.workers),
                inner.listener_id.take(),
                std::mem::take(&mut inner.paused_by_controller),
                was_active,
            )
        };

        if let Some(cancellation) = cancellation {
            cancellation.cancel();
        }
        for worker in workers {
            worker.abort();
            let _ = worker.await;
        }
        if let Some(engine) = engine {
            if let Err(error) = engine.lock().await.stop().await {
                log::warn!("[AiSubtitle] Failed to stop subtitle engine (ignored): {error}");
            }
        }
        if let Some(listener_id) = listener_id {
            app.unlisten(listener_id);
        }
        if should_resume && resume_player {
            if let Err(error) = resume_main_player(app).await {
                log::error!("[AiSubtitle] Failed to resume player while stopping: {error}");
            }
        }

        if was_active && resume_player {
            if let Ok(mut config) = super::config::get_current_config(app) {
                config.enabled = false;
                if let Err(error) = super::config::update_config(app, config) {
                    log::error!("[AiSubtitle] Failed to persist disabled config while stopping: {error}");
                }
            }
            emit_state(app, &AiSubtitleState::Disabled);
            let _ = app.emit("ai-subtitle:cue", Option::<AiSubtitleCue>::None);
        }
        Ok(())
    }

    pub async fn on_stream_changed(
        &self,
        app: AppHandle,
        stream_url: String,
        headers: HashMap<String, String>,
    ) -> Result<bool, String> {
        let is_active = {
            let inner = self
                .inner
                .lock()
                .map_err(|error| format!("AI 字幕控制器锁不可用：{error}"))?;
            inner.active
        };
        if !is_active {
            return Ok(false);
        }
        self.start_with_options(app, stream_url, headers, false)
            .await?;
        Ok(true)
    }

    pub fn adjust_sync(&self, app: &AppHandle, delta_ms: i32) -> Result<AiSubtitleConfig, String> {
        let config = super::config::adjust_sync_offset(app, delta_ms)?;
        let cue_update = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|error| format!("AI 字幕控制器锁不可用：{error}"))?;
            inner.sync_offset_ms = config.sync_offset_ms;
            inner.last_emitted_cue = None;
            (select_cue(&mut inner), inner.sync_offset_ms)
        };
        emit_cue_update(app, cue_update.0);
        let _ = app.emit("ai-subtitle:sync-offset", cue_update.1);
        Ok(config)
    }

    pub fn sync_to_live_now(&self, app: &AppHandle) -> Result<AiSubtitleConfig, String> {
        let config = super::config::reset_sync_offset(app)?;
        let maybe_cue = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|error| format!("AI 字幕控制器锁不可用：{error}"))?;
            inner.sync_offset_ms = 0;
            inner.last_emitted_cue = None;
            select_cue(&mut inner)
        };
        emit_cue_update(app, maybe_cue);
        let _ = app.emit("ai-subtitle:sync-offset", 0);
        Ok(config)
    }

    pub fn set_sync_offset(
        &self,
        app: &AppHandle,
        sync_offset_ms: i32,
    ) -> Result<AiSubtitleConfig, String> {
        let config = super::config::set_sync_offset(app, sync_offset_ms)?;
        let maybe_cue = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|error| format!("AI 字幕控制器锁不可用：{error}"))?;
            if inner.sync_offset_ms == config.sync_offset_ms {
                None
            } else {
                inner.sync_offset_ms = config.sync_offset_ms;
                inner.last_emitted_cue = None;
                select_cue(&mut inner)
            }
        };
        emit_cue_update(app, maybe_cue);
        let _ = app.emit("ai-subtitle:sync-offset", config.sync_offset_ms);
        Ok(config)
    }
}

async fn run_session(
    app: AppHandle,
    inner: Arc<Mutex<ControllerInner>>,
    generation: u64,
    stream_url: String,
    headers: BTreeMap<String, String>,
    engine: SharedSubtitleEngine,
    lead_ms: u64,
    audio_source: AiSubtitleAudioSource,
    player_played_ms: Arc<AtomicU64>,
    cancellation: CancellationToken,
) {
    // 探路器与播放器蓄水后统一从 0ms 起步，直接 1:1 对齐时间轴。
    let decoded_audio_ms = Arc::new(AtomicU64::new(0));
    log::debug!("[AiSubtitleDiag] standalone decoded_audio init decoded_initial=0");
    if audio_source == AiSubtitleAudioSource::DirectTap {
        if let Err(error) = engine.lock().await.start().await {
            set_error(&app, &inner, generation, error).await;
            return;
        }
        start_direct_fallback(
            app,
            inner,
            generation,
            engine,
            decoded_audio_ms,
            cancellation,
        )
        .await;
        return;
    }

    let mut scout_session = None;
    let mut engine_started = false;
    let mut attempt = 0_u32;
    while attempt < MAX_SCOUT_ATTEMPTS {
        attempt += 1;
        if attempt > 1 {
            // 探路重试前重载并暂停主播放器，丢弃 mpv 旧缓存并与新启动的 FFmpeg 重新对齐在当前直播边缘。
            let _ = reload_main_player(&app, stream_url.clone()).await;
            let _ = pause_main_player(&app).await;
            if cancellation.is_cancelled() {
                return;
            }
            if let Ok(mut guard) = inner.lock() {
                if guard.generation == generation {
                    guard.main_played_pts_ms = 0;
                    guard.shared_played_pts_ms.store(0, Ordering::SeqCst);
                    guard.last_player_position_ms = None;
                }
            }
        }
        let scout_cancel = cancellation.child_token();
        // 先持有引擎锁再立即拉起 FFmpeg 进程，让 FFmpeg 建连/探测与百炼 WebSocket 握手并发执行。
        let mut engine_guard = if !engine_started {
            Some(engine.lock().await)
        } else {
            None
        };
        let scout = match audio_scout::start_standalone(
            stream_url.clone(),
            headers.clone(),
            Arc::clone(&engine),
            Arc::clone(&decoded_audio_ms),
            Arc::clone(&player_played_ms),
            lead_ms,
            scout_cancel.clone(),
        ) {
            Ok(scout) => scout,
            Err(error) => {
                drop(engine_guard);
                log::warn!(
                    "[AiSubtitle] Standalone scout unavailable (attempt {attempt}/{MAX_SCOUT_ATTEMPTS}): {error}"
                );
                if attempt < MAX_SCOUT_ATTEMPTS && wait_before_scout_retry(&cancellation).await {
                    continue;
                }
                break;
            }
        };
        if let Some(mut guard) = engine_guard.take() {
            let start_result = tokio::select! {
                _ = cancellation.cancelled() => {
                    drop(guard);
                    scout_cancel.cancel();
                    let _ = scout.task.await;
                    return;
                }
                result = guard.start() => result,
            };
            drop(guard);
            if let Err(error) = start_result {
                scout_cancel.cancel();
                let _ = scout.task.await;
                set_error(&app, &inner, generation, error).await;
                return;
            }
            engine_started = true;
        }

        let task = scout.task;
        let mut first_frame = scout.first_frame;
        let readiness = tokio::select! {
            _ = cancellation.cancelled() => {
                scout_cancel.cancel();
                return;
            }
            _ = tokio::time::sleep(PREPARATION_TIMEOUT) => None,
            first = &mut first_frame => Some(first),
        };

        if matches!(&readiness, Some(Ok(Ok(())))) {
            scout_session = Some((task, scout_cancel));
            break;
        }

        scout_cancel.cancel();
        let _ = task.await;
        if cancellation.is_cancelled() {
            return;
        }
        log::warn!(
            "[AiSubtitle] Standalone scout attempt {attempt}/{MAX_SCOUT_ATTEMPTS} did not produce its first frame within {} seconds: {}",
            PREPARATION_TIMEOUT.as_secs(),
            match &readiness {
                Some(Ok(Err(error))) => error.clone(),
                Some(Err(error)) => format!("首帧通知失败：{error}"),
                _ => "首帧超时".to_string(),
            }
        );
        if attempt < MAX_SCOUT_ATTEMPTS && !wait_before_scout_retry(&cancellation).await {
            return;
        }
    }

    let Some((mut task, standalone_cancel)) = scout_session else {
        if !engine_started {
            if let Err(error) = engine.lock().await.start().await {
                set_error(&app, &inner, generation, error).await;
                return;
            }
        }
        start_direct_fallback(
            app,
            inner,
            generation,
            engine,
            decoded_audio_ms,
            cancellation,
        )
        .await;
        return;
    };

    {
            let mut first_translation_deadline = None;
            loop {
                if decoded_audio_ms.load(Ordering::SeqCst) >= lead_ms {
                    let now = tokio::time::Instant::now();
                    let deadline = *first_translation_deadline
                        .get_or_insert_with(|| now + FIRST_TRANSLATION_WAIT);
                    let has_translation = match has_translated_cue(&inner, generation) {
                        Ok(has_translation) => has_translation,
                        Err(error) => {
                            set_error(&app, &inner, generation, error).await;
                            standalone_cancel.cancel();
                            return;
                        }
                    };
                    if !has_translation && now < deadline {
                        let seconds_left =
                            deadline.saturating_duration_since(now).as_secs_f64().ceil() as u32;
                        emit_phase(
                            &app,
                            &inner,
                            generation,
                            AiSubtitleState::Preparing {
                                seconds_left,
                                lead_seconds: (lead_ms / 1_000) as u32,
                                waiting_for_translation: true,
                            },
                        );
                        tokio::select! {
                            _ = cancellation.cancelled() => {
                                standalone_cancel.cancel();
                                return;
                            }
                            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                        }
                        continue;
                    }
                    if !has_translation {
                        log::warn!(
                            "[AiSubtitle] No translated cue was ready within {}s of audio prebuffer; resuming playback",
                            FIRST_TRANSLATION_WAIT.as_secs()
                        );
                    }
                    if let Err(error) = resume_main_player(&app).await {
                        set_error(&app, &inner, generation, error).await;
                        standalone_cancel.cancel();
                        return;
                    }
                    mark_running(&app, &inner, generation);
                    break;
                }
                if cancellation.is_cancelled() {
                    standalone_cancel.cancel();
                    return;
                }

                let decoded = decoded_audio_ms.load(Ordering::SeqCst);
                let seconds_left = lead_ms.saturating_sub(decoded).div_ceil(1_000) as u32;
                emit_phase(
                    &app,
                    &inner,
                    generation,
                    AiSubtitleState::Preparing {
                        seconds_left,
                        lead_seconds: (lead_ms / 1_000) as u32,
                        waiting_for_translation: false,
                    },
                );
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        standalone_cancel.cancel();
                        return;
                    }
                    result = &mut task => {
                        let error = match result {
                            Ok(Err(error)) => error,
                            Ok(Ok(())) => "FFmpeg 音频探路器已提前结束".to_string(),
                            Err(error) => format!("FFmpeg 探路任务失败：{error}"),
                        };
                        log::warn!("[AiSubtitle] Standalone scout failed while preparing: {error}");
                        start_direct_fallback(
                            app,
                            inner,
                            generation,
                            engine,
                            decoded_audio_ms,
                            cancellation,
                        ).await;
                        return;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
            }

            match task.await {
                Ok(Err(error)) => {
                    log::warn!("[AiSubtitle] Standalone scout stopped: {error}");
                    start_direct_fallback(
                        app,
                        inner,
                        generation,
                        engine,
                        decoded_audio_ms,
                        cancellation,
                    )
                    .await;
                }
                Ok(Ok(())) if !cancellation.is_cancelled() => {
                    set_error(
                        &app,
                        &inner,
                        generation,
                        "FFmpeg 音频探路器已停止".to_string(),
                    )
                    .await;
                }
                Err(error) if !cancellation.is_cancelled() => {
                    set_error(
                        &app,
                        &inner,
                        generation,
                        format!("FFmpeg 探路任务失败：{error}"),
                    )
                    .await;
                }
                _ => {}
            }
        }
}

/// Waits out a short backoff before retrying the audio scout.
///
/// IPTV providers frequently answer a burst of playlists with `403 Forbidden`
/// and recover a second later, so a single failed scout should not condemn the
/// session to the latency-bound direct-tap fallback.
async fn wait_before_scout_retry(cancellation: &CancellationToken) -> bool {
    tokio::select! {
        _ = cancellation.cancelled() => false,
        _ = tokio::time::sleep(SCOUT_RETRY_DELAY) => true,
    }
}

async fn start_direct_fallback(
    app: AppHandle,
    inner: Arc<Mutex<ControllerInner>>,
    generation: u64,
    engine: SharedSubtitleEngine,
    decoded_audio_ms: Arc<AtomicU64>,
    cancellation: CancellationToken,
) {
    if cancellation.is_cancelled() || !is_current_session(&inner, generation) {
        return;
    }

    let receiver = match app.try_state::<crate::audio_capture::AudioCaptureState>() {
        Some(state) => state.subscribe_pcm(),
        None => {
            set_error(
                &app,
                &inner,
                generation,
                "WASAPI 音频采集服务不可用".to_string(),
            )
            .await;
            return;
        }
    };
    let Some(start_pts_ms) = main_clock(&inner, generation) else {
        set_error(
            &app,
            &inner,
            generation,
            "无法读取主播放器时间轴".to_string(),
        )
        .await;
        return;
    };
    if let Err(error) = resume_main_player(&app).await {
        set_error(&app, &inner, generation, error).await;
        return;
    }
    if let Ok(mut state) = inner.lock() {
        if state.generation == generation {
            state.direct_tap_mode = true;
            state.paused_by_controller = false;
        }
    }

    let (first_frame, task) = audio_scout::start_direct_tap(
        receiver,
        engine,
        decoded_audio_ms,
        start_pts_ms,
        cancellation.clone(),
    );
    let readiness = tokio::select! {
        _ = cancellation.cancelled() => return,
        _ = tokio::time::sleep(PREPARATION_TIMEOUT) => None,
        first = first_frame => Some(first),
    };
    match readiness {
        Some(Ok(Ok(()))) => mark_running(&app, &inner, generation),
        Some(Ok(Err(error))) => {
            set_error(&app, &inner, generation, error).await;
            cancellation.cancel();
            return;
        }
        Some(Err(error)) => {
            set_error(
                &app,
                &inner,
                generation,
                format!("WASAPI 首帧音频读取失败：{error}"),
            )
            .await;
            cancellation.cancel();
            return;
        }
        None => {
            set_error(
                &app,
                &inner,
                generation,
                format!(
                    "WASAPI 在 {} 秒内没有产出音频",
                    PREPARATION_TIMEOUT.as_secs()
                ),
            )
            .await;
            cancellation.cancel();
            return;
        }
    }

    match task.await {
        Ok(Err(error)) if !cancellation.is_cancelled() => {
            set_error(&app, &inner, generation, error).await;
        }
        Err(error) if !cancellation.is_cancelled() => {
            set_error(
                &app,
                &inner,
                generation,
                format!("WASAPI 采集任务失败：{error}"),
            )
            .await;
        }
        _ => {}
    }
}

fn update_playback_status(
    app: &AppHandle,
    inner: &Arc<Mutex<ControllerInner>>,
    generation: u64,
    payload: &str,
) {
    let Ok(payload) = serde_json::from_str::<Value>(payload) else {
        return;
    };
    let Some(position_ms) = payload
        .get("position")
        .and_then(Value::as_f64)
        .filter(|position| position.is_finite() && *position >= 0.0)
        .map(|position| (position * 1_000.0) as u64)
    else {
        return;
    };
    let playing = payload
        .get("playing")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let paused_for_cache = payload
        .get("pausedForCache")
        .or_else(|| payload.get("paused_for_cache"))
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let (state_to_emit, cue_to_emit, diag_line) = {
        let Ok(mut inner) = inner.lock() else {
            return;
        };
        if inner.generation != generation || !inner.active {
            return;
        }
        advance_main_clock(&mut inner, position_ms, playing, paused_for_cache);
        prune_cues(&mut inner);

        let state_to_emit = if inner.phase_ready {
            let state = if inner.playback_advancing {
                AiSubtitleState::Running
            } else {
                AiSubtitleState::Buffering
            };
            if inner.state != state {
                inner.state = state.clone();
                Some(state)
            } else {
                None
            }
        } else {
            None
        };
        let cue_to_emit = select_cue(&mut inner);
        let diag_line = if DIAG_STATUS_LINES.fetch_add(1, Ordering::Relaxed) % 20 == 0 {
            Some(format!(
                "[AiSubtitleDiag] tick played={} direct_tap={} cues={} phase_ready={} advancing={} state={:?}",
                inner.main_played_pts_ms,
                inner.direct_tap_mode,
                inner.cues.len(),
                inner.phase_ready,
                inner.playback_advancing,
                inner.state,
            ))
        } else {
            None
        };
        (state_to_emit, cue_to_emit, diag_line)
    };

    if let Some(diag_line) = diag_line {
        log::debug!("{diag_line}");
    }

    if let Some(state) = state_to_emit {
        emit_state(app, &state);
    }
    emit_cue_update(app, cue_to_emit);
}

fn accept_cue(
    app: &AppHandle,
    inner: &Arc<Mutex<ControllerInner>>,
    generation: u64,
    cue: AiSubtitleCue,
) {
    let (cue_to_emit, diag_line) = {
        let Ok(mut inner) = inner.lock() else {
            return;
        };
        if inner.generation != generation || !inner.active {
            return;
        }
        let cue_id = cue.id.clone();
        let is_partial = cue.is_partial;
        let source_chars = cue.source_text.chars().count();
        let translated_chars = cue.translated_text.chars().count();
        let (is_new, cue_start_pts_ms, cue_end_pts_ms, cue_to_emit) =
            apply_accepted_cue(&mut inner, cue);
        let selected = match &cue_to_emit {
            Some(Some(selected)) => selected.id.chars().take(8).collect::<String>(),
            Some(None) => "cleared".to_string(),
            None => "unchanged".to_string(),
        };
        let diag_line = format!(
            "[AiSubtitleDiag] accept new={is_new} id={} partial={is_partial} start={cue_start_pts_ms} end={cue_end_pts_ms} src={source_chars} dst={translated_chars} played={} direct_tap={} selected={selected}",
            cue_id.chars().take(8).collect::<String>(),
            inner.main_played_pts_ms,
            inner.direct_tap_mode,
        );
        (cue_to_emit, Some(diag_line))
    };
    if let Some(diag_line) = diag_line {
        let sequence = DIAG_ACCEPT_LINES.fetch_add(1, Ordering::Relaxed);
        if sequence < 1_500 {
            log::debug!("{diag_line}");
        }
    }
    emit_cue_update(app, cue_to_emit);
}

fn apply_accepted_cue(
    inner: &mut ControllerInner,
    mut cue: AiSubtitleCue,
) -> (bool, u64, u64, Option<Option<AiSubtitleCue>>) {
    let is_new = !inner.cues.iter().any(|item| item.id == cue.id);
    if is_new && !is_non_empty_cue(&cue) {
        return (false, cue.start_pts_ms, cue.end_pts_ms, None);
    }

    let source_chars = cue.source_text.chars().count();
    let translated_chars = cue.translated_text.chars().count();
    let duration_ms = display_duration_ms(source_chars, translated_chars);
    cue.end_pts_ms = cue
        .end_pts_ms
        .max(cue.start_pts_ms.saturating_add(duration_ms));

    if is_new {
        // 迟到字幕重锚定保护：若新字幕起点已落后于当前播放进度，直接锚定至当前进度并赋予完整可读时长，永不丢弃。
        if cue.start_pts_ms < inner.main_played_pts_ms {
            cue.start_pts_ms = inner.main_played_pts_ms;
            cue.end_pts_ms = cue.start_pts_ms.saturating_add(duration_ms);
        }
        // 单调递增保护：防止乱序分片导致新字幕起始时间早于队列末条。
        if let Some(prev) = inner.cues.back() {
            if cue.start_pts_ms < prev.start_pts_ms {
                cue.start_pts_ms = prev.start_pts_ms;
                cue.end_pts_ms = cue
                    .end_pts_ms
                    .max(cue.start_pts_ms.saturating_add(duration_ms));
            }
        }
        // 将旧字幕的结束窗口截断至新字幕起始点，确保新字幕到来时立即无缝顶替。
        for existing in inner.cues.iter_mut() {
            if existing.end_pts_ms > cue.start_pts_ms {
                existing.end_pts_ms = cue.start_pts_ms.max(existing.start_pts_ms);
            }
        }
        let final_start = cue.start_pts_ms;
        let final_end = cue.end_pts_ms;
        inner.cues.push_back(cue);
        prune_cues(inner);
        return (true, final_start, final_end, select_cue(inner));
    }

    if let Some(existing) = inner.cues.iter_mut().find(|item| item.id == cue.id) {
        // 同 ID 流式增量更新：若先前只有 ASR 杂音尚无译文而首次带入译文且被重锚定，则采纳更晚起点；否则保留最初起播时间戳并延长至最短可读时长。
        if existing.translated_text.trim().is_empty() && !cue.translated_text.trim().is_empty() {
            cue.start_pts_ms = existing.start_pts_ms.max(cue.start_pts_ms);
        } else {
            cue.start_pts_ms = existing.start_pts_ms;
        }
        cue.end_pts_ms = existing
            .end_pts_ms
            .max(cue.end_pts_ms)
            .max(cue.start_pts_ms.saturating_add(duration_ms));
        // 若流式增量更新到达时剩余显示窗口已不足最短可读时长，则顺延至当前进度之后，避免字幕闪烁或提前消失。
        if cue.end_pts_ms < inner.main_played_pts_ms.saturating_add(MIN_CUE_DISPLAY_MS) {
            cue.end_pts_ms = inner.main_played_pts_ms.saturating_add(duration_ms);
        }
        let final_start = cue.start_pts_ms;
        let final_end = cue.end_pts_ms;
        *existing = cue;
        prune_cues(inner);
        return (false, final_start, final_end, select_cue(inner));
    }

    prune_cues(inner);
    (false, cue.start_pts_ms, cue.end_pts_ms, select_cue(inner))
}

fn advance_main_clock(
    inner: &mut ControllerInner,
    position_ms: u64,
    playing: bool,
    paused_for_cache: bool,
) {
    // 蓄水准备完成前冻结播放时钟，避免准备阶段残留的 mpv-status 事件提前推高时间轴。
    if !inner.phase_ready {
        inner.playback_advancing = false;
        return;
    }
    if let Some(last_position) = inner.last_player_position_ms {
        if playing && !paused_for_cache && position_ms >= last_position {
            inner.main_played_pts_ms = inner
                .main_played_pts_ms
                .saturating_add(position_ms.saturating_sub(last_position));
            inner
                .shared_played_pts_ms
                .store(inner.main_played_pts_ms, Ordering::SeqCst);
        }
    }
    inner.last_player_position_ms = Some(position_ms);
    inner.playback_advancing = playing && !paused_for_cache;
}

fn prune_cues(inner: &mut ControllerInner) {
    let render_pts_ms = inner.main_played_pts_ms;
    while inner.cues.front().is_some_and(|cue| {
        render_pts_ms.saturating_sub(cue.end_pts_ms) > MAX_CUE_HISTORY_MS
    }) {
        inner.cues.pop_front();
    }
}

/// 根据原文/译文字符数计算字幕最短可读显示时长（2000ms ~ 6000ms）。
fn display_duration_ms(source_chars: usize, translated_chars: usize) -> u64 {
    let effective_chars = translated_chars.max(source_chars / 2) as u64;
    (1_200 + effective_chars * 90).clamp(MIN_CUE_DISPLAY_MS, MAX_CUE_DISPLAY_MS)
}

fn is_non_empty_cue(cue: &AiSubtitleCue) -> bool {
    !cue.source_text.trim().is_empty() || !cue.translated_text.trim().is_empty()
}

fn select_cue(inner: &mut ControllerInner) -> Option<Option<AiSubtitleCue>> {
    if !inner.phase_ready || !inner.playback_advancing {
        return None;
    }

    let cue = if inner.direct_tap_mode {
        // WASAPI 直采模式下音频已实时产出，跳过提前量匹配，直接显示未超过宽限期的最新非空字幕。
        let grace_ms = u64::try_from(LATE_CUE_GRACE_MS).unwrap_or(0);
        inner.cues.iter().rev().find(|cue| {
            is_non_empty_cue(cue)
                && inner.main_played_pts_ms <= cue.end_pts_ms.saturating_add(grace_ms)
        })
    } else {
        // 独立探路模式下，探路音频流（0ms 起步）与播放器实际播放进度（0ms 起步）1:1 对齐。
        let render_pts_ms = i128::from(inner.main_played_pts_ms);
        let sync_offset_ms = i128::from(inner.sync_offset_ms);
        inner.cues.iter().rev().find(|cue| {
            let shifted_start = i128::from(cue.start_pts_ms) + sync_offset_ms;
            let shifted_end = i128::from(cue.end_pts_ms) + sync_offset_ms;
            is_non_empty_cue(cue)
                && shifted_start <= render_pts_ms
                && render_pts_ms <= shifted_end + LATE_CUE_GRACE_MS
        })
    };

    if let Some(cue) = cue {
        if inner.last_emitted_cue.as_ref() == Some(cue) {
            return None;
        }
        if inner
            .last_emitted_cue
            .as_ref()
            .is_some_and(|last| cue.start_pts_ms < last.start_pts_ms)
        {
            return None;
        }
        let cue_start_pts_ms = cue.start_pts_ms;
        let backlog_ms = inner.main_played_pts_ms.saturating_sub(cue_start_pts_ms);
        inner.last_emitted_cue = Some(cue.clone());
        log::debug!(
            "[AiSubtitleDiag] show id={} cue_start={} played={} direct_tap={} backlog_ms={}",
            cue.id.chars().take(8).collect::<String>(),
            cue_start_pts_ms,
            inner.main_played_pts_ms,
            inner.direct_tap_mode,
            backlog_ms,
        );
        Some(Some(cue.clone()))
    } else if inner.last_emitted_cue.take().is_some() {
        Some(None)
    } else {
        None
    }
}

fn emit_cue_update(app: &AppHandle, cue_update: Option<Option<AiSubtitleCue>>) {
    match cue_update {
        Some(Some(cue)) => {
            let _ = app.emit("ai-subtitle:cue", cue);
        }
        Some(None) => {
            let _ = app.emit("ai-subtitle:cue", Option::<AiSubtitleCue>::None);
        }
        None => {}
    }
}

fn mark_running(app: &AppHandle, inner: &Arc<Mutex<ControllerInner>>, generation: u64) {
    let cue_to_emit = {
        let Ok(mut inner) = inner.lock() else {
            return;
        };
        if inner.generation != generation || !inner.active {
            return;
        }
        inner.phase_ready = true;
        inner.paused_by_controller = false;
        inner.state = AiSubtitleState::Running;
        inner.playback_advancing = true;
        select_cue(&mut inner)
    };
    emit_state(app, &AiSubtitleState::Running);
    emit_cue_update(app, cue_to_emit);
}

fn emit_phase(
    app: &AppHandle,
    inner: &Arc<Mutex<ControllerInner>>,
    generation: u64,
    state: AiSubtitleState,
) {
    let should_emit = match inner.lock() {
        Ok(mut inner) if inner.generation == generation && inner.active => {
            if inner.state == state {
                false
            } else {
                inner.state = state.clone();
                true
            }
        }
        _ => false,
    };
    if should_emit {
        emit_state(app, &state);
    }
}

async fn set_error(
    app: &AppHandle,
    inner: &Arc<Mutex<ControllerInner>>,
    generation: u64,
    message: String,
) {
    let (state, cancellation, engine, listener_id, should_resume) = match inner.lock() {
        Ok(mut inner) if inner.generation == generation && inner.active => {
            inner.active = false;
            inner.phase_ready = false;
            inner.playback_advancing = false;
            inner.cues.clear();
            inner.last_emitted_cue = None;
            let should_resume = std::mem::take(&mut inner.paused_by_controller);
            inner.state = AiSubtitleState::Error { message };
            (
                Some(inner.state.clone()),
                inner.cancellation.take(),
                inner.engine.take(),
                inner.listener_id.take(),
                should_resume,
            )
        }
        _ => (None, None, None, None, false),
    };
    let Some(mut state) = state else {
        return;
    };
    if let Some(cancellation) = cancellation {
        cancellation.cancel();
    }
    if let Some(engine) = engine {
        if let Err(error) = engine.lock().await.stop().await {
            if let AiSubtitleState::Error { message } = &mut state {
                message.push_str(&format!("；停止字幕引擎失败：{error}"));
            }
        }
    }
    if let Some(listener_id) = listener_id {
        app.unlisten(listener_id);
    }
    if should_resume {
        if let Err(error) = resume_main_player(app).await {
            if let AiSubtitleState::Error { message } = &mut state {
                message.push_str(&format!("；恢复播放器失败：{error}"));
            }
        }
    }
    if let AiSubtitleState::Error { message } = &state {
        match super::config::get_current_config(app) {
            Ok(mut config) => {
                config.enabled = false;
                if let Err(error) = super::config::update_config(app, config) {
                    log::error!("[AiSubtitle] Failed to save disabled config after error: {error}");
                }
            }
            Err(error) => {
                log::error!(
                    "[AiSubtitle] Failed to load config while reporting {message}: {error}"
                );
            }
        }
    }
    emit_state(app, &state);
    let _ = app.emit("ai-subtitle:cue", Option::<AiSubtitleCue>::None);
}

fn main_clock(inner: &Arc<Mutex<ControllerInner>>, generation: u64) -> Option<u64> {
    inner
        .lock()
        .ok()
        .filter(|inner| inner.generation == generation)
        .map(|inner| inner.main_played_pts_ms)
}

fn has_translated_cue(
    inner: &Arc<Mutex<ControllerInner>>,
    generation: u64,
) -> Result<bool, String> {
    let inner = inner
        .lock()
        .map_err(|error| format!("AI 字幕控制器锁不可用：{error}"))?;
    Ok(inner.generation == generation
        && inner
            .cues
            .iter()
            .any(|cue| !cue.translated_text.trim().is_empty()))
}

fn is_current_session(inner: &Arc<Mutex<ControllerInner>>, generation: u64) -> bool {
    inner
        .lock()
        .map(|inner| inner.generation == generation && inner.active)
        .unwrap_or(false)
}

fn emit_state(app: &AppHandle, state: &AiSubtitleState) {
    let _ = app.emit("ai-subtitle:state", state);
}

async fn reload_main_player(app: &AppHandle, url: String) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    if crate::get_player_engine(app).await == crate::PlayerEngine::Sidecar {
        return crate::mpv_windows::load_file(app, url).await;
    }
    crate::mpv_core::load_file(app, url).await
}

async fn pause_main_player(app: &AppHandle) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    if crate::get_player_engine(app).await == crate::PlayerEngine::Sidecar {
        return crate::mpv_windows::pause(app).await;
    }
    crate::mpv_core::pause(app).await
}

async fn resume_main_player(app: &AppHandle) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    if crate::get_player_engine(app).await == crate::PlayerEngine::Sidecar {
        return crate::mpv_windows::resume(app).await;
    }
    crate::mpv_core::resume(app).await
}

#[tauri::command]
pub async fn ai_subtitle_start(
    app: AppHandle,
    controller: State<'_, AiSubtitleController>,
    stream_url: String,
    headers: HashMap<String, String>,
) -> Result<(), String> {
    controller.start(app, stream_url, headers).await
}

#[tauri::command]
pub async fn ai_subtitle_stream_changed(
    app: AppHandle,
    controller: State<'_, AiSubtitleController>,
    stream_url: String,
    headers: HashMap<String, String>,
) -> Result<bool, String> {
    controller.on_stream_changed(app, stream_url, headers).await
}

#[tauri::command]
pub async fn ai_subtitle_stop(
    app: AppHandle,
    controller: State<'_, AiSubtitleController>,
) -> Result<(), String> {
    controller.stop(&app).await
}

#[tauri::command]
pub async fn ai_subtitle_toggle(
    app: AppHandle,
    controller: State<'_, AiSubtitleController>,
    stream_url: String,
    headers: HashMap<String, String>,
) -> Result<bool, String> {
    let is_active = controller
        .inner
        .lock()
        .map_err(|error| format!("AI 字幕控制器锁不可用：{error}"))?
        .active;
    if is_active {
        controller.stop(&app).await?;
        Ok(false)
    } else {
        controller.start(app, stream_url, headers).await?;
        Ok(true)
    }
}

#[tauri::command]
pub fn ai_subtitle_adjust_sync(
    app: AppHandle,
    controller: State<'_, AiSubtitleController>,
    delta_ms: i32,
) -> Result<AiSubtitleConfig, String> {
    controller.adjust_sync(&app, delta_ms)
}

#[tauri::command]
pub fn ai_subtitle_set_sync_offset(
    app: AppHandle,
    controller: State<'_, AiSubtitleController>,
    sync_offset_ms: i32,
) -> Result<AiSubtitleConfig, String> {
    controller.set_sync_offset(&app, sync_offset_ms)
}

#[tauri::command]
pub fn ai_subtitle_sync_now(
    app: AppHandle,
    controller: State<'_, AiSubtitleController>,
) -> Result<AiSubtitleConfig, String> {
    controller.sync_to_live_now(&app)
}

#[cfg(test)]
mod tests {
    use super::{
        advance_main_clock, apply_accepted_cue, display_duration_ms, has_translated_cue,
        select_cue, ControllerInner, MAX_CUE_DISPLAY_MS, MIN_CUE_DISPLAY_MS,
    };
    use crate::ai_subtitle::models::{AiSubtitleCue, AiSubtitleState};
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};

    fn test_inner() -> ControllerInner {
        ControllerInner {
            generation: 1,
            active: true,
            phase_ready: true,
            playback_advancing: true,
            main_played_pts_ms: 1_500,
            shared_played_pts_ms: Arc::new(AtomicU64::new(1_500)),
            last_player_position_ms: None,
            sync_offset_ms: 0,
            state: AiSubtitleState::Running,
            cues: VecDeque::new(),
            last_emitted_cue: None,
            cancellation: None,
            engine: None,
            workers: Vec::new(),
            listener_id: None,
            paused_by_controller: false,
            stream_url: None,
            headers: BTreeMap::new(),
            direct_tap_mode: false,
        }
    }

    #[test]
    fn playback_clock_freezes_while_paused_or_buffering() {
        let mut inner = test_inner();
        inner.main_played_pts_ms = 0;
        inner.last_player_position_ms = None;

        advance_main_clock(&mut inner, 1_000, true, false);
        advance_main_clock(&mut inner, 2_000, true, false);
        assert_eq!(inner.main_played_pts_ms, 1_000);

        advance_main_clock(&mut inner, 8_000, true, true);
        assert_eq!(inner.main_played_pts_ms, 1_000);
        assert!(!inner.playback_advancing);

        advance_main_clock(&mut inner, 8_000, false, false);
        assert_eq!(inner.main_played_pts_ms, 1_000);
        assert!(!inner.playback_advancing);
    }

    #[test]
    fn playback_clock_stays_zero_until_phase_ready() {
        let mut inner = test_inner();
        inner.phase_ready = false;
        inner.main_played_pts_ms = 0;
        inner.last_player_position_ms = None;

        advance_main_clock(&mut inner, 1_000, true, false);
        advance_main_clock(&mut inner, 2_500, true, false);
        assert_eq!(inner.main_played_pts_ms, 0);
        assert_eq!(inner.last_player_position_ms, None);
        assert!(!inner.playback_advancing);

        inner.phase_ready = true;
        advance_main_clock(&mut inner, 2_500, true, false);
        advance_main_clock(&mut inner, 3_000, true, false);
        assert_eq!(inner.main_played_pts_ms, 500);
        assert!(inner.playback_advancing);
    }

    #[test]
    fn preparation_detects_when_a_translation_is_buffered() {
        let inner = Arc::new(Mutex::new(test_inner()));
        assert!(!has_translated_cue(&inner, 1).unwrap());

        inner.lock().unwrap().cues.push_back(AiSubtitleCue {
            id: "translated-cue".to_string(),
            source_text: "hello".to_string(),
            translated_text: "你好".to_string(),
            start_pts_ms: 0,
            end_pts_ms: 1_000,
            is_partial: false,
        });

        assert!(has_translated_cue(&inner, 1).unwrap());
    }

    #[test]
    fn cue_selection_clears_a_subtitle_after_its_late_grace_period() {
        let mut inner = test_inner();
        inner.cues.push_back(AiSubtitleCue {
            id: "cue-1".to_string(),
            source_text: "hello".to_string(),
            translated_text: "你好".to_string(),
            start_pts_ms: 1_000,
            end_pts_ms: 1_500,
            is_partial: false,
        });

        assert!(select_cue(&mut inner).unwrap().is_some());
        inner.main_played_pts_ms = 5_501;
        assert_eq!(select_cue(&mut inner), Some(None));
        assert_eq!(select_cue(&mut inner), None);
    }

    #[test]
    fn late_transcription_is_kept_visible_for_a_bounded_grace_period() {
        let mut inner = test_inner();
        inner.main_played_pts_ms = 4_000;
        inner.cues.push_back(AiSubtitleCue {
            id: "late-cue".to_string(),
            source_text: "hello".to_string(),
            translated_text: "你好".to_string(),
            start_pts_ms: 1_000,
            end_pts_ms: 2_000,
            is_partial: false,
        });

        assert_eq!(
            select_cue(&mut inner).unwrap().unwrap().id,
            "late-cue",
            "a result that arrives shortly after its audio interval should not be dropped"
        );
        inner.main_played_pts_ms = 6_001;
        assert_eq!(select_cue(&mut inner), Some(None));
    }

    #[test]
    fn positive_sync_offset_delays_cue_rendering() {
        let mut inner = test_inner();
        inner.main_played_pts_ms = 1_499;
        inner.sync_offset_ms = 500;
        inner.cues.push_back(AiSubtitleCue {
            id: "delayed-cue".to_string(),
            source_text: "hello".to_string(),
            translated_text: "你好".to_string(),
            start_pts_ms: 1_000,
            end_pts_ms: 2_000,
            is_partial: false,
        });

        assert_eq!(select_cue(&mut inner), None);
        inner.main_played_pts_ms = 1_500;
        assert!(select_cue(&mut inner).unwrap().is_some());
    }

    #[test]
    fn negative_sync_offset_advances_cue_rendering() {
        let mut inner = test_inner();
        inner.main_played_pts_ms = 500;
        inner.sync_offset_ms = -500;
        inner.cues.push_back(AiSubtitleCue {
            id: "early-cue".to_string(),
            source_text: "hello".to_string(),
            translated_text: "你好".to_string(),
            start_pts_ms: 1_000,
            end_pts_ms: 2_000,
            is_partial: false,
        });

        assert!(select_cue(&mut inner).unwrap().is_some());
    }

    #[test]
    fn changing_sync_offset_clears_a_cue_outside_its_shifted_window() {
        let mut inner = test_inner();
        inner.main_played_pts_ms = 1_500;
        inner.cues.push_back(AiSubtitleCue {
            id: "shifted-out-cue".to_string(),
            source_text: "hello".to_string(),
            translated_text: "你好".to_string(),
            start_pts_ms: 1_000,
            end_pts_ms: 2_000,
            is_partial: false,
        });

        assert!(select_cue(&mut inner).unwrap().is_some());
        inner.sync_offset_ms = 6_000;
        assert_eq!(select_cue(&mut inner), Some(None));
    }

    #[test]
    fn sync_offset_is_applied_once_without_mutating_queued_cue_windows() {
        let mut inner = test_inner();
        inner.main_played_pts_ms = 1_500;
        inner.cues.push_back(AiSubtitleCue {
            id: "stable-window".to_string(),
            source_text: "hello".to_string(),
            translated_text: "你好".to_string(),
            start_pts_ms: 1_000,
            end_pts_ms: 2_000,
            is_partial: false,
        });

        let window = (inner.cues[0].start_pts_ms, inner.cues[0].end_pts_ms);
        inner.sync_offset_ms = -500;
        assert!(select_cue(&mut inner).unwrap().is_some());
        assert_eq!((inner.cues[0].start_pts_ms, inner.cues[0].end_pts_ms), window);

        inner.last_emitted_cue = None;
        inner.sync_offset_ms = 500;
        assert!(select_cue(&mut inner).unwrap().is_some());
        assert_eq!((inner.cues[0].start_pts_ms, inner.cues[0].end_pts_ms), window);
    }

    #[test]
    fn cue_selection_emits_updated_partial_text_with_the_same_id() {
        let mut inner = test_inner();
        inner.cues.push_back(AiSubtitleCue {
            id: "realtime-1".to_string(),
            source_text: "Hello".to_string(),
            translated_text: String::new(),
            start_pts_ms: 1_000,
            end_pts_ms: 3_000,
            is_partial: true,
        });

        assert_eq!(select_cue(&mut inner).unwrap().unwrap().translated_text, "");
        inner.cues[0].translated_text = "你好".to_string();
        assert_eq!(
            select_cue(&mut inner).unwrap().unwrap().translated_text,
            "你好"
        );
    }

    fn sample_cue(id: &str, source_text: &str, translated_text: &str, start_pts_ms: u64, end_pts_ms: u64) -> AiSubtitleCue {
        AiSubtitleCue {
            id: id.to_string(),
            source_text: source_text.to_string(),
            translated_text: translated_text.to_string(),
            start_pts_ms,
            end_pts_ms,
            is_partial: false,
        }
    }

    #[test]
    fn apply_accepted_cue_enforces_display_duration_bounds_and_ignores_empty() {
        let mut inner = test_inner();
        inner.main_played_pts_ms = 1_000;

        // 空文本新字幕直接忽略，不污染队列。
        let (is_new, _, _, emitted) = apply_accepted_cue(
            &mut inner,
            sample_cue("empty-cue", "   ", "\n", 1_000, 1_500),
        );
        assert!(!is_new);
        assert!(emitted.is_none());
        assert!(inner.cues.is_empty());

        // 最短显示时长保护（2000ms）。
        assert_eq!(display_duration_ms(2, 2), MIN_CUE_DISPLAY_MS);
        let (_, start, end, _) = apply_accepted_cue(
            &mut inner,
            sample_cue("short-cue", "hi", "嗨", 1_000, 1_200),
        );
        assert_eq!(start, 1_000);
        assert_eq!(end, 1_000 + MIN_CUE_DISPLAY_MS);

        // 最长自动延展阅读时长上限（6000ms）。
        let long_text = "这是一段超过六十个字符的超长翻译字幕内容用于校验最短阅读时长计算上限不会无限制放大到超出六秒钟".repeat(2);
        assert_eq!(
            display_duration_ms(0, long_text.chars().count()),
            MAX_CUE_DISPLAY_MS
        );
    }

    #[test]
    fn apply_accepted_cue_keeps_monotonic_start_and_truncates_previous_cues() {
        let mut inner = test_inner();
        inner.main_played_pts_ms = 500;

        // 第 1 句（流式半成品 -> 增量更新）。
        let mut first_partial = sample_cue("cue-1", "Hello", "你好", 1_000, 1_500);
        first_partial.is_partial = true;
        let (is_new, start, end, emitted) = apply_accepted_cue(&mut inner, first_partial);
        assert!(is_new);
        assert_eq!(start, 1_000);
        assert_eq!(end, 3_000);
        assert!(emitted.is_none());

        // 同 ID 流式更新，保留最初 start_pts_ms 并延长窗口。
        let updated = sample_cue("cue-1", "Hello everyone", "大家好，欢迎收看本期节目", 2_200, 2_400);
        let (is_new, start, end, _) = apply_accepted_cue(&mut inner, updated);
        assert!(!is_new);
        assert_eq!(start, 1_000);
        assert!(end >= 3_000);

        // 第 2 句在 2_500ms 到达：旧句 end_pts_ms 应被截断为 2_500ms，避免遮挡新句。
        let (is_new, start, _, _) = apply_accepted_cue(
            &mut inner,
            sample_cue("cue-2", "Next", "下一句", 2_500, 3_000),
        );
        assert!(is_new);
        assert_eq!(start, 2_500);
        assert_eq!(inner.cues[0].end_pts_ms, 2_500);

        // 乱序到来一条 start_pts_ms 更早的新句时，起点应单调不倒退至 2_500ms。
        let (_, mono_start, _, _) = apply_accepted_cue(
            &mut inner,
            sample_cue("cue-3", "Monotonic", "保持单调", 2_100, 2_600),
        );
        assert_eq!(mono_start, 2_500);
        inner.main_played_pts_ms = 2_500;
        assert_eq!(select_cue(&mut inner).unwrap().unwrap().id, "cue-3");
    }

    #[test]
    fn apply_accepted_cue_reanchors_late_cues_to_never_drop() {
        let mut inner = test_inner();
        // 播放器已走到 20_000ms，新字幕因网络滞后才收到，其原始时间戳只有 5_000ms（落后 15s）。
        inner.main_played_pts_ms = 20_000;
        let (is_new, start, end, emitted) = apply_accepted_cue(
            &mut inner,
            sample_cue("late-new", "Delayed speech", "这条迟到的字幕不会丢", 5_000, 7_000),
        );
        assert!(is_new);
        assert_eq!(start, 20_000);
        assert!(end >= 20_000 + MIN_CUE_DISPLAY_MS);
        assert_eq!(emitted.unwrap().unwrap().id, "late-new");

        // 随着播放推进到 21_500ms，同 ID 增量文本更新时剩余显示不足 2000ms，应自动顺延确保能看清。
        inner.main_played_pts_ms = 21_500;
        let (is_new, _, extended_end, _) = apply_accepted_cue(
            &mut inner,
            sample_cue(
                "late-new",
                "Delayed speech finalized",
                "这条迟到的字幕不会丢，还会顺延确保阅读时间。",
                5_000,
                7_000,
            ),
        );
        assert!(!is_new);
        assert!(extended_end >= 21_500 + MIN_CUE_DISPLAY_MS);
    }

    #[test]
    fn select_cue_matches_one_to_one_without_double_prebuffer_offset() {
        let mut inner = test_inner();
        inner.main_played_pts_ms = 0;
        inner.cues.push_back(sample_cue(
            "cue-at-5s",
            "Welcome back",
            "欢迎回来",
            5_000,
            7_500,
        ));

        // 播放器推进尚未到 5_000ms 时不会提前命中。
        assert_eq!(select_cue(&mut inner), None);

        // 播放进度与探路 5_000ms 1:1 对齐时精准出字。
        inner.main_played_pts_ms = 5_000;
        assert_eq!(
            select_cue(&mut inner).unwrap().unwrap().id,
            "cue-at-5s"
        );
    }

    #[test]
    fn direct_tap_mode_displays_latest_non_empty_cue_immediately() {
        let mut inner = test_inner();
        inner.direct_tap_mode = true;
        inner.main_played_pts_ms = 1_500;

        // 直采模式下即便采集时间戳略领先播放时钟，仍应立即显示最新非空字幕。
        inner.cues.push_back(sample_cue(
            "tap-1",
            "Breaking news",
            "突发新闻",
            2_400,
            4_400,
        ));
        inner.cues.push_back(sample_cue("tap-empty", "", "   ", 2_500, 4_500));

        assert_eq!(select_cue(&mut inner).unwrap().unwrap().id, "tap-1");

        // 超过宽限期（4_400 + 4_000 = 8_400）后清除。
        inner.main_played_pts_ms = 8_401;
        assert_eq!(select_cue(&mut inner), Some(None));
    }
}
