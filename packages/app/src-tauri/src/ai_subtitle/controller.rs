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

const PREPARATION_TIMEOUT: Duration = Duration::from_secs(8);
const FIRST_TRANSLATION_WAIT: Duration = Duration::from_secs(3);
const MAX_SCOUT_ATTEMPTS: u32 = 3;
const SCOUT_RETRY_DELAY: Duration = Duration::from_millis(1_500);
const MAX_CUE_HISTORY_MS: u64 = 60_000;
const LATE_CUE_GRACE_MS: i128 = 4_000;
/// Every displayed sentence holds for at least this long so it can be read.
const MIN_CUE_DISPLAY_MS: u64 = 2_000;
const MAX_CUE_DISPLAY_MS: u64 = 6_000;
/// Queued sentences may only trail the picture by this much before the queue
/// catches up (older sentences are cut short so the newest one shows).
const MAX_DISPLAY_LAG_MS: u64 = 4_000;
/// A sentence that is still streaming stays visible while more text arrives.
const PARTIAL_REFRESH_MS: u64 = 1_500;
/// A newly finalized sentence may replace the one on screen once it has been
/// visible this long. Without this the queue would wait out every 2-6s reading
/// window and captions would fall seconds behind the voice.
const MIN_PREEMPT_HOLD_MS: u64 = 1_000;
/// Direct-tap sessions have no prebuffer, so a finalized sentence is already
/// behind the voice; allow it to take over after a shorter anti-flicker hold.
const DIRECT_TAP_PREEMPT_HOLD_MS: u64 = 600;
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
    main_played_pts_ms: u64,
    /// Scout-timeline position that lines up with `main_played_pts_ms == 0`.
    ///
    /// Cues are timestamped on the audio scout clock, which starts as soon as the
    /// scout connects, while the player clock only starts when playback resumes
    /// after the prebuffer. Without this base the two timelines are off by the
    /// whole prebuffer (the configured lead), so every caption shows up late.
    cue_base_pts_ms: u64,
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
    /// Realtime translations arrive on the scout audio clock, which drifts far
    /// ahead of the player clock, so their timestamps cannot be trusted. When
    /// this flag is on, the controller re-anchors every cue to the render clock
    /// and paces sentences through a display queue.
    pace_cue_windows: bool,
    /// Render time at which the next newly finished sentence may appear.
    next_display_start_ms: Option<u64>,
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
                cue_base_pts_ms: 0,
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
                pace_cue_windows: false,
                next_display_start_ms: None,
            })),
        }
    }

    pub async fn start(
        &self,
        app: AppHandle,
        stream_url: String,
        headers: HashMap<String, String>,
    ) -> Result<(), String> {
        if stream_url.trim().is_empty() {
            return Err("直播流地址不能为空".to_string());
        }
        let mut config = super::config::get_current_config(&app)?;
        config.enabled = true;
        let (cue_sender, cue_receiver) = mpsc::unbounded_channel();
        let mut engine_config = config.clone();
        if engine_config
            .model
            .to_ascii_lowercase()
            .contains("realtime")
        {
            engine_config.route = AiSubtitleRoute::Realtime;
        }
        let pace_cue_windows = engine_config.route == AiSubtitleRoute::Realtime;
        let mut engine = create_ai_subtitle_engine(engine_config, cue_sender)?;

        self.stop(&app).await?;
        engine.start().await?;
        config = match super::config::update_config(&app, config) {
            Ok(config) => config,
            Err(error) => {
                if let Err(stop_error) = engine.stop().await {
                    log::error!(
                        "[AiSubtitle] Failed to stop engine after config persistence failed: {stop_error}"
                    );
                }
                return Err(error);
            }
        };
        let engine: SharedSubtitleEngine = Arc::new(tokio::sync::Mutex::new(engine));
        let cancellation = CancellationToken::new();
        let sorted_headers: BTreeMap<String, String> = headers.into_iter().collect();

        if let Err(error) = pause_main_player(&app).await {
            if let Err(stop_error) = engine.lock().await.stop().await {
                log::error!(
                    "[AiSubtitle] Failed to stop engine after player pause failed: {stop_error}"
                );
            }
            if let Ok(mut config) = super::config::get_current_config(&app) {
                config.enabled = false;
                if let Err(save_error) = super::config::update_config(&app, config) {
                    log::error!(
                        "[AiSubtitle] Failed to disable config after player pause failed: {save_error}"
                    );
                }
            }
            return Err(error);
        }

        let generation = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|error| format!("AI 字幕控制器锁不可用：{error}"))?;
            inner.generation = inner.generation.wrapping_add(1);
            inner.active = true;
            inner.phase_ready = false;
            inner.playback_advancing = false;
            inner.main_played_pts_ms = 0;
            inner.cue_base_pts_ms = 0;
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
            inner.pace_cue_windows = pace_cue_windows;
            inner.next_display_start_ms = None;
            inner.generation
        };
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
        let lead_ms = config.lead_seconds as u64 * 1_000;
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
            inner.cues.clear();
            inner.last_emitted_cue = None;
            inner.next_display_start_ms = None;
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
        for mut worker in workers {
            match tokio::time::timeout(Duration::from_secs(2), &mut worker).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    log::warn!("[AiSubtitle] Background task stopped with an error: {error}");
                }
                Err(_) => {
                    worker.abort();
                    let _ = worker.await;
                }
            }
        }
        let engine_result = if let Some(engine) = engine {
            engine.lock().await.stop().await
        } else {
            Ok(())
        };
        if let Err(error) = &engine_result {
            log::error!("[AiSubtitle] Failed to stop subtitle engine: {error}");
        }
        if let Some(listener_id) = listener_id {
            app.unlisten(listener_id);
        }
        let resume_result = if should_resume {
            resume_main_player(app).await
        } else {
            Ok(())
        };
        if let Err(error) = &resume_result {
            log::error!("[AiSubtitle] Failed to resume player while stopping: {error}");
        }

        if was_active {
            let mut config = super::config::get_current_config(app)?;
            config.enabled = false;
            super::config::update_config(app, config)?;
            emit_state(app, &AiSubtitleState::Disabled);
            let _ = app.emit("ai-subtitle:cue", Option::<AiSubtitleCue>::None);
        }
        resume_result.and(engine_result)
    }

    pub async fn on_stream_changed(
        &self,
        app: AppHandle,
        stream_url: String,
        headers: HashMap<String, String>,
    ) -> Result<bool, String> {
        let is_active = self
            .inner
            .lock()
            .map_err(|error| format!("AI 字幕控制器锁不可用：{error}"))?
            .active;
        if !is_active {
            return Ok(false);
        }
        self.start(app, stream_url, headers).await?;
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
    cancellation: CancellationToken,
) {
    let initial_main_played_ms = main_clock(&inner, generation).unwrap_or(0);
    let decoded_audio_ms = Arc::new(AtomicU64::new(initial_main_played_ms));
    log::warn!(
        "[AiSubtitleDiag] standalone decoded_audio init main_played={} decoded_initial={}",
        initial_main_played_ms,
        decoded_audio_ms.load(Ordering::SeqCst),
    );
    if audio_source == AiSubtitleAudioSource::DirectTap {
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
    let mut attempt = 0_u32;
    while attempt < MAX_SCOUT_ATTEMPTS {
        attempt += 1;
        let scout_cancel = cancellation.child_token();
        let scout = match audio_scout::start_standalone(
            stream_url.clone(),
            headers.clone(),
            Arc::clone(&engine),
            Arc::clone(&decoded_audio_ms),
            scout_cancel.clone(),
        ) {
            Ok(scout) => scout,
            Err(error) => {
                log::warn!(
                    "[AiSubtitle] Standalone scout unavailable (attempt {attempt}/{MAX_SCOUT_ATTEMPTS}): {error}"
                );
                if attempt < MAX_SCOUT_ATTEMPTS && wait_before_scout_retry(&cancellation).await {
                    continue;
                }
                break;
            }
        };

        let mut task = scout.task;
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
                    if let Ok(mut state) = inner.lock() {
                        if state.generation == generation {
                            let scout_pts = decoded_audio_ms
                                .load(Ordering::SeqCst)
                                .min(state.main_played_pts_ms + lead_ms);
                            sync_cue_base(&mut state, scout_pts);
                        }
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
            state.paused_by_controller = false;
            sync_cue_base(&mut state, start_pts_ms);
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
                "[AiSubtitleDiag] tick render={} played={} base={} cues={} phase_ready={} advancing={} state={:?}",
                inner.main_played_pts_ms.saturating_add(inner.cue_base_pts_ms),
                inner.main_played_pts_ms,
                inner.cue_base_pts_ms,
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
        log::warn!("{diag_line}");
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
        let mut cue = cue;
        let is_new = !inner.cues.iter().any(|item| item.id == cue.id);
        if inner.pace_cue_windows {
            if cue.is_partial {
                let render_now_ms = inner
                    .main_played_pts_ms
                    .saturating_add(inner.cue_base_pts_ms);
                hold_displayed_cue(&mut inner, render_now_ms);
            }
            if is_new {
                assign_new_cue_window(&mut inner, &mut cue);
            } else if let Some((existing_start, existing_end)) = inner
                .cues
                .iter()
                .find(|item| item.id == cue.id)
                .map(|item| (item.start_pts_ms, item.end_pts_ms))
            {
                let engine_start_pts_ms = cue.start_pts_ms;
                cue.start_pts_ms = existing_start;
                extend_cue_window(&mut inner, &mut cue, existing_end, engine_start_pts_ms);
            }
        }
        let cue_start_pts_ms = cue.start_pts_ms;
        let cue_end_pts_ms = cue.end_pts_ms;
        let cue_id = cue.id.clone();
        let is_partial = cue.is_partial;
        let source_chars = cue.source_text.chars().count();
        let translated_chars = cue.translated_text.chars().count();
        if let Some(existing) = inner.cues.iter_mut().find(|item| item.id == cue.id) {
            *existing = cue;
        } else {
            inner.cues.push_back(cue);
        }
        prune_cues(&mut inner);
        let render_pts_ms = inner.main_played_pts_ms.saturating_add(inner.cue_base_pts_ms);
        let cue_to_emit = select_cue(&mut inner);
        let selected = match &cue_to_emit {
            Some(Some(selected)) => selected.id.chars().take(8).collect::<String>(),
            Some(None) => "cleared".to_string(),
            None => "unchanged".to_string(),
        };
        let diag_line = format!(
            "[AiSubtitleDiag] accept new={is_new} id={} partial={is_partial} start={cue_start_pts_ms} end={cue_end_pts_ms} src={source_chars} dst={translated_chars} render={render_pts_ms} played={} base={} selected={selected}",
            cue_id.chars().take(8).collect::<String>(),
            inner.main_played_pts_ms,
            inner.cue_base_pts_ms,
        );
        (cue_to_emit, Some(diag_line))
    };
    if let Some(diag_line) = diag_line {
        let sequence = DIAG_ACCEPT_LINES.fetch_add(1, Ordering::Relaxed);
        if sequence < 1_500 {
            log::warn!("{diag_line}");
        }
    }
    emit_cue_update(app, cue_to_emit);
}

fn advance_main_clock(
    inner: &mut ControllerInner,
    position_ms: u64,
    playing: bool,
    paused_for_cache: bool,
) {
    if let Some(last_position) = inner.last_player_position_ms {
        if playing && !paused_for_cache && position_ms >= last_position {
            inner.main_played_pts_ms = inner
                .main_played_pts_ms
                .saturating_add(position_ms.saturating_sub(last_position));
        }
    }
    inner.last_player_position_ms = Some(position_ms);
    inner.playback_advancing = playing && !paused_for_cache;
}

fn prune_cues(inner: &mut ControllerInner) {
    let render_pts_ms = inner
        .main_played_pts_ms
        .saturating_add(inner.cue_base_pts_ms);
    while inner.cues.front().is_some_and(|cue| {
        render_pts_ms.saturating_sub(cue.end_pts_ms) > MAX_CUE_HISTORY_MS
    }) {
        inner.cues.pop_front();
    }
}

/// Reading time for one sentence: roughly 11 characters per second with a
/// floor of 2 seconds and a ceiling of 6 seconds. English source text counts
/// at half weight because words are longer than Chinese characters.
fn display_duration_ms(source_chars: usize, translated_chars: usize) -> u64 {
    let effective_chars = translated_chars.max(source_chars / 2) as u64;
    (1_200 + effective_chars * 90).clamp(MIN_CUE_DISPLAY_MS, MAX_CUE_DISPLAY_MS)
}

/// Places a newly finished sentence on the render timeline at the moment the
/// viewer will actually hear it. Engine timestamps live on the scout clock,
/// which runs ahead of playback, so they are mapped through
/// `cue_base_pts_ms` first; a sentence whose audio has already been heard
/// starts right away. Sentences are then queued in order so they never
/// overlap, and the queue is trimmed when it would trail the audio too far.
fn assign_new_cue_window(inner: &mut ControllerInner, cue: &mut AiSubtitleCue) {
    let render_now_ms = inner
        .main_played_pts_ms
        .saturating_add(inner.cue_base_pts_ms);
    let engine_start_pts_ms = cue.start_pts_ms;
    // The direct-tap fallback starts with an uncalibrated base of 0. On that
    // path the sample-driven capture clock can run ahead of the player clock,
    // so a future timestamp is drift, not audio the viewer has not heard yet.
    // Anchor those sentences to the render clock instead of making them wait.
    // 将 scout 时钟上的时间戳映射到画面渲染时钟：render = scout - cue_base_pts_ms
    // 原代码错误地使用了 saturating_add，导致首条字幕的锚点偏差等于 cue_base_pts_ms
    //（通常就是 lead_seconds 设置的秒数），表现为刚开播时字幕随机快/慢几秒。
    let heard_at_ms = if inner.cue_base_pts_ms == 0 {
        render_now_ms
    } else {
        cue.start_pts_ms
            .saturating_sub(inner.cue_base_pts_ms)
            .max(render_now_ms)
    };
    let mut start_pts_ms = inner
        .next_display_start_ms
        .take()
        .unwrap_or(heard_at_ms)
        .max(heard_at_ms);
    if start_pts_ms.saturating_sub(heard_at_ms) > MAX_DISPLAY_LAG_MS {
        start_pts_ms = heard_at_ms;
    }
    let end_pts_ms = start_pts_ms.saturating_add(display_duration_ms(
        cue.source_text.chars().count(),
        cue.translated_text.chars().count(),
    ));
    cue.start_pts_ms = start_pts_ms;
    cue.end_pts_ms = end_pts_ms;
    inner.next_display_start_ms = Some(end_pts_ms);
    preempt_finished_cue(inner, cue, render_now_ms, engine_start_pts_ms);
}

/// A sentence that just became final may take over the screen once the
/// sentence before it has been visible for `MIN_PREEMPT_HOLD_MS`, instead of
/// waiting out that sentence's full 2-6s reading window. Without this the
/// display queue builds a 2-4s lag behind the voice.
fn preempt_finished_cue(
    inner: &mut ControllerInner,
    cue: &mut AiSubtitleCue,
    render_now_ms: u64,
    engine_start_pts_ms: u64,
) -> bool {
    if cue.is_partial {
        return false;
    }
    let direct_tap = inner.cue_base_pts_ms == 0;
    // Outside the direct-tap path the engine timestamp is ahead of playback; a
    // sentence may only take over once its audio has actually been heard.
    // engine_start_pts_ms 是 scout 时钟，render_now_ms 是画面时钟，需减去 cue_base_pts_ms 对齐。
    let audio_reached = engine_start_pts_ms
        .saturating_sub(inner.cue_base_pts_ms)
        <= render_now_ms;
    if !direct_tap && !audio_reached {
        return false;
    }
    let Some((prev_start_ms, prev_end_ms)) = inner
        .cues
        .iter()
        .rev()
        .find(|item| item.id != cue.id && !item.is_partial)
        .map(|item| (item.start_pts_ms, item.end_pts_ms))
    else {
        // Nothing is on screen yet, so the normal placement already starts this
        // sentence immediately; there is no tail to cut short.
        return false;
    };
    let target_start_ms = if direct_tap {
        // No prebuffer means the audio has already been heard: the only wait
        // allowed is a short anti-flicker hold after the previous sentence.
        render_now_ms.max(prev_start_ms.saturating_add(DIRECT_TAP_PREEMPT_HOLD_MS))
    } else {
        // The previous sentence has already left the screen, so the normal
        // "start right now" path handles this cue.
        if prev_end_ms <= render_now_ms {
            return false;
        }
        render_now_ms.max(prev_start_ms.saturating_add(MIN_PREEMPT_HOLD_MS))
    };
    // Cut the older sentences short so `select_cue` switches to this one at
    // `target_start_ms` instead of waiting out their remaining tails. When the
    // target is "now", end them just before now so this tick must pick the new
    // sentence.
    let zero_latency = target_start_ms == render_now_ms;
    for item in inner.cues.iter_mut() {
        if item.id == cue.id {
            continue;
        }
        if zero_latency {
            if item.end_pts_ms >= render_now_ms {
                item.end_pts_ms = render_now_ms.saturating_sub(1);
            }
        } else if item.end_pts_ms > target_start_ms {
            item.end_pts_ms = target_start_ms.max(item.start_pts_ms.saturating_add(1));
        }
    }
    cue.start_pts_ms = target_start_ms;
    cue.end_pts_ms = target_start_ms.saturating_add(display_duration_ms(
        cue.source_text.chars().count(),
        cue.translated_text.chars().count(),
    ));
    inner.next_display_start_ms = Some(target_start_ms.saturating_add(MIN_PREEMPT_HOLD_MS));
    true
}

/// Refreshes the window of a sentence that is still arriving or has just been
/// finalized, keeping the slot it already holds on the render timeline.
/// `stored_end_ms` is the slot end previously assigned by the controller; the
/// engine's raw timestamps must not leak back into the render timeline.
fn extend_cue_window(
    inner: &mut ControllerInner,
    cue: &mut AiSubtitleCue,
    stored_end_ms: u64,
    engine_start_pts_ms: u64,
) {
    let render_now_ms = inner
        .main_played_pts_ms
        .saturating_add(inner.cue_base_pts_ms);
    // A sentence that just became final may take the screen over early.
    if preempt_finished_cue(inner, cue, render_now_ms, engine_start_pts_ms) {
        return;
    }
    let duration_ms = display_duration_ms(
        cue.source_text.chars().count(),
        cue.translated_text.chars().count(),
    );
    // A finished sentence is shown as soon as its polished translation lands.
    // If that happens after its audio slot, re-anchor the slot to "now" so the
    // viewer still gets the full reading time instead of a flicker.
    if !cue.is_partial && cue.start_pts_ms < render_now_ms {
        cue.start_pts_ms = render_now_ms;
    }
    let max_end_ms = cue
        .start_pts_ms
        .saturating_add(MAX_CUE_DISPLAY_MS);
    let desired_end_ms = if cue.is_partial {
        render_now_ms.saturating_add(PARTIAL_REFRESH_MS)
    } else {
        cue.start_pts_ms.saturating_add(duration_ms)
    };
    cue.end_pts_ms = stored_end_ms
        .max(desired_end_ms)
        .min(max_end_ms)
        .max(cue.start_pts_ms.saturating_add(1));
    let next = inner.next_display_start_ms.get_or_insert(cue.end_pts_ms);
    if *next < cue.end_pts_ms {
        *next = cue.end_pts_ms;
    }
}

/// Keeps the sentence that is currently on screen alive while the next one is
/// still waiting for its first translated character, so the subtitle never
/// blinks out between sentences.
fn hold_displayed_cue(inner: &mut ControllerInner, render_now_ms: u64) {
    let Some(shown_id) = inner.last_emitted_cue.as_ref().map(|cue| cue.id.clone()) else {
        return;
    };
    let Some(shown_start_ms) = inner
        .cues
        .iter()
        .find(|cue| cue.id == shown_id)
        .map(|cue| cue.start_pts_ms)
    else {
        return;
    };
    // Never stretch the sentence on screen over a newer sentence that has
    // already been finalized and is waiting for its turn.
    let newer_finished_waiting = inner.cues.iter().any(|cue| {
        cue.id != shown_id && !cue.is_partial && cue.start_pts_ms >= shown_start_ms
    });
    if newer_finished_waiting {
        return;
    }
    if let Some(shown) = inner.cues.iter_mut().find(|cue| cue.id == shown_id) {
        let hold_until_ms = render_now_ms
            .saturating_add(PARTIAL_REFRESH_MS)
            .min(shown.start_pts_ms.saturating_add(MAX_CUE_DISPLAY_MS));
        shown.end_pts_ms = shown.end_pts_ms.max(hold_until_ms);
    }
}

fn select_cue(inner: &mut ControllerInner) -> Option<Option<AiSubtitleCue>> {
    if !inner.phase_ready || !inner.playback_advancing {
        return None;
    }
    let render_pts_ms = i128::from(
        inner
            .main_played_pts_ms
            .saturating_add(inner.cue_base_pts_ms),
    );
    let sync_offset_ms = i128::from(inner.sync_offset_ms);
    let cue_base_pts_ms = i128::from(inner.cue_base_pts_ms);
    let paced = inner.pace_cue_windows;
    // Show partial (streaming) translations as soon as they arrive so the
    // viewer does not have to wait for the whole sentence to be translated.
    // Finished sentences still take over when their window arrives.
    let showable = |_cue: &AiSubtitleCue| true;
    let cue = inner
        .cues
        .iter()
            .rev()
            .find(|cue| {
                showable(cue)
                && i128::from(cue.start_pts_ms) + cue_base_pts_ms + sync_offset_ms <= render_pts_ms
                && i128::from(cue.end_pts_ms) + cue_base_pts_ms + sync_offset_ms >= render_pts_ms
            })
        .or_else(|| {
            inner.cues.iter().rev().find(|cue| {
                showable(cue)
                    && i128::from(cue.start_pts_ms) + cue_base_pts_ms + sync_offset_ms <= render_pts_ms
                    && render_pts_ms > i128::from(cue.end_pts_ms) + cue_base_pts_ms + sync_offset_ms
                    && render_pts_ms
                        - (i128::from(cue.end_pts_ms) + cue_base_pts_ms + sync_offset_ms)
                        <= LATE_CUE_GRACE_MS
            })
        });
    if let Some(cue) = cue {
        if inner.last_emitted_cue.as_ref() == Some(cue) {
            return None;
        }
        let cue_start_pts_ms = cue.start_pts_ms;
        let jumped = inner
            .last_emitted_cue
            .as_ref()
            .is_some_and(|last| cue_start_pts_ms < last.start_pts_ms);
        let latest_audio_pts_ms = inner
            .main_played_pts_ms
            .saturating_add(inner.cue_base_pts_ms);
        let backlog_ms = latest_audio_pts_ms.saturating_sub(cue_start_pts_ms);
        inner.last_emitted_cue = Some(cue.clone());
        log::warn!(
            "[AiSubtitleDiag] show id={} audio_pts={} display_pts={} base={} delta={} backlog_ms={} jumped={}",
            cue.id.chars().take(8).collect::<String>(),
            cue_start_pts_ms,
            render_pts_ms,
            inner.cue_base_pts_ms,
            render_pts_ms - (i128::from(cue_start_pts_ms) + i128::from(inner.cue_base_pts_ms)),
            backlog_ms,
            jumped,
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

/// Aligns the scout timeline with the player timeline the moment playback resumes:
/// `scout_pts` points at the audio the viewer is hearing right now, so every later
/// player position maps to `scout_pts + elapsed`.
fn sync_cue_base(inner: &mut ControllerInner, scout_pts_ms: u64) {
    let new_base_pts_ms = scout_pts_ms.saturating_sub(inner.main_played_pts_ms);
    log::warn!(
        "[AiSubtitleDiag] sync_cue_base scout_pts={} main_played={} new_base={}",
        scout_pts_ms,
        inner.main_played_pts_ms,
        new_base_pts_ms,
    );
    if inner.pace_cue_windows && new_base_pts_ms > inner.cue_base_pts_ms {
        // Cues accepted before playback resumed were paced on a render clock
        // that had not started yet; shift their slots onto the real timeline.
        let delta_ms = new_base_pts_ms - inner.cue_base_pts_ms;
        for cue in &mut inner.cues {
            cue.start_pts_ms = cue.start_pts_ms.saturating_add(delta_ms);
            cue.end_pts_ms = cue.end_pts_ms.saturating_add(delta_ms);
        }
        if let Some(next) = inner.next_display_start_ms.as_mut() {
            *next = next.saturating_add(delta_ms);
        }
    }
    inner.cue_base_pts_ms = new_base_pts_ms;
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
        advance_main_clock, assign_new_cue_window, display_duration_ms, extend_cue_window,
        has_translated_cue, hold_displayed_cue, select_cue, sync_cue_base, ControllerInner,
        MIN_CUE_DISPLAY_MS,
    };
    use crate::ai_subtitle::models::{AiSubtitleCue, AiSubtitleState};
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    fn test_inner() -> ControllerInner {
        ControllerInner {
            generation: 1,
            active: true,
            phase_ready: true,
            playback_advancing: true,
            main_played_pts_ms: 1_500,
            cue_base_pts_ms: 0,
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
            pace_cue_windows: false,
            next_display_start_ms: None,
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

    fn paced_cue(id: &str, start_pts_ms: u64, end_pts_ms: u64) -> AiSubtitleCue {
        AiSubtitleCue {
            id: id.to_string(),
            source_text: String::new(),
            translated_text: String::new(),
            start_pts_ms,
            end_pts_ms,
            is_partial: true,
        }
    }

    #[test]
    fn paced_cues_appear_when_the_viewer_hears_them() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.main_played_pts_ms = 100_000;
        inner.cue_base_pts_ms = 20_000;
        // The calibrated scout clock is 20s ahead of playback: the sentence's
        // audio at 120s on the scout clock is heard 20s from now, so its window
        // starts there.
        let mut cue = paced_cue("scout-cue", 120_000, 125_000);

        assign_new_cue_window(&mut inner, &mut cue);

        assert_eq!(cue.start_pts_ms, 120_000);
        assert_eq!(cue.end_pts_ms, 122_000);
        assert_eq!(inner.next_display_start_ms, Some(122_000));
    }

    #[test]
    fn uncalibrated_direct_tap_does_not_wait_for_the_capture_clock() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.cue_base_pts_ms = 0;
        inner.main_played_pts_ms = 100_000;
        // The direct tap clock is 2.4s ahead of the render clock. That future
        // timestamp is drift, not audio the viewer has not heard yet, so the
        // finished sentence must start now instead of waiting 2.4s.
        let mut cue = paced_cue("direct-tap-cue", 102_400, 105_000);

        assign_new_cue_window(&mut inner, &mut cue);

        assert_eq!(cue.start_pts_ms, 100_000);
        assert_eq!(cue.end_pts_ms, 102_000);
        assert_eq!(inner.next_display_start_ms, Some(102_000));
    }

    #[test]
    fn paced_cues_whose_audio_was_already_heard_start_immediately() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.main_played_pts_ms = 100_000;
        // This sentence belongs to audio 5s in the past (translation lagged).
        let mut cue = paced_cue("late-cue", 95_000, 98_000);

        assign_new_cue_window(&mut inner, &mut cue);

        assert_eq!(cue.start_pts_ms, 100_000);
        assert_eq!(cue.end_pts_ms, 102_000);
    }

    #[test]
    fn paced_sentences_queue_one_after_another_with_a_minimum_hold() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.main_played_pts_ms = 50_000;

        for index in 0..3 {
            let mut cue = paced_cue(&format!("cue-{index}"), 50_000, 55_000);
            assign_new_cue_window(&mut inner, &mut cue);
            assert_eq!(cue.start_pts_ms, 50_000 + index * MIN_CUE_DISPLAY_MS);
        }
        assert_eq!(inner.next_display_start_ms, Some(56_000));
    }

    #[test]
    fn paced_queue_is_trimmed_once_it_trails_the_audio_by_four_seconds() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.main_played_pts_ms = 50_000;
        inner.cue_base_pts_ms = 10_000;

        for index in 0..4 {
            let mut cue = paced_cue(
                &format!("cue-{index}"),
                50_000 + index * MIN_CUE_DISPLAY_MS,
                55_000,
            );
            assign_new_cue_window(&mut inner, &mut cue);
        }
        assert_eq!(inner.next_display_start_ms, Some(62_000));

        // The next sentence is already being heard, but the queue would hold it
        // back; the cap trims the wait so it shows right away.
        let mut next = paced_cue("queued-cue", 50_000, 55_000);
        assign_new_cue_window(&mut inner, &mut next);

        assert_eq!(next.start_pts_ms, 62_000);
        assert_eq!(inner.next_display_start_ms, Some(64_000));
    }

    #[test]
    fn finalized_sentences_hold_for_their_reading_time() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.main_played_pts_ms = 10_000;
        let mut cue = paced_cue("final-cue", 10_000, 12_000);
        assign_new_cue_window(&mut inner, &mut cue);
        assert_eq!(cue.end_pts_ms, 12_000);

        cue.is_partial = false;
        cue.translated_text = "这是一个大约二十个字的句子用来测试时长。".to_string();
        extend_cue_window(&mut inner, &mut cue, 12_000, 10_000);
        let expected_end =
            cue.start_pts_ms + display_duration_ms(0, cue.translated_text.chars().count());
        assert_eq!(cue.end_pts_ms, expected_end);
        assert!(cue.end_pts_ms > 12_000);
    }

    #[test]
    fn partial_sentences_are_shown_as_they_stream() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.main_played_pts_ms = 1_500;
        inner.cues.push_back(AiSubtitleCue {
            id: "streaming".to_string(),
            source_text: "Hello".to_string(),
            translated_text: String::new(),
            start_pts_ms: 1_000,
            end_pts_ms: 3_000,
            is_partial: true,
        });

        // Partial sentences are shown as soon as they arrive instead of
        // waiting for the full translation.
        assert!(select_cue(&mut inner).unwrap().is_some());

        // Streaming translation updates the same cue and re-emits it.
        inner.cues[0].translated_text = "你好".to_string();
        assert!(select_cue(&mut inner).unwrap().is_some());

        // A finalized sentence is also shown.
        inner.cues[0].is_partial = false;
        assert!(select_cue(&mut inner).unwrap().is_some());
    }

    #[test]
    fn a_finished_sentence_that_arrives_late_gets_a_full_reading_slot() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.main_played_pts_ms = 10_000;
        let mut cue = paced_cue("late-final", 10_000, 12_000);
        assign_new_cue_window(&mut inner, &mut cue);

        // The polished translation only lands 3s after the audio slot started.
        inner.main_played_pts_ms = 13_000;
        cue.is_partial = false;
        cue.translated_text = "你好世界".to_string();
        let stored_end = cue.end_pts_ms;
        extend_cue_window(&mut inner, &mut cue, stored_end, 10_000);

        assert_eq!(cue.start_pts_ms, 13_000);
        assert_eq!(cue.end_pts_ms, 13_000 + display_duration_ms(0, 4));
    }

    #[test]
    fn an_untranslated_sentence_keeps_the_displayed_one_alive() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.main_played_pts_ms = 1_500;
        inner.cues.push_back(AiSubtitleCue {
            id: "shown".to_string(),
            source_text: "Hello".to_string(),
            translated_text: "你好".to_string(),
            start_pts_ms: 1_000,
            end_pts_ms: 2_000,
            is_partial: false,
        });
        assert!(select_cue(&mut inner).unwrap().is_some());

        hold_displayed_cue(&mut inner, 2_500);

        assert_eq!(inner.cues[0].end_pts_ms, 4_000);
    }

    #[test]
    fn partial_windows_stop_growing_at_the_maximum_hold() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.main_played_pts_ms = 10_000;
        let mut cue = paced_cue("long-partial", 10_000, 12_000);
        assign_new_cue_window(&mut inner, &mut cue);

        let mut stored_end = cue.end_pts_ms;
        for step in 1..=6 {
            inner.main_played_pts_ms = 10_000 + step * 1_000;
            extend_cue_window(&mut inner, &mut cue, stored_end, 10_000);
            stored_end = cue.end_pts_ms;
        }

        // The window may slide forward while text streams in, but it must stop
        // at start + 6s so it can never outlive the sentences that follow it.
        assert_eq!(cue.end_pts_ms, 16_000);
    }

    #[test]
    fn refreshing_a_window_ignores_the_engines_future_timestamps() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.main_played_pts_ms = 10_000;
        let mut cue = paced_cue("engine-end-cue", 10_000, 12_000);
        assign_new_cue_window(&mut inner, &mut cue);

        // The engine stamps this sentence with its scout-clock end (125s), which
        // is far in the future; it must not stretch the paced window.
        cue.end_pts_ms = 125_000;
        cue.is_partial = false;
        extend_cue_window(&mut inner, &mut cue, 12_000, 10_000);

        assert_eq!(cue.end_pts_ms, 12_000);
        assert_eq!(inner.next_display_start_ms, Some(12_000));
    }

    #[test]
    fn a_newly_finalized_sentence_preempts_after_a_short_direct_tap_hold() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.cue_base_pts_ms = 0;
        inner.main_played_pts_ms = 10_000;

        // The first sentence owns the screen with a full reading window.
        let mut first = paced_cue("first-cue", 10_000, 12_000);
        first.is_partial = false;
        first.translated_text = "第一句已经定稿".to_string();
        assign_new_cue_window(&mut inner, &mut first);
        assert_eq!(first.end_pts_ms, 12_000);
        inner.cues.push_back(first);

        // The next sentence is already final but was queued behind the first
        // one's tail. On the direct tap it takes over 600ms after the first
        // one appeared.
        inner.main_played_pts_ms = 10_500;
        let mut second = paced_cue("second-cue", 10_200, 12_000);
        second.is_partial = false;
        second.translated_text = "第二句也已经定稿".to_string();
        inner.cues.push_back(second.clone());
        let stored_end = second.end_pts_ms;
        extend_cue_window(&mut inner, &mut second, stored_end, 10_200);

        assert_eq!(second.start_pts_ms, 10_600);
        assert_eq!(inner.cues[0].end_pts_ms, 10_600);
        assert_eq!(
            second.end_pts_ms,
            10_600 + display_duration_ms(0, second.translated_text.chars().count())
        );
        assert_eq!(inner.next_display_start_ms, Some(11_600));

        // At the handover instant the new sentence is the one on screen.
        inner.main_played_pts_ms = 10_600;
        let shown = select_cue(&mut inner).unwrap().unwrap();
        assert_eq!(shown.id, "second-cue");
    }

    #[test]
    fn a_direct_tap_sentence_takes_over_on_the_current_tick_once_the_old_one_has_aged() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.cue_base_pts_ms = 0;
        inner.main_played_pts_ms = 12_000;
        inner.cues.push_back(AiSubtitleCue {
            id: "old-cue".to_string(),
            source_text: "Old".to_string(),
            translated_text: "旧句子还在屏幕上".to_string(),
            start_pts_ms: 10_000,
            end_pts_ms: 15_000,
            is_partial: false,
        });

        let mut cue = paced_cue("new-cue", 11_800, 13_000);
        cue.is_partial = false;
        cue.translated_text = "新句子已经定稿".to_string();
        // In the real accept path the cue is already stored before the window is
        // refreshed, so mirror that here.
        inner.cues.push_back(cue.clone());
        let stored_end = cue.end_pts_ms;
        extend_cue_window(&mut inner, &mut cue, stored_end, 11_800);

        // The old sentence has been visible for 2s, so the new one starts now
        // and the old one ends just before now so this tick selects the new one.
        assert_eq!(cue.start_pts_ms, 12_000);
        assert_eq!(inner.cues[0].end_pts_ms, 11_999);
        let shown = select_cue(&mut inner).unwrap().unwrap();
        assert_eq!(shown.id, "new-cue");
    }

    #[test]
    fn resuming_playback_shifts_paced_prep_windows_onto_the_real_timeline() {
        let mut inner = test_inner();
        inner.pace_cue_windows = true;
        inner.main_played_pts_ms = 0;
        // During preparation the render clock has not started, so the first
        // translated sentence is slotted at 0..2s.
        let mut cue = paced_cue("prep-cue", 0, 0);
        assign_new_cue_window(&mut inner, &mut cue);
        assert_eq!((cue.start_pts_ms, cue.end_pts_ms), (0, 2_000));
        inner.cues.push_back(cue);

        sync_cue_base(&mut inner, 5_900);

        let stored = &inner.cues[0];
        assert_eq!(inner.cue_base_pts_ms, 5_900);
        assert_eq!((stored.start_pts_ms, stored.end_pts_ms), (5_900, 7_900));
        assert_eq!(inner.next_display_start_ms, Some(7_900));
    }
}
