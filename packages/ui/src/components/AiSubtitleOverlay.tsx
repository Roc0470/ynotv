import { useCallback, useEffect, useRef, useState } from 'react';
import { listen } from '@tauri-apps/api/event';
import { invoke } from '@tauri-apps/api/core';
import {
  normalizeAiSubtitleCue,
  normalizeAiSubtitleStatus,
  type AiSubtitleConfig,
  type AiSubtitleCue,
  type AiSubtitleStatusPayload,
} from './aiSubtitleTypes';
import './AiSubtitleOverlay.css';

const CUE_LIFETIME_MS = 7_000;
const POSITION_STORAGE_KEY = 'ynotv_ai_subtitle_position';

interface AiSubtitleOverlayProps {
  visible?: boolean;
}

interface OverlayPosition {
  x: number;
  y: number;
}

interface DragStart extends OverlayPosition {
  pointerId: number;
  clientX: number;
  clientY: number;
}

interface SyncToast {
  offsetMs: number;
  shownAt: number;
}

function readPosition(): OverlayPosition {
  try {
    const stored = localStorage.getItem(POSITION_STORAGE_KEY);
    if (stored) {
      const position = JSON.parse(stored) as Partial<OverlayPosition>;
      if (typeof position.x === 'number' && typeof position.y === 'number') {
        return {
          x: Math.max(10, Math.min(90, position.x)),
          y: Math.max(15, Math.min(90, position.y)),
        };
      }
    }
  } catch (error) {
    console.warn('[AiSubtitleOverlay] Failed to read overlay position:', error);
  }
  return { x: 50, y: 82 };
}

function formatSyncOffset(offsetMs: number): string {
  const seconds = (offsetMs / 1_000).toFixed(1);
  return `${offsetMs > 0 ? '+' : ''}${seconds}s`;
}

export function AiSubtitleOverlay({ visible = true }: AiSubtitleOverlayProps) {
  const [enabled, setEnabled] = useState(false);
  const [status, setStatus] = useState<AiSubtitleStatusPayload>({ status: 'disabled' });
  const [cue, setCue] = useState<AiSubtitleCue | null>(null);
  const [showOriginal, setShowOriginal] = useState(true);
  const [fontSize, setFontSize] = useState(32);
  const [position, setPosition] = useState(readPosition);
  const [lastCueAt, setLastCueAt] = useState(0);
  const [syncToast, setSyncToast] = useState<SyncToast | null>(null);
  const [syncOffsetMs, setSyncOffsetMs] = useState(0);
  const [showSyncControls, setShowSyncControls] = useState(false);
  const syncControlsTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const dragStartRef = useRef<DragStart | null>(null);
  const positionRef = useRef(position);
  positionRef.current = position;

  const acceptCue = useCallback((payload: unknown) => {
    const normalized = normalizeAiSubtitleCue(payload);
    if (normalized === undefined) {
      console.warn('[AiSubtitleOverlay] Ignoring malformed subtitle cue:', payload);
      return;
    }
    if (normalized === null) {
      setCue(null);
      setLastCueAt(0);
      return;
    }
    setCue(normalized);
    setLastCueAt(Date.now());
  }, []);

  const acceptStatus = useCallback((payload: unknown) => {
    const normalized = normalizeAiSubtitleStatus(payload);
    if (!normalized) return;
    setStatus(normalized);
    if (normalized.status === 'disabled' || normalized.status === 'error') {
      setEnabled(false);
      setCue(null);
      setLastCueAt(0);
      setSyncToast(null);
    } else {
      setEnabled(true);
    }
  }, []);

  useEffect(() => {
    let disposed = false;
    const unlisten: Array<() => void> = [];

    const subscribe = async <T,>(
      eventName: string,
      handler: (payload: unknown) => void,
    ) => {
      try {
        const stopListening = await listen<T>(eventName, (event) => handler(event.payload));
        if (disposed) stopListening();
        else unlisten.push(stopListening);
      } catch (error) {
        console.warn(`[AiSubtitleOverlay] Could not listen for ${eventName}:`, error);
      }
    };

    void subscribe('ai-subtitle:cue', acceptCue);
    void subscribe('ai-subtitle://cue', acceptCue);
    void subscribe('ai-subtitle:state', acceptStatus);
    void subscribe('ai-subtitle://status', acceptStatus);
    void subscribe('ai-subtitle:sync-offset', (payload) => {
      if (typeof payload === 'number' && Number.isFinite(payload)) setSyncOffsetMs(payload);
    });

    void invoke<AiSubtitleConfig>('ai_subtitle_get_config')
      .then((config) => {
        if (disposed) return;
        if (typeof config.showOriginal === 'boolean') setShowOriginal(config.showOriginal);
        if (typeof config.fontSize === 'number') setFontSize(config.fontSize);
        if (typeof config.syncOffsetMs === 'number') setSyncOffsetMs(config.syncOffsetMs);
      })
      .catch((error) => {
        console.warn('[AiSubtitleOverlay] Could not load subtitle appearance config:', error);
      });

    return () => {
      disposed = true;
      hideSyncControls();
      unlisten.forEach((stopListening) => stopListening());
    };
  }, [acceptCue, acceptStatus]);

  const hideSyncControls = useCallback(() => {
    if (syncControlsTimerRef.current) {
      clearTimeout(syncControlsTimerRef.current);
      syncControlsTimerRef.current = null;
    }
    setShowSyncControls(false);
  }, []);

  const showSyncControlsTemporarily = useCallback(() => {
    hideSyncControls();
    setShowSyncControls(true);
    syncControlsTimerRef.current = setTimeout(() => {
      setShowSyncControls(false);
      syncControlsTimerRef.current = null;
    }, 1_500);
  }, [hideSyncControls]);

  const adjustSync = useCallback((deltaMs: number) => {
    void invoke<AiSubtitleConfig>('ai_subtitle_adjust_sync', { deltaMs })
      .then((config) => {
        setSyncOffsetMs(config.syncOffsetMs);
        setSyncToast({ offsetMs: config.syncOffsetMs, shownAt: Date.now() });
        showSyncControlsTemporarily();
      })
      .catch((error) => {
        console.error('[AiSubtitleOverlay] Could not adjust subtitle sync offset:', error);
        setSyncToast({ offsetMs: Number.NaN, shownAt: Date.now() });
        showSyncControlsTemporarily();
      });
  }, [showSyncControlsTemporarily]);

  useEffect(() => {
    if (!cue || !lastCueAt || status.status === 'buffering') return;
    const remaining = Math.max(0, CUE_LIFETIME_MS - (Date.now() - lastCueAt));
    const timeout = window.setTimeout(() => setCue(null), remaining);
    return () => window.clearTimeout(timeout);
  }, [cue, lastCueAt, status.status]);

  useEffect(() => {
    if (!syncToast) return;
    const timeout = window.setTimeout(() => setSyncToast(null), 1_500);
    return () => window.clearTimeout(timeout);
  }, [syncToast]);

  useEffect(() => {
    if (!enabled || !visible) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.ctrlKey || event.altKey || event.metaKey || event.repeat) return;
      if (event.target instanceof Element && event.target.closest(
        'input, textarea, select, button, [contenteditable="true"], [role="dialog"]',
      )) return;

      const deltaMs = event.key === '[' ? -500 : event.key === ']' ? 500 : 0;
      if (deltaMs !== 0) {
        event.preventDefault();
        adjustSync(deltaMs);
        return;
      }
      if (event.key === '\\') {
        event.preventDefault();
        void invoke<AiSubtitleConfig>('ai_subtitle_sync_now', {})
          .then((config) => {
            setSyncOffsetMs(config.syncOffsetMs);
            setSyncToast({ offsetMs: config.syncOffsetMs, shownAt: Date.now() });
            showSyncControlsTemporarily();
          })
          .catch((error) => {
            console.error('[AiSubtitleOverlay] Could not sync subtitle to live voice:', error);
          });
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [adjustSync, enabled, visible]);

  const handlePointerDown = (event: React.PointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) return;
    event.currentTarget.setPointerCapture(event.pointerId);
    dragStartRef.current = {
      pointerId: event.pointerId,
      clientX: event.clientX,
      clientY: event.clientY,
      ...positionRef.current,
    };
  };

  const handlePointerMove = (event: React.PointerEvent<HTMLDivElement>) => {
    const start = dragStartRef.current;
    if (!start || start.pointerId !== event.pointerId) return;
    setPosition({
      x: Math.max(10, Math.min(90, start.x + ((event.clientX - start.clientX) / window.innerWidth) * 100)),
      y: Math.max(15, Math.min(90, start.y + ((event.clientY - start.clientY) / window.innerHeight) * 100)),
    });
  };

  const handlePointerUp = (event: React.PointerEvent<HTMLDivElement>) => {
    if (!dragStartRef.current || dragStartRef.current.pointerId !== event.pointerId) return;
    dragStartRef.current = null;
    try {
      localStorage.setItem(POSITION_STORAGE_KEY, JSON.stringify(positionRef.current));
    } catch (error) {
      console.warn('[AiSubtitleOverlay] Failed to save overlay position:', error);
    }
  };

  const isPreparing = status.status === 'preparing';
  const isBuffering = status.status === 'buffering';
  const hasVisibleContent = enabled && (
    (cue !== null && status.status !== 'error') ||
    isPreparing ||
    isBuffering
  );

  return (
    <div
      className="ai-subtitle-overlay"
      aria-live="polite"
      aria-hidden={!visible}
      style={{ display: visible ? undefined : 'none' }}
    >
      {enabled && (isPreparing || isBuffering) && (
        <div className="ai-subtitle-status">
          {isPreparing
            ? status.waitingForTranslation
              ? `✦ 首条译文生成中 · 最多再等约 ${status.secondsLeft ?? 0} 秒`
              : `✦ AI 字幕准备中 · 约 ${status.secondsLeft ?? status.leadSeconds ?? 0} 秒后开始播放`
            : '等待画面缓冲同步中…'}
        </div>
      )}
      {enabled && syncToast && (
        <div key={syncToast.shownAt} className="ai-subtitle-sync-toast visible" role="status">
          {Number.isFinite(syncToast.offsetMs)
            ? `字幕补偿：${formatSyncOffset(syncToast.offsetMs)}`
            : '字幕补偿设置失败，请检查日志'}
        </div>
      )}
      {showSyncControls && (
        <div className="ai-subtitle-sync-controls" aria-label="AI 字幕延迟补偿">
          <button type="button" title="字幕提前 0.5 秒" aria-label="字幕提前 0.5 秒" onClick={() => adjustSync(-500)}>−0.5s</button>
          <span>{formatSyncOffset(syncOffsetMs)}</span>
          <button type="button" title="字幕延后 0.5 秒" aria-label="字幕延后 0.5 秒" onClick={() => adjustSync(500)}>+0.5s</button>
        </div>
      )}
      <div
        className={`ai-subtitle-cue${hasVisibleContent ? ' visible' : ''}${cue?.isPartial ? ' partial' : ''}`}
        style={{
          left: `${position.x}%`,
          top: `${position.y}%`,
          '--ai-subtitle-font-size': `${fontSize}px`,
        } as React.CSSProperties}
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerCancel={handlePointerUp}
        title="拖动调整 AI 字幕位置"
      >
        {cue && (
          <>
            {showOriginal && cue.sourceText && (
              <span className="ai-subtitle-original">
                {cue.sourceText}
                {cue.isPartial && !cue.translatedText && (
                  <span className="ai-subtitle-dots" aria-label="字幕生成中"> •••</span>
                )}
              </span>
            )}
            {cue.translatedText && (
              <span className="ai-subtitle-translation">
                {cue.translatedText}
                {cue.isPartial && (
                  <span className="ai-subtitle-dots" aria-label="字幕生成中"> •••</span>
                )}
              </span>
            )}
          </>
        )}
      </div>
    </div>
  );
}

export default AiSubtitleOverlay;
