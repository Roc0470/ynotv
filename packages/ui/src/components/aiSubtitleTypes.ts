export type AiSubtitleProvider =
  | 'dash_scope'
  | 'gemini'
  | 'zhipu'
  | 'deep_seek'
  | 'tvgo'
  | 'open_ai_compatible';

export type AiSubtitleRoute = 'two_stage' | 'realtime' | 'multimodal';
export type AiSubtitleAudioSource = 'auto_scout' | 'direct_tap';
export type AiSubtitleStatus =
  | 'disabled'
  | 'idle'
  | 'preparing'
  | 'listening'
  | 'running'
  | 'buffering'
  | 'paused'
  | 'error';

export interface AiSubtitleConfig {
  enabled: boolean;
  provider: AiSubtitleProvider;
  route: AiSubtitleRoute;
  apiKey: string | null;
  baseUrl: string | null;
  model: string;
  sourceLanguage: string;
  targetLanguage: string;
  audioSource: AiSubtitleAudioSource;
  leadSeconds: number;
  syncOffsetMs: number;
  fontSize: number;
  showOriginal: boolean;
}

export interface AiSubtitleCue {
  id: string;
  sourceText: string;
  translatedText: string;
  isPartial: boolean;
}

export interface AiSubtitleStatusPayload {
  status: AiSubtitleStatus;
  secondsLeft?: number;
  leadSeconds?: number;
  waitingForTranslation?: boolean;
  message?: string;
}

export const DEFAULT_AI_SUBTITLE_CONFIG: AiSubtitleConfig = {
  enabled: false,
  provider: 'dash_scope',
  route: 'two_stage',
  apiKey: null,
  baseUrl: null,
  model: 'qwen-plus',
  sourceLanguage: 'auto',
  targetLanguage: 'zh',
  audioSource: 'auto_scout',
  leadSeconds: 9,
  syncOffsetMs: 0,
  fontSize: 32,
  showOriginal: true,
};

function asRecord(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === 'object'
    ? value as Record<string, unknown>
    : null;
}

export function normalizeAiSubtitleCue(value: unknown): AiSubtitleCue | null | undefined {
  if (value == null) return null;
  const cue = asRecord(value);
  if (!cue) return undefined;

  const sourceText = cue.sourceText ?? cue.source_text ?? cue.original_text ?? cue.originalText;
  const translatedText = cue.translatedText ?? cue.translated_text ?? cue.translation;
  if (typeof sourceText !== 'string' || typeof translatedText !== 'string') return undefined;

  const partial = cue.isPartial ?? cue.is_partial;
  const isFinal = cue.is_final ?? cue.isFinal;
  const isPartial = typeof partial === 'boolean'
    ? partial
    : typeof isFinal === 'boolean'
      ? !isFinal
      : true;

  return {
    id: typeof cue.id === 'string' ? cue.id : `${sourceText}:${translatedText}`,
    sourceText,
    translatedText,
    isPartial,
  };
}

export function normalizeAiSubtitleStatus(value: unknown): AiSubtitleStatusPayload | null {
  const payload = asRecord(value);
  if (!payload) return null;

  const rawStatus = payload.status ?? payload.state;
  if (typeof rawStatus !== 'string') return null;
  const statusMap: Record<string, AiSubtitleStatus> = {
    disabled: 'disabled',
    stopped: 'disabled',
    idle: 'idle',
    preparing: 'preparing',
    listening: 'listening',
    running: 'running',
    streaming: 'running',
    buffering: 'buffering',
    paused: 'paused',
    error: 'error',
  };
  const status = statusMap[rawStatus.toLowerCase()];
  if (!status) return null;

  const secondsLeft = payload.secondsLeft ?? payload.seconds_left;
  const leadSeconds = payload.leadSeconds ?? payload.lead_seconds;
  const waitingForTranslation = payload.waitingForTranslation ?? payload.waiting_for_translation;
  return {
    status,
    secondsLeft: typeof secondsLeft === 'number' ? secondsLeft : undefined,
    leadSeconds: typeof leadSeconds === 'number' ? leadSeconds : undefined,
    waitingForTranslation: typeof waitingForTranslation === 'boolean'
      ? waitingForTranslation
      : undefined,
    message: typeof payload.message === 'string' ? payload.message : undefined,
  };
}
