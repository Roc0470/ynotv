import { describe, expect, it } from 'vitest';
import {
  DEFAULT_AI_SUBTITLE_CONFIG,
  normalizeAiSubtitleCue,
  normalizeAiSubtitleStatus,
} from './aiSubtitleTypes';

describe('AI subtitle defaults', () => {
  it('uses DashScope realtime with five-second lead-in by default', () => {
    expect(DEFAULT_AI_SUBTITLE_CONFIG.provider).toBe('dash_scope');
    expect(DEFAULT_AI_SUBTITLE_CONFIG.route).toBe('realtime');
    expect(DEFAULT_AI_SUBTITLE_CONFIG.model).toBe('qwen3.8-livetranslate-flash-realtime');
    expect(DEFAULT_AI_SUBTITLE_CONFIG.leadSeconds).toBe(5);
  });
});

describe('AI subtitle event normalization', () => {
  it('accepts the Rust camelCase cue payload', () => {
    expect(normalizeAiSubtitleCue({
      id: 'cue-1',
      sourceText: 'Hello',
      translatedText: '你好',
      isPartial: false,
    })).toEqual({
      id: 'cue-1',
      sourceText: 'Hello',
      translatedText: '你好',
      isPartial: false,
    });
  });

  it('accepts the documented snake_case partial and final cue payloads', () => {
    expect(normalizeAiSubtitleCue({
      original_text: 'Hello',
      translated_text: '你好',
      is_final: false,
    })).toMatchObject({ sourceText: 'Hello', translatedText: '你好', isPartial: true });

    expect(normalizeAiSubtitleCue({
      original_text: 'Goodbye',
      translated_text: '再见',
      is_final: true,
    })).toMatchObject({ sourceText: 'Goodbye', translatedText: '再见', isPartial: false });
  });

  it('maps alternate state names and rejects malformed event payloads', () => {
    expect(normalizeAiSubtitleStatus({ status: 'streaming' })).toEqual({ status: 'running' });
    expect(normalizeAiSubtitleStatus({ status: 'stopped' })).toEqual({ status: 'disabled' });
    expect(normalizeAiSubtitleStatus({ state: 'preparing', seconds_left: 4 }))
      .toEqual({
        status: 'preparing',
        secondsLeft: 4,
        leadSeconds: undefined,
        waitingForTranslation: undefined,
        message: undefined,
      });
    expect(normalizeAiSubtitleStatus({
      status: 'preparing',
      waiting_for_translation: true,
    })).toMatchObject({ status: 'preparing', waitingForTranslation: true });
    expect(normalizeAiSubtitleStatus({ status: 'unknown' })).toBeNull();
    expect(normalizeAiSubtitleCue({ original_text: 'missing translation' })).toBeUndefined();
  });
});
