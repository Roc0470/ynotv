import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { invoke } from '@tauri-apps/api/core';
import {
  DEFAULT_AI_SUBTITLE_CONFIG,
  type AiSubtitleConfig,
  type AiSubtitleProvider,
} from './aiSubtitleTypes';
import './AiSubtitleSettingsModal.css';

interface AiSubtitleSettingsModalProps {
  isOpen: boolean;
  active: boolean;
  onClose: () => void;
  onApplyEnabled: (enabled: boolean) => Promise<void>;
}

interface ConnectionTestResult {
  success: boolean;
  latencyMs: number | null;
  message: string;
}

const PROVIDER_DEFAULTS: Record<AiSubtitleProvider, { label: string; model: string; baseUrl: string }> = {
  dash_scope: {
    label: '阿里百炼（DashScope）',
    model: 'qwen-plus',
    baseUrl: 'https://dashscope.aliyuncs.com/compatible-mode/v1',
  },
  gemini: {
    label: 'Google Gemini',
    model: 'gemini-2.5-flash',
    baseUrl: 'https://generativelanguage.googleapis.com/v1beta/openai/chat/completions',
  },
  zhipu: {
    label: '智谱 GLM',
    model: 'glm-4-flash',
    baseUrl: 'https://open.bigmodel.cn/api/paas/v4/chat/completions',
  },
  deep_seek: {
    label: 'DeepSeek',
    model: 'deepseek-chat',
    baseUrl: 'https://api.deepseek.com/v1',
  },
  tvgo: {
    label: 'TVGO',
    model: 'qwen-plus',
    baseUrl: 'http://127.0.0.1:11470/v1',
  },
  open_ai_compatible: {
    label: 'OpenAI 兼容服务',
    model: 'qwen-plus',
    baseUrl: 'https://api.openai.com/v1',
  },
};

function providerEndpoint(provider: AiSubtitleProvider): string {
  if (provider === 'dash_scope') return PROVIDER_DEFAULTS.dash_scope.baseUrl;
  return PROVIDER_DEFAULTS[provider].baseUrl;
}

export function AiSubtitleSettingsModal({
  isOpen,
  active,
  onClose,
  onApplyEnabled,
}: AiSubtitleSettingsModalProps) {
  const [config, setConfig] = useState<AiSubtitleConfig>(DEFAULT_AI_SUBTITLE_CONFIG);
  const [apiKeyVisible, setApiKeyVisible] = useState(false);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [connectionResult, setConnectionResult] = useState<ConnectionTestResult | null>(null);
  const [formError, setFormError] = useState<string | null>(null);

  useEffect(() => {
    if (!isOpen) return;
    let disposed = false;
    setLoading(true);
    setFormError(null);
    setConnectionResult(null);
    invoke<AiSubtitleConfig>('ai_subtitle_get_config')
      .then((saved) => {
        if (!disposed) {
          setConfig({ ...DEFAULT_AI_SUBTITLE_CONFIG, ...saved });
          setApiKeyVisible(false);
        }
      })
      .catch((error) => {
        console.error('[AiSubtitleSettingsModal] Could not load config:', error);
        if (!disposed) setFormError(String(error));
      })
      .finally(() => {
        if (!disposed) setLoading(false);
      });
    return () => {
      disposed = true;
    };
  }, [isOpen]);

  useEffect(() => {
    if (!isOpen) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && !saving && !testing) onClose();
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [isOpen, onClose, saving, testing]);

  if (!isOpen) return null;

  const setField = <K extends keyof AiSubtitleConfig>(key: K, value: AiSubtitleConfig[K]) => {
    setConfig((current) => ({ ...current, [key]: value }));
    setFormError(null);
  };

  const handleProviderChange = (provider: AiSubtitleProvider) => {
    const defaults = PROVIDER_DEFAULTS[provider];
    setConfig((current) => ({
      ...current,
      provider,
      model: defaults.model,
      baseUrl: providerEndpoint(provider),
    }));
    setConnectionResult(null);
    setFormError(null);
  };

  const handleTestConnection = async () => {
    setTesting(true);
    setConnectionResult(null);
    setFormError(null);
    try {
      const result = await invoke<ConnectionTestResult>('ai_subtitle_test_connection', {
        baseUrl: config.baseUrl?.trim() || '',
        apiKey: config.apiKey?.trim() || '',
        model: config.model.trim() || undefined,
      });
      setConnectionResult(result);
    } catch (error) {
      setFormError(String(error));
    } finally {
      setTesting(false);
    }
  };

  const handleSave = async () => {
    setSaving(true);
    setFormError(null);
    try {
      const saved = await invoke<AiSubtitleConfig>('ai_subtitle_save_config', {
        config: {
          ...config,
          apiKey: config.apiKey?.trim() || null,
          baseUrl: config.baseUrl?.trim() || null,
          model: config.model.trim(),
        },
      });
      setConfig({ ...DEFAULT_AI_SUBTITLE_CONFIG, ...saved });
      if (saved.enabled !== active) await onApplyEnabled(saved.enabled);
      await invoke<AiSubtitleConfig>('ai_subtitle_set_sync_offset', {
        syncOffsetMs: saved.syncOffsetMs,
      });
      onClose();
    } catch (error) {
      setFormError(String(error));
    } finally {
      setSaving(false);
    }
  };

  return createPortal(
    <div
      className="ai-subtitle-modal-backdrop"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget && !saving && !testing) onClose();
      }}
    >
      <section
        className="ai-subtitle-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="ai-subtitle-modal-title"
      >
        <header className="ai-subtitle-modal-header">
          <div>
            <span className="ai-subtitle-modal-eyebrow">ynoTV · AI</span>
            <h2 id="ai-subtitle-modal-title">AI 同传设置</h2>
          </div>
          <button type="button" className="ai-subtitle-modal-close" onClick={onClose} aria-label="关闭">
            ×
          </button>
        </header>

        <div className="ai-subtitle-modal-body">
          {loading ? (
            <div className="ai-subtitle-modal-loading">正在读取配置…</div>
          ) : (
            <>
              <label className="ai-subtitle-setting-toggle">
                <span>
                  <strong>开启 AI 同传</strong>
                  <small>保存时会立即启动或停止当前字幕会话</small>
                </span>
                <input
                  type="checkbox"
                  checked={config.enabled}
                  onChange={(event) => setField('enabled', event.target.checked)}
                  disabled={saving}
                />
              </label>

              <label className="ai-subtitle-setting-field">
                <span>AI 服务商</span>
                <select
                  value={config.provider}
                  onChange={(event) => handleProviderChange(event.target.value as AiSubtitleProvider)}
                >
                  {Object.entries(PROVIDER_DEFAULTS).map(([provider, item]) => (
                    <option key={provider} value={provider}>{item.label}</option>
                  ))}
                </select>
                {config.provider !== 'dash_scope' && (
                  <small>该服务商的字幕引擎尚未接入；可保存并测试连接，但启动时会提示暂不支持。</small>
                )}
              </label>

              <label className="ai-subtitle-setting-field">
                <span>Base URL</span>
                <input
                  type="url"
                  value={config.baseUrl ?? ''}
                  onChange={(event) => setField('baseUrl', event.target.value)}
                  placeholder="https://…"
                  autoComplete="url"
                />
              </label>

              <label className="ai-subtitle-setting-field">
                <span>模型</span>
                <input
                  type="text"
                  value={config.model}
                  onChange={(event) => setField('model', event.target.value)}
                  autoComplete="off"
                />
              </label>

              <label className="ai-subtitle-setting-field">
                <span>API Key</span>
                <div className="ai-subtitle-key-field">
                  <input
                    type={apiKeyVisible ? 'text' : 'password'}
                    value={config.apiKey ?? ''}
                    onChange={(event) => setField('apiKey', event.target.value)}
                    autoComplete="new-password"
                    placeholder="输入服务商 API Key"
                  />
                  <button
                    type="button"
                    onClick={() => setApiKeyVisible((visible) => !visible)}
                    aria-label={apiKeyVisible ? '隐藏 API Key' : '显示 API Key'}
                  >
                    {apiKeyVisible ? '隐藏' : '显示'}
                  </button>
                </div>
              </label>

              <label className="ai-subtitle-setting-field">
                <span className="ai-subtitle-range-heading">
                  <span>字幕提前量</span>
                  <strong>{config.leadSeconds} 秒</strong>
                </span>
                <input
                  type="range"
                  min={2}
                  max={20}
                  step={1}
                  value={config.leadSeconds}
                  onChange={(event) => setField('leadSeconds', Number(event.target.value))}
                />
                <small>启用时短暂暂停画面，为语音识别与翻译预留时间</small>
              </label>

              <div className="ai-subtitle-setting-field">
                <span className="ai-subtitle-range-heading">
                  <span>字幕延迟补偿（秒）</span>
                  <strong>{config.syncOffsetMs > 0 ? '+' : ''}{(config.syncOffsetMs / 1000).toFixed(1)} 秒</strong>
                </span>
                <input
                  type="range"
                  min={-6}
                  max={6}
                  step={0.1}
                  value={config.syncOffsetMs / 1000}
                  onChange={(event) => setField('syncOffsetMs', Math.round(Number(event.target.value) * 1000))}
                />
                <small>正数表示字幕延后显示，负数表示提前显示。字幕比声音快时往正方向调，字幕比声音慢时往负方向调。播放时按 [ / ] 可快捷微调 500ms。</small>
                <button
                  type="button"
                  className="ai-subtitle-reset-sync"
                  onClick={() => setField('syncOffsetMs', 0)}
                >
                  重置为 0 秒
                </button>
              </div>

              <label className="ai-subtitle-setting-field">
                <span>音频采集来源</span>
                <select
                  value={config.audioSource}
                  onChange={(event) => setField('audioSource', event.target.value as AiSubtitleConfig['audioSource'])}
                >
                  <option value="auto_scout">自动探路（FFmpeg，失败时回退 WASAPI）</option>
                  <option value="direct_tap">WASAPI 系统直采（不建立额外连接）</option>
                </select>
                <small>
                  {config.audioSource === 'direct_tap'
                    ? '直采只能获取正在播放的音频，无法提前预翻译；想减少字幕滞后请使用自动探路。'
                    : '探路成功时会预缓存音频并等首条译文（最多 3 秒）；失败回退直采时仍可能有模型处理延迟。'}
                </small>
              </label>

              <label className="ai-subtitle-setting-field">
                <span className="ai-subtitle-range-heading">
                  <span>译文字幕字号</span>
                  <strong>{config.fontSize}px</strong>
                </span>
                <input
                  type="range"
                  min={20}
                  max={48}
                  step={1}
                  value={config.fontSize}
                  onChange={(event) => setField('fontSize', Number(event.target.value))}
                />
              </label>

              <label className="ai-subtitle-setting-toggle">
                <span>
                  <strong>显示双语对照</strong>
                  <small>在译文上方显示原语言识别文本</small>
                </span>
                <input
                  type="checkbox"
                  checked={config.showOriginal}
                  onChange={(event) => setField('showOriginal', event.target.checked)}
                />
              </label>

              <div className="ai-subtitle-connection-test">
                <button type="button" onClick={handleTestConnection} disabled={testing || loading || saving}>
                  {testing ? '正在测试…' : '测试连接'}
                </button>
                {connectionResult && (
                  <p className={connectionResult.success ? 'success' : 'failure'}>
                    <span>{connectionResult.message}</span>
                    {connectionResult.latencyMs != null && <strong>{connectionResult.latencyMs} ms</strong>}
                  </p>
                )}
              </div>
              {formError && <p className="ai-subtitle-form-error">{formError}</p>}
            </>
          )}
        </div>

        <footer className="ai-subtitle-modal-footer">
          <button type="button" className="secondary" onClick={onClose} disabled={saving || testing}>取消</button>
          <button type="button" className="primary" onClick={handleSave} disabled={loading || saving || testing}>
            {saving ? '保存中…' : '保存设置'}
          </button>
        </footer>
      </section>
    </div>,
    document.body,
  );
}

export default AiSubtitleSettingsModal;
