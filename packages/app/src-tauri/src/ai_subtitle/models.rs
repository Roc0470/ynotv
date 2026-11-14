use serde::{Deserialize, Serialize};

/// User-configurable provider and presentation settings for AI subtitles.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct AiSubtitleConfig {
    pub enabled: bool,
    pub provider: AiSubtitleProvider,
    pub route: AiSubtitleRoute,
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    pub model: String,
    pub source_language: String,
    pub target_language: String,
    pub audio_source: AiSubtitleAudioSource,
    pub lead_seconds: u8,
    pub sync_offset_ms: i32,
    pub font_size: u8,
    pub show_original: bool,
}

impl Default for AiSubtitleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: AiSubtitleProvider::DashScope,
            route: AiSubtitleRoute::TwoStage,
            api_key: None,
            base_url: None,
            model: "qwen-plus".to_string(),
            source_language: "auto".to_string(),
            target_language: "zh".to_string(),
            audio_source: AiSubtitleAudioSource::AutoScout,
            lead_seconds: 5,
            sync_offset_ms: 0,
            font_size: 32,
            show_original: true,
        }
    }
}

impl std::fmt::Debug for AiSubtitleConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AiSubtitleConfig")
            .field("enabled", &self.enabled)
            .field("provider", &self.provider)
            .field("route", &self.route)
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("source_language", &self.source_language)
            .field("target_language", &self.target_language)
            .field("audio_source", &self.audio_source)
            .field("lead_seconds", &self.lead_seconds)
            .field("sync_offset_ms", &self.sync_offset_ms)
            .field("font_size", &self.font_size)
            .field("show_original", &self.show_original)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AiSubtitleAudioSource {
    #[default]
    AutoScout,
    DirectTap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AiSubtitleProvider {
    #[default]
    DashScope,
    Zhipu,
    Gemini,
    DeepSeek,
    Tvgo,
    OpenAiCompatible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AiSubtitleRoute {
    #[default]
    TwoStage,
    Realtime,
    Multimodal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiSubtitleCue {
    pub id: String,
    pub source_text: String,
    pub translated_text: String,
    pub start_pts_ms: u64,
    pub end_pts_ms: u64,
    pub is_partial: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AiSubtitleState {
    Disabled,
    Idle,
    Preparing {
        #[serde(rename = "secondsLeft")]
        seconds_left: u32,
        #[serde(rename = "leadSeconds")]
        lead_seconds: u32,
        #[serde(rename = "waitingForTranslation")]
        waiting_for_translation: bool,
    },
    Listening,
    Running,
    Buffering,
    Paused,
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pcm16kChunk {
    pub pts_ms: u64,
    pub duration_ms: u32,
    pub samples: Vec<i16>,
    pub rms_energy: f32,
}

#[cfg(test)]
mod tests {
    use super::{AiSubtitleConfig, AiSubtitleState};

    #[test]
    fn config_defaults_when_fields_are_missing() {
        let config: AiSubtitleConfig = serde_json::from_str("{}").unwrap();

        assert_eq!(config.model, "qwen-plus");
        assert_eq!(config.lead_seconds, 5);
        assert_eq!(config.target_language, "zh");
    }

    #[test]
    fn debug_output_redacts_api_key() {
        let config = AiSubtitleConfig {
            api_key: Some("secret-token".to_string()),
            ..AiSubtitleConfig::default()
        };

        let debug_output = format!("{config:?}");

        assert!(!debug_output.contains("secret-token"));
        assert!(debug_output.contains("[REDACTED]"));
    }

    #[test]
    fn preparing_state_uses_frontend_field_names() {
        let value = serde_json::to_value(AiSubtitleState::Preparing {
            seconds_left: 3,
            lead_seconds: 5,
            waiting_for_translation: false,
        })
        .unwrap();

        assert_eq!(value["status"], "preparing");
        assert_eq!(value["secondsLeft"], 3);
        assert_eq!(value["leadSeconds"], 5);
        assert_eq!(value["waitingForTranslation"], false);
    }
}
