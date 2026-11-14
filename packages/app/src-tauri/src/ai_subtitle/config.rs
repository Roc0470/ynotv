use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tauri::{AppHandle, Manager};

use super::models::AiSubtitleConfig;

const CONFIG_FILE_NAME: &str = "ynotv_ai_subtitle.json";
const MIN_SYNC_OFFSET_MS: i32 = -15_000;
const MAX_SYNC_OFFSET_MS: i32 = 15_000;

static CONFIG_LOCK: Mutex<()> = Mutex::new(());

fn config_path(app: &AppHandle) -> Result<PathBuf, String> {
    app.path()
        .app_config_dir()
        .map(|dir| dir.join(CONFIG_FILE_NAME))
        .map_err(|error| format!("无法获取应用配置目录：{error}"))
}

fn read_config(path: &Path) -> Result<AiSubtitleConfig, String> {
    match fs::read_to_string(path) {
        Ok(contents) => serde_json::from_str(&contents)
            .map(normalize_config)
            .map_err(|error| format!("AI 字幕配置文件格式无效：{error}")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(AiSubtitleConfig::default())
        }
        Err(error) => Err(format!("读取 AI 字幕配置失败：{error}")),
    }
}

fn write_config(path: &Path, config: &AiSubtitleConfig) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "AI 字幕配置路径无效".to_string())?;
    fs::create_dir_all(parent).map_err(|error| format!("创建配置目录失败：{error}"))?;

    let contents = serde_json::to_string_pretty(config)
        .map_err(|error| format!("序列化 AI 字幕配置失败：{error}"))?;
    fs::write(path, contents).map_err(|error| format!("保存 AI 字幕配置失败：{error}"))
}

fn normalize_config(mut config: AiSubtitleConfig) -> AiSubtitleConfig {
    config.sync_offset_ms = config
        .sync_offset_ms
        .clamp(MIN_SYNC_OFFSET_MS, MAX_SYNC_OFFSET_MS);
    config.lead_seconds = config.lead_seconds.clamp(2, 20);
    config
}

fn change_sync_offset(config: &mut AiSubtitleConfig, delta_ms: i32) {
    config.sync_offset_ms = config
        .sync_offset_ms
        .saturating_add(delta_ms)
        .clamp(MIN_SYNC_OFFSET_MS, MAX_SYNC_OFFSET_MS);
}

fn clear_sync_offset(config: &mut AiSubtitleConfig) {
    config.sync_offset_ms = 0;
}

pub fn get_current_config(app: &AppHandle) -> Result<AiSubtitleConfig, String> {
    let _guard = CONFIG_LOCK
        .lock()
        .map_err(|error| format!("AI 字幕配置锁不可用：{error}"))?;
    read_config(&config_path(app)?)
}

pub fn update_config(
    app: &AppHandle,
    config: AiSubtitleConfig,
) -> Result<AiSubtitleConfig, String> {
    let _guard = CONFIG_LOCK
        .lock()
        .map_err(|error| format!("AI 字幕配置锁不可用：{error}"))?;
    let path = config_path(app)?;
    let config = normalize_config(config);
    write_config(&path, &config)?;
    Ok(config)
}

pub fn adjust_sync_offset(app: &AppHandle, delta_ms: i32) -> Result<AiSubtitleConfig, String> {
    let _guard = CONFIG_LOCK
        .lock()
        .map_err(|error| format!("AI 字幕配置锁不可用：{error}"))?;
    let path = config_path(app)?;
    let mut config = read_config(&path)?;
    change_sync_offset(&mut config, delta_ms);
    write_config(&path, &config)?;
    Ok(config)
}

pub fn set_sync_offset(app: &AppHandle, sync_offset_ms: i32) -> Result<AiSubtitleConfig, String> {
    let _guard = CONFIG_LOCK
        .lock()
        .map_err(|error| format!("AI 字幕配置锁不可用：{error}"))?;
    let path = config_path(app)?;
    let mut config = read_config(&path)?;
    config.sync_offset_ms = sync_offset_ms;
    let config = normalize_config(config);
    write_config(&path, &config)?;
    Ok(config)
}

pub fn reset_sync_offset(app: &AppHandle) -> Result<AiSubtitleConfig, String> {
    let _guard = CONFIG_LOCK
        .lock()
        .map_err(|error| format!("AI 字幕配置锁不可用：{error}"))?;
    let path = config_path(app)?;
    let mut config = read_config(&path)?;
    clear_sync_offset(&mut config);
    write_config(&path, &config)?;
    Ok(config)
}

#[tauri::command]
pub fn ai_subtitle_get_config(app: AppHandle) -> Result<AiSubtitleConfig, String> {
    get_current_config(&app)
}

#[tauri::command]
pub fn ai_subtitle_save_config(
    app: AppHandle,
    config: AiSubtitleConfig,
) -> Result<AiSubtitleConfig, String> {
    update_config(&app, config)
}

#[cfg(test)]
mod tests {
    use super::{
        change_sync_offset, clear_sync_offset, normalize_config, read_config, write_config,
    };
    use crate::ai_subtitle::models::AiSubtitleConfig;
    use tempfile::tempdir;

    #[test]
    fn missing_config_file_uses_defaults_and_saved_config_round_trips() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("ynotv_ai_subtitle.json");

        assert_eq!(
            read_config(&path).unwrap().model,
            "qwen3.8-livetranslate-flash-realtime"
        );

        let config = AiSubtitleConfig {
            sync_offset_ms: 750,
            ..AiSubtitleConfig::default()
        };
        write_config(&path, &config).unwrap();

        assert_eq!(read_config(&path).unwrap().sync_offset_ms, 750);
    }

    #[test]
    fn config_normalization_keeps_documented_ranges() {
        let config = normalize_config(AiSubtitleConfig {
            lead_seconds: 30,
            sync_offset_ms: 20_000,
            ..AiSubtitleConfig::default()
        });

        assert_eq!(config.lead_seconds, 20);
        assert_eq!(config.sync_offset_ms, 15_000);
    }

    #[test]
    fn sync_offset_adjustment_saturates_to_supported_range() {
        let mut config = AiSubtitleConfig {
            sync_offset_ms: 14_900,
            ..AiSubtitleConfig::default()
        };
        change_sync_offset(&mut config, 500);
        assert_eq!(config.sync_offset_ms, 15_000);
        change_sync_offset(&mut config, i32::MIN);
        assert_eq!(config.sync_offset_ms, -15_000);
    }

    #[test]
    fn reset_clears_sync_offset() {
        let mut config = AiSubtitleConfig {
            sync_offset_ms: 2_500,
            ..AiSubtitleConfig::default()
        };
        clear_sync_offset(&mut config);
        assert_eq!(config.sync_offset_ms, 0);
    }
}
