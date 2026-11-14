use std::time::{Duration, Instant};

use reqwest::{Client, StatusCode};
use serde::Serialize;
use serde_json::json;
use tauri::command;
use tokio_tungstenite::connect_async;

use super::engine::{
    build_realtime_request, describe_realtime_connect_error, DEFAULT_BAILIAN_REALTIME_MODEL,
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AiConnectionTestResult {
    pub success: bool,
    pub latency_ms: Option<u64>,
    pub message: String,
}

pub async fn test_ai_connection(
    base_url: String,
    api_key: String,
    model: Option<String>,
) -> Result<AiConnectionTestResult, String> {
    let api_key = api_key.trim();
    if api_key.is_empty() {
        return Err("请先填写 API Key".to_string());
    }
    let realtime = uses_realtime_websocket(&base_url, model.as_deref().unwrap_or_default());
    let model = model
        .as_deref()
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .unwrap_or(if realtime {
            DEFAULT_BAILIAN_REALTIME_MODEL
        } else {
            "qwen-turbo"
        });
    if realtime {
        return test_realtime_connection(&base_url, api_key, model).await;
    }

    let endpoint = chat_completions_endpoint(&base_url)?;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|error| format!("创建连通性测试客户端失败：{error}"))?;

    let started_at = Instant::now();
    let response = client
        .post(endpoint)
        .bearer_auth(api_key)
        .json(&json!({
            "model": model,
            "messages": [{ "role": "user", "content": "ping" }],
            "max_tokens": 1
        }))
        .send()
        .await
        .map_err(|error| format!("连接 AI 服务失败：{error}"))?;
    let latency_ms = started_at.elapsed().as_millis() as u64;
    let status = response.status();

    if status.is_success() {
        return Ok(AiConnectionTestResult {
            success: true,
            latency_ms: Some(latency_ms),
            message: format!("连接成功，响应耗时 {latency_ms} ms"),
        });
    }

    let error_text = response.text().await.unwrap_or_default();
    let message = describe_http_error(status, &error_text);
    Ok(AiConnectionTestResult {
        success: false,
        latency_ms: Some(latency_ms),
        message,
    })
}

async fn test_realtime_connection(
    base_url: &str,
    api_key: &str,
    model: &str,
) -> Result<AiConnectionTestResult, String> {
    let request = build_realtime_request(Some(base_url), model, api_key)?;
    let started_at = Instant::now();
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(15), connect_async(request))
        .await
        .map_err(|_| "百炼 Realtime WebSocket 握手超时（15 秒）".to_string())?
        .map_err(|error| describe_realtime_connect_error(&error))?;
    let latency_ms = started_at.elapsed().as_millis() as u64;
    let _ = socket.close(None).await;

    Ok(AiConnectionTestResult {
        success: true,
        latency_ms: Some(latency_ms),
        message: format!("Realtime WebSocket 握手成功，耗时 {latency_ms} ms"),
    })
}

#[command]
pub async fn ai_subtitle_test_connection(
    base_url: String,
    api_key: String,
    model: Option<String>,
) -> Result<AiConnectionTestResult, String> {
    test_ai_connection(base_url, api_key, model).await
}

fn uses_realtime_websocket(base_url: &str, model: &str) -> bool {
    base_url.to_ascii_lowercase().contains("realtime")
        || model.to_ascii_lowercase().contains("realtime")
}

fn chat_completions_endpoint(base_url: &str) -> Result<String, String> {
    let base_url = base_url.trim().trim_end_matches('/');
    if base_url.is_empty() {
        return Err("请先填写 AI 服务 Base URL".to_string());
    }

    if base_url.ends_with("/chat/completions") {
        return Ok(base_url.to_string());
    }
    if base_url.ends_with("/v1") {
        return Ok(format!("{base_url}/chat/completions"));
    }
    Ok(format!("{base_url}/v1/chat/completions"))
}

fn describe_http_error(status: StatusCode, error_body: &str) -> String {
    let detail = if !error_body.trim().is_empty() {
        format!("：{error_body}")
    } else {
        String::new()
    };
    match status {
        StatusCode::UNAUTHORIZED => format!("API Key 错误或已失效（HTTP 401）{detail}"),
        StatusCode::FORBIDDEN => format!("API Key 无权访问该模型（HTTP 403）{detail}"),
        StatusCode::NOT_FOUND => format!("Base URL 或模型地址不存在（HTTP 404）{detail}"),
        StatusCode::TOO_MANY_REQUESTS => {
            format!("请求频率超限或额度不足（HTTP 429）{detail}")
        }
        _ => format!("AI 服务返回 HTTP {}{detail}", status.as_u16()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        chat_completions_endpoint, describe_http_error, test_ai_connection, uses_realtime_websocket,
    };
    use futures_util::StreamExt;
    use reqwest::StatusCode;
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message;

    #[test]
    fn chat_endpoint_accepts_common_base_url_forms() {
        assert_eq!(
            chat_completions_endpoint("https://example.com/compatible-mode/v1/").unwrap(),
            "https://example.com/compatible-mode/v1/chat/completions"
        );
        assert_eq!(
            chat_completions_endpoint("https://example.com").unwrap(),
            "https://example.com/v1/chat/completions"
        );
        assert_eq!(
            chat_completions_endpoint("https://example.com/v1/chat/completions").unwrap(),
            "https://example.com/v1/chat/completions"
        );
    }

    #[test]
    fn common_auth_and_address_errors_are_actionable() {
        assert!(describe_http_error(StatusCode::UNAUTHORIZED, "").contains("API Key"));
        assert!(describe_http_error(StatusCode::NOT_FOUND, "").contains("Base URL"));
    }

    #[test]
    fn empty_base_url_is_rejected() {
        assert!(chat_completions_endpoint("  ").is_err());
    }

    #[test]
    fn realtime_model_or_endpoint_selects_websocket_test() {
        assert!(uses_realtime_websocket(
            "https://dashscope.aliyuncs.com/compatible-mode/v1",
            "qwen3.8-livetranslate-flash-realtime"
        ));
        assert!(uses_realtime_websocket(
            "wss://dashscope.aliyuncs.com/compatible-mode/v1/realtime",
            "qwen-plus"
        ));
        assert!(!uses_realtime_websocket(
            "https://dashscope.aliyuncs.com/compatible-mode/v1",
            "qwen-plus"
        ));
    }

    #[tokio::test]
    async fn realtime_connection_test_returns_after_a_successful_websocket_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(message) = socket.next().await {
                if matches!(message.unwrap(), Message::Close(_)) {
                    break;
                }
            }
        });

        let result = test_ai_connection(
            format!("http://{address}/compatible-mode/v1"),
            "test-token".to_string(),
            Some("qwen3.8-livetranslate-flash-realtime".to_string()),
        )
        .await
        .unwrap();

        assert!(result.success);
        assert!(result.latency_ms.is_some());
        assert!(result.message.contains("WebSocket 握手成功"));
        server.await.unwrap();
    }
}
