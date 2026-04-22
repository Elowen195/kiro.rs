//! Anthropic API Handler 函数

use std::convert::Infallible;

use crate::kiro::model::events::Event;
use crate::kiro::model::events::{ContextUsageEvent, ToolUseEvent};
use crate::kiro::model::requests::kiro::KiroRequest;
use crate::kiro::parser::decoder::EventStreamDecoder;
use crate::token;
use anyhow::Error;
use axum::{
    Json as JsonExtractor,
    body::Body,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Json, Response},
};
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::interval;
use uuid::Uuid;

use super::converter::{ConversionError, convert_request};
use super::middleware::AppState;
use super::models::{fetch_upstream_models, to_anthropic_models};
use super::stream::{BufferedStreamContext, SseEvent, StreamContext};
use super::types::{
    CountTokensRequest, CountTokensResponse, ErrorResponse, MessagesRequest, OutputConfig,
    Thinking,
};
use super::websearch;

/// 将 KiroProvider 错误映射为 HTTP 响应
fn map_provider_error(err: Error) -> Response {
    let err_str = err.to_string();

    // 上下文窗口满了（对话历史累积超出模型上下文窗口限制）
    if err_str.contains("CONTENT_LENGTH_EXCEEDS_THRESHOLD") {
        tracing::warn!(error = %err, "上游拒绝请求：上下文窗口已满（不应重试）");
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "Context window is full. Reduce conversation history, system prompt, or tools.",
            )),
        )
            .into_response();
    }

    // 单次输入太长（请求体本身超出上游限制）
    if err_str.contains("Input is too long") {
        tracing::warn!(error = %err, "上游拒绝请求：输入过长（不应重试）");
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "Input is too long. Reduce the size of your messages.",
            )),
        )
            .into_response();
    }
    tracing::error!("Kiro API 调用失败: {}", err);
    (
        StatusCode::BAD_GATEWAY,
        Json(ErrorResponse::new(
            "api_error",
            format!("上游 API 调用失败: {}", err),
        )),
    )
        .into_response()
}

/// GET /cc/v1/models
pub async fn get_models(State(state): State<AppState>) -> Response {
    tracing::info!("Received GET /cc/v1/models request");

    let models = match fetch_upstream_models(&state).await {
        Ok(models) => models,
        Err(response) => return response,
    };

    Json(to_anthropic_models(&models)).into_response()
}

/// POST /v1/messages
///
/// 创建消息（对话）
pub async fn post_messages(
    State(state): State<AppState>,
    JsonExtractor(mut payload): JsonExtractor<MessagesRequest>,
) -> Response {
    tracing::info!(
        model = %payload.model,
        max_tokens = %payload.max_tokens,
        stream = %payload.stream,
        message_count = %payload.messages.len(),
        "Received POST /v1/messages request"
    );
    // 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
    override_thinking_from_model_name(&mut payload);

    // 检查 KiroProvider 是否可用
    let provider = match &state.kiro_provider {
        Some(p) => p.clone(),
        None => {
            tracing::error!("KiroProvider 未配置");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::new(
                    "service_unavailable",
                    "Kiro API provider not configured",
                )),
            )
                .into_response();
        }
    };

    // 检查是否为 WebSearch 请求
    if websearch::has_web_search_tool(&payload) {
        tracing::info!("检测到 WebSearch 工具，路由到 WebSearch 处理");

        // 估算输入 tokens
        let input_tokens = token::count_all_tokens(
            payload.model.clone(),
            payload.system.clone(),
            payload.messages.clone(),
            payload.tools.clone(),
        ) as i32;

        return websearch::handle_websearch_request(provider, &payload, input_tokens).await;
    }

    // 转换请求
    let conversion_result = match convert_request(&payload) {
        Ok(result) => result,
        Err(e) => {
            let (error_type, message) = match &e {
                ConversionError::UnsupportedModel(model) => {
                    ("invalid_request_error", format!("模型不支持: {}", model))
                }
                ConversionError::EmptyMessages => {
                    ("invalid_request_error", "消息列表为空".to_string())
                }
            };
            tracing::warn!("请求转换失败: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(error_type, message)),
            )
                .into_response();
        }
    };

    // 构建 Kiro 请求（profile_arn 由 provider 层根据实际凭据注入）
    let kiro_request = KiroRequest {
        conversation_state: conversion_result.conversation_state,
        profile_arn: None,
    };

    let request_body = match serde_json::to_string(&kiro_request) {
        Ok(body) => body,
        Err(e) => {
            tracing::error!("序列化请求失败: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(
                    "internal_error",
                    format!("序列化请求失败: {}", e),
                )),
            )
                .into_response();
        }
    };

    tracing::debug!("Kiro request body: {}", request_body);

    // 估算输入 tokens
    let input_tokens = token::count_all_tokens(
        payload.model.clone(),
        payload.system,
        payload.messages,
        payload.tools,
    ) as i32;

    // 检查是否启用了thinking
    let thinking_enabled = payload
        .thinking
        .as_ref()
        .map(|t| t.is_enabled())
        .unwrap_or(false);

    let tool_name_map = conversion_result.tool_name_map;

    if payload.stream {
        // 流式响应
        handle_stream_request(
            provider,
            &request_body,
            &payload.model,
            input_tokens,
            thinking_enabled,
            tool_name_map,
        )
        .await
    } else {
        // 非流式响应：仅在配置开启时提取 thinking 块
        let extract_thinking = state.extract_thinking && thinking_enabled;
        handle_non_stream_request(
            provider,
            &request_body,
            &payload.model,
            input_tokens,
            extract_thinking,
            tool_name_map,
        )
        .await
    }
}

/// 处理流式请求
async fn handle_stream_request(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &str,
    model: &str,
    input_tokens: i32,
    thinking_enabled: bool,
    tool_name_map: std::collections::HashMap<String, String>,
) -> Response {
    // 调用 Kiro API（支持多凭据故障转移）
    let response = match provider.call_api_stream(request_body).await {
        Ok(resp) => resp,
        Err(e) => return map_provider_error(e),
    };

    if response_content_type_is_json(&response) {
        tracing::info!("上游返回 JSON 响应，切换到 JSON 回退流式解析");
        return handle_stream_json_fallback_response(
            response,
            model,
            input_tokens,
            thinking_enabled,
            tool_name_map,
        )
        .await;
    }

    // 创建流处理上下文
    let mut ctx =
        StreamContext::new_with_thinking(model, input_tokens, thinking_enabled, tool_name_map);

    // 生成初始事件
    let initial_events = ctx.generate_initial_events();

    // 创建 SSE 流
    let stream = create_sse_stream(response, ctx, initial_events);

    // 返回 SSE 响应
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// Ping 事件间隔（25秒）
const PING_INTERVAL_SECS: u64 = 25;

/// 创建 ping 事件的 SSE 字符串
fn create_ping_sse() -> Bytes {
    Bytes::from("event: ping\ndata: {\"type\": \"ping\"}\n\n")
}

#[derive(Debug, Default, Clone)]
struct JsonBodyFallback {
    text_content: String,
    tool_uses: Vec<ToolUseEvent>,
    context_usage_percentage: Option<f64>,
    input_tokens: Option<i32>,
    output_tokens: Option<i32>,
}

impl JsonBodyFallback {
    fn is_meaningful(&self) -> bool {
        !self.text_content.is_empty()
            || !self.tool_uses.is_empty()
            || self.context_usage_percentage.is_some()
            || self.input_tokens.is_some()
            || self.output_tokens.is_some()
    }
}

fn response_content_type_is_json(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.to_ascii_lowercase().contains("json"))
        .unwrap_or(false)
}

fn value_as_i32(value: &Value) -> Option<i32> {
    if let Some(v) = value.as_i64() {
        return i32::try_from(v).ok();
    }
    value.as_u64().and_then(|v| i32::try_from(v).ok())
}

fn extract_i32_at_paths(root: &Value, paths: &[&str]) -> Option<i32> {
    paths
        .iter()
        .find_map(|path| root.pointer(path).and_then(value_as_i32))
}

fn extract_f64_at_paths(root: &Value, paths: &[&str]) -> Option<f64> {
    paths
        .iter()
        .find_map(|path| root.pointer(path).and_then(|v| v.as_f64()))
}

fn extract_text_from_content_value(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => {
            if s.is_empty() {
                None
            } else {
                Some(s.clone())
            }
        }
        Value::Array(arr) => {
            let mut parts = Vec::new();
            for item in arr {
                match item {
                    Value::String(s) => {
                        if !s.is_empty() {
                            parts.push(s.clone());
                        }
                    }
                    Value::Object(_) => {
                        let block_type = item.get("type").and_then(|v| v.as_str());
                        if block_type == Some("text") {
                            if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                                if !text.is_empty() {
                                    parts.push(text.to_string());
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            if parts.is_empty() {
                None
            } else {
                Some(parts.join(""))
            }
        }
        Value::Object(obj) => obj
            .get("text")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string()),
        _ => None,
    }
}

fn first_non_empty_text(root: &Value, paths: &[&str]) -> Option<String> {
    paths.iter().find_map(|path| {
        root.pointer(path)
            .and_then(extract_text_from_content_value)
            .filter(|s| !s.is_empty())
    })
}

fn build_tool_use_event(name: &str, tool_use_id: &str, input: Value, stop: bool) -> ToolUseEvent {
    let input_string = if let Some(raw) = input.as_str() {
        raw.to_string()
    } else {
        serde_json::to_string(&input).unwrap_or_else(|_| "{}".to_string())
    };
    ToolUseEvent {
        name: name.to_string(),
        tool_use_id: tool_use_id.to_string(),
        input: input_string,
        stop,
    }
}

fn append_tool_uses_from_content(content: &Value, out: &mut Vec<ToolUseEvent>) {
    let Some(arr) = content.as_array() else {
        return;
    };

    for item in arr {
        let Some(block_type) = item.get("type").and_then(|v| v.as_str()) else {
            continue;
        };
        if block_type != "tool_use" {
            continue;
        }

        let Some(name) = item.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(id) = item.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        let input = item.get("input").cloned().unwrap_or_else(|| json!({}));
        out.push(build_tool_use_event(name, id, input, true));
    }
}

fn append_tool_uses_from_openai_message(message: &Value, out: &mut Vec<ToolUseEvent>) {
    let Some(arr) = message.get("tool_calls").and_then(|v| v.as_array()) else {
        return;
    };

    for tool_call in arr {
        let Some(id) = tool_call.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(name) = tool_call
            .get("function")
            .and_then(|f| f.get("name"))
            .and_then(|v| v.as_str())
        else {
            continue;
        };
        let arguments = tool_call
            .get("function")
            .and_then(|f| f.get("arguments"))
            .cloned()
            .unwrap_or_else(|| Value::String("{}".to_string()));
        out.push(build_tool_use_event(name, id, arguments, true));
    }
}

fn parse_tool_use_event_object(value: &Value) -> Option<ToolUseEvent> {
    let name = value.get("name").and_then(|v| v.as_str())?;
    let tool_use_id = value
        .get("toolUseId")
        .or_else(|| value.get("tool_use_id"))
        .and_then(|v| v.as_str())?;
    let input = value.get("input").cloned().unwrap_or_else(|| json!({}));
    let stop = value.get("stop").and_then(|v| v.as_bool()).unwrap_or(true);
    Some(build_tool_use_event(name, tool_use_id, input, stop))
}

fn context_usage_to_input_tokens(model: &str, context_usage_percentage: f64) -> i32 {
    let window_size = get_context_window_size(model);
    (context_usage_percentage * (window_size as f64) / 100.0) as i32
}

fn parse_json_body_fallback(body_bytes: &[u8]) -> Option<JsonBodyFallback> {
    let root: Value = serde_json::from_slice(body_bytes).ok()?;
    let mut fallback = JsonBodyFallback::default();

    fallback.text_content = first_non_empty_text(
        &root,
        &[
            "/assistantResponseEvent/content",
            "/assistantResponseMessage/content",
            "/conversationState/currentMessage/assistantResponseMessage/content",
            "/choices/0/message/content",
            "/message/content",
            "/content",
        ],
    )
    .unwrap_or_default();

    append_tool_uses_from_content(&root["content"], &mut fallback.tool_uses);
    if let Some(message) = root.pointer("/choices/0/message") {
        append_tool_uses_from_openai_message(message, &mut fallback.tool_uses);
    }
    if let Some(tool_use_event) = root
        .get("toolUseEvent")
        .and_then(parse_tool_use_event_object)
    {
        fallback.tool_uses.push(tool_use_event);
    }

    fallback.context_usage_percentage = extract_f64_at_paths(
        &root,
        &[
            "/contextUsageEvent/contextUsagePercentage",
            "/contextUsagePercentage",
        ],
    );

    fallback.input_tokens = extract_i32_at_paths(
        &root,
        &[
            "/usage/input_tokens",
            "/usage/inputTokens",
            "/usage/prompt_tokens",
            "/usage/promptTokens",
        ],
    );
    fallback.output_tokens = extract_i32_at_paths(
        &root,
        &[
            "/usage/output_tokens",
            "/usage/outputTokens",
            "/usage/completion_tokens",
            "/usage/completionTokens",
        ],
    );

    // 兼容 events 数组包装格式
    if let Some(events) = root.get("events").and_then(|v| v.as_array()) {
        if fallback.text_content.is_empty() {
            for event in events {
                if let Some(text) = first_non_empty_text(
                    event,
                    &[
                        "/assistantResponseEvent/content",
                        "/assistantResponseMessage/content",
                    ],
                ) {
                    fallback.text_content.push_str(&text);
                }
            }
        }
        for event in events {
            if let Some(tool_use_event) = event
                .get("toolUseEvent")
                .and_then(parse_tool_use_event_object)
            {
                fallback.tool_uses.push(tool_use_event);
            }
            if fallback.context_usage_percentage.is_none() {
                fallback.context_usage_percentage = extract_f64_at_paths(
                    event,
                    &[
                        "/contextUsageEvent/contextUsagePercentage",
                        "/contextUsagePercentage",
                    ],
                );
            }
        }
    }

    if fallback.is_meaningful() {
        Some(fallback)
    } else {
        None
    }
}

fn apply_tool_use_event(
    tool_use: &ToolUseEvent,
    tool_json_buffers: &mut std::collections::HashMap<String, String>,
    has_tool_use: &mut bool,
    tool_uses: &mut Vec<serde_json::Value>,
    tool_name_map: &std::collections::HashMap<String, String>,
) {
    *has_tool_use = true;

    let buffer = tool_json_buffers
        .entry(tool_use.tool_use_id.clone())
        .or_insert_with(String::new);
    buffer.push_str(&tool_use.input);

    // 如果是完整的工具调用，添加到列表
    if tool_use.stop {
        let input: serde_json::Value = if buffer.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_str(buffer).unwrap_or_else(|e| {
                tracing::warn!(
                    "工具输入 JSON 解析失败: {}, tool_use_id: {}",
                    e,
                    tool_use.tool_use_id
                );
                serde_json::json!({})
            })
        };

        let original_name = tool_name_map
            .get(&tool_use.name)
            .cloned()
            .unwrap_or_else(|| tool_use.name.clone());

        tool_uses.push(json!({
            "type": "tool_use",
            "id": tool_use.tool_use_id,
            "name": original_name,
            "input": input
        }));
    }
}

/// 创建 SSE 事件流
fn create_sse_stream(
    response: reqwest::Response,
    ctx: StreamContext,
    initial_events: Vec<SseEvent>,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    // 先发送初始事件
    let initial_stream = stream::iter(
        initial_events
            .into_iter()
            .map(|e| Ok(Bytes::from(e.to_sse_string()))),
    );

    // 然后处理 Kiro 响应流，同时每25秒发送 ping 保活
    let body_stream = response.bytes_stream();

    let processing_stream = stream::unfold(
        (body_stream, ctx, EventStreamDecoder::new(), false, interval(Duration::from_secs(PING_INTERVAL_SECS))),
        |(mut body_stream, mut ctx, mut decoder, finished, mut ping_interval)| async move {
            if finished {
                return None;
            }

            // 使用 select! 同时等待数据和 ping 定时器
            tokio::select! {
                // 处理数据流
                chunk_result = body_stream.next() => {
                    match chunk_result {
                        Some(Ok(chunk)) => {
                            // 解码事件
                            if let Err(e) = decoder.feed(&chunk) {
                                tracing::warn!("缓冲区溢出: {}", e);
                            }

                            let mut events = Vec::new();
                            for result in decoder.decode_iter() {
                                match result {
                                    Ok(frame) => {
                                        if let Ok(event) = Event::from_frame(frame) {
                                            let sse_events = ctx.process_kiro_event(&event);
                                            events.extend(sse_events);
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("解码事件失败: {}", e);
                                    }
                                }
                            }

                            // 转换为 SSE 字节流
                            let bytes: Vec<Result<Bytes, Infallible>> = events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();

                            Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval)))
                        }
                        Some(Err(e)) => {
                            tracing::error!("读取响应流失败: {}", e);
                            // 发送最终事件并结束
                            let final_events = ctx.generate_final_events();
                            let bytes: Vec<Result<Bytes, Infallible>> = final_events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval)))
                        }
                        None => {
                            // 流结束，发送最终事件
                            let final_events = ctx.generate_final_events();
                            let bytes: Vec<Result<Bytes, Infallible>> = final_events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval)))
                        }
                    }
                }
                // 发送 ping 保活
                _ = ping_interval.tick() => {
                    tracing::trace!("发送 ping 保活事件");
                    let bytes: Vec<Result<Bytes, Infallible>> = vec![Ok(create_ping_sse())];
                    Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval)))
                }
            }
        },
    )
    .flatten();

    initial_stream.chain(processing_stream)
}

async fn handle_stream_json_fallback_response(
    response: reqwest::Response,
    model: &str,
    estimated_input_tokens: i32,
    thinking_enabled: bool,
    tool_name_map: std::collections::HashMap<String, String>,
) -> Response {
    let body_bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::error!("读取上游 JSON 响应失败: {}", e);
            return (
                StatusCode::BAD_GATEWAY,
                Json(ErrorResponse::new(
                    "api_error",
                    format!("读取响应失败: {}", e),
                )),
            )
                .into_response();
        }
    };

    let fallback = parse_json_body_fallback(&body_bytes);
    if fallback.is_none() {
        let preview = String::from_utf8_lossy(&body_bytes);
        tracing::warn!(
            "JSON 回退解析失败，body 预览: {}",
            preview.chars().take(200).collect::<String>()
        );
    }
    let fallback = fallback.unwrap_or_default();

    let final_input_tokens = fallback
        .input_tokens
        .or_else(|| {
            fallback
                .context_usage_percentage
                .map(|pct| context_usage_to_input_tokens(model, pct))
        })
        .unwrap_or(estimated_input_tokens);

    let mut ctx = StreamContext::new_with_thinking(
        model,
        final_input_tokens,
        thinking_enabled,
        tool_name_map,
    );
    let mut all_events = ctx.generate_initial_events();

    if let Some(context_usage_percentage) = fallback.context_usage_percentage {
        let context_event = ContextUsageEvent {
            context_usage_percentage,
        };
        all_events.extend(ctx.process_kiro_event(&Event::ContextUsage(context_event)));
    }

    if !fallback.text_content.is_empty() {
        all_events.extend(ctx.process_assistant_response(&fallback.text_content));
    }

    for tool_use in fallback.tool_uses {
        all_events.extend(ctx.process_kiro_event(&Event::ToolUse(tool_use)));
    }

    all_events.extend(ctx.generate_final_events());

    let bytes: Vec<Result<Bytes, Infallible>> = all_events
        .into_iter()
        .map(|e| Ok(Bytes::from(e.to_sse_string())))
        .collect();

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(stream::iter(bytes)))
        .unwrap()
}

use super::converter::get_context_window_size;

/// 处理非流式请求
async fn handle_non_stream_request(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &str,
    model: &str,
    input_tokens: i32,
    thinking_enabled: bool,
    tool_name_map: std::collections::HashMap<String, String>,
) -> Response {
    // 调用 Kiro API（支持多凭据故障转移）
    let response = match provider.call_api(request_body).await {
        Ok(resp) => resp,
        Err(e) => return map_provider_error(e),
    };
    let prefer_json_fallback = response_content_type_is_json(&response);

    // 读取响应体
    let body_bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::error!("读取响应体失败: {}", e);
            return (
                StatusCode::BAD_GATEWAY,
                Json(ErrorResponse::new(
                    "api_error",
                    format!("读取响应失败: {}", e),
                )),
            )
                .into_response();
        }
    };

    // 解析事件流（仅在 JSON 回退未命中时）
    let mut decoder = EventStreamDecoder::new();

    let mut text_content = String::new();
    let mut tool_uses: Vec<serde_json::Value> = Vec::new();
    let mut has_tool_use = false;
    let mut stop_reason = "end_turn".to_string();
    // 从 contextUsageEvent 计算的实际输入 tokens
    let mut context_input_tokens: Option<i32> = None;
    // 直接从上游 usage 获取的输出 tokens（若存在）
    let mut upstream_output_tokens: Option<i32> = None;

    // 收集工具调用的增量 JSON
    let mut tool_json_buffers: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    let mut parsed_from_json = false;

    // glm-5 等模型在 CLI 端点可能直接返回 JSON（非 event-stream）
    if prefer_json_fallback {
        if let Some(fallback) = parse_json_body_fallback(&body_bytes) {
            parsed_from_json = true;
            text_content.push_str(&fallback.text_content);
            for tool_use in &fallback.tool_uses {
                apply_tool_use_event(
                    tool_use,
                    &mut tool_json_buffers,
                    &mut has_tool_use,
                    &mut tool_uses,
                    &tool_name_map,
                );
            }
            if let Some(pct) = fallback.context_usage_percentage {
                let actual_input_tokens = context_usage_to_input_tokens(model, pct);
                context_input_tokens = Some(actual_input_tokens);
                if pct >= 100.0 {
                    stop_reason = "model_context_window_exceeded".to_string();
                }
            }
            if context_input_tokens.is_none() {
                context_input_tokens = fallback.input_tokens;
            }
            upstream_output_tokens = fallback.output_tokens;
        }
    }

    if !parsed_from_json {
        if let Err(e) = decoder.feed(&body_bytes) {
            tracing::warn!("缓冲区溢出: {}", e);
        }

        for result in decoder.decode_iter() {
            match result {
                Ok(frame) => {
                    if let Ok(event) = Event::from_frame(frame) {
                        match event {
                            Event::AssistantResponse(resp) => {
                                text_content.push_str(&resp.content);
                            }
                            Event::ToolUse(tool_use) => {
                                apply_tool_use_event(
                                    &tool_use,
                                    &mut tool_json_buffers,
                                    &mut has_tool_use,
                                    &mut tool_uses,
                                    &tool_name_map,
                                );
                            }
                            Event::ContextUsage(context_usage) => {
                                let actual_input_tokens = context_usage_to_input_tokens(
                                    model,
                                    context_usage.context_usage_percentage,
                                );
                                context_input_tokens = Some(actual_input_tokens);
                                if context_usage.context_usage_percentage >= 100.0 {
                                    stop_reason = "model_context_window_exceeded".to_string();
                                }
                                tracing::debug!(
                                    "收到 contextUsageEvent: {}%, 计算 input_tokens: {}",
                                    context_usage.context_usage_percentage,
                                    actual_input_tokens
                                );
                            }
                            Event::Exception { exception_type, .. } => {
                                if exception_type == "ContentLengthExceededException" {
                                    stop_reason = "max_tokens".to_string();
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("解码事件失败: {}", e);
                }
            }
        }

        // content-type 不可靠时，按内容再次尝试 JSON 回退
        if text_content.is_empty() && tool_uses.is_empty() {
            if let Some(fallback) = parse_json_body_fallback(&body_bytes) {
                text_content.push_str(&fallback.text_content);
                for tool_use in &fallback.tool_uses {
                    apply_tool_use_event(
                        tool_use,
                        &mut tool_json_buffers,
                        &mut has_tool_use,
                        &mut tool_uses,
                        &tool_name_map,
                    );
                }
                if let Some(pct) = fallback.context_usage_percentage {
                    let actual_input_tokens = context_usage_to_input_tokens(model, pct);
                    context_input_tokens = Some(actual_input_tokens);
                    if pct >= 100.0 {
                        stop_reason = "model_context_window_exceeded".to_string();
                    }
                }
                if context_input_tokens.is_none() {
                    context_input_tokens = fallback.input_tokens;
                }
                upstream_output_tokens = fallback.output_tokens;
            } else if body_bytes.starts_with(b"{") {
                tracing::warn!(
                    "非流式 JSON 回退未命中，body 预览: {}",
                    String::from_utf8_lossy(&body_bytes)
                        .chars()
                        .take(2000)
                        .collect::<String>()
                );
            }
        }
    }

    // 确定 stop_reason
    if has_tool_use && stop_reason == "end_turn" {
        stop_reason = "tool_use".to_string();
    }

    // 构建响应内容
    let mut content: Vec<serde_json::Value> = Vec::new();

    if thinking_enabled {
        // 从完整文本中提取 thinking 块
        let (thinking, remaining_text) =
            super::stream::extract_thinking_from_complete_text(&text_content);

        if let Some(thinking_text) = thinking {
            content.push(json!({
                "type": "thinking",
                "thinking": thinking_text
            }));
        }

        if !remaining_text.is_empty() {
            content.push(json!({
                "type": "text",
                "text": remaining_text
            }));
        }
    } else if !text_content.is_empty() {
        content.push(json!({
            "type": "text",
            "text": text_content
        }));
    }

    content.extend(tool_uses);

    // 优先使用上游 usage 的 output_tokens，其次本地估算
    let output_tokens =
        upstream_output_tokens.unwrap_or_else(|| token::estimate_output_tokens(&content));

    // 使用从 contextUsageEvent 计算的 input_tokens，如果没有则使用估算值
    let final_input_tokens = context_input_tokens.unwrap_or(input_tokens);

    // 构建 Anthropic 响应
    let response_body = json!({
        "id": format!("msg_{}", Uuid::new_v4().to_string().replace('-', "")),
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": model,
        "stop_reason": stop_reason,
        "stop_sequence": null,
        "usage": {
            "input_tokens": final_input_tokens,
            "output_tokens": output_tokens
        }
    });

    (StatusCode::OK, Json(response_body)).into_response()
}

/// 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
///
/// - Opus 4.6：覆写为 adaptive 类型
/// - 其他模型：覆写为 enabled 类型
/// - budget_tokens 固定为 20000
pub(crate) fn override_thinking_from_model_name(payload: &mut MessagesRequest) {
    let model_lower = payload.model.to_lowercase();
    if !model_lower.contains("thinking") {
        return;
    }

    let is_opus_4_6 = model_lower.contains("opus")
        && (model_lower.contains("4-6") || model_lower.contains("4.6"));

    let thinking_type = if is_opus_4_6 { "adaptive" } else { "enabled" };

    tracing::info!(
        model = %payload.model,
        thinking_type = thinking_type,
        "模型名包含 thinking 后缀，覆写 thinking 配置"
    );

    payload.thinking = Some(Thinking {
        thinking_type: thinking_type.to_string(),
        budget_tokens: 20000,
    });

    if is_opus_4_6 {
        payload.output_config = Some(OutputConfig {
            effort: "high".to_string(),
        });
    }
}

/// POST /v1/messages/count_tokens
///
/// 计算消息的 token 数量
pub async fn count_tokens(
    JsonExtractor(payload): JsonExtractor<CountTokensRequest>,
) -> impl IntoResponse {
    tracing::info!(
        model = %payload.model,
        message_count = %payload.messages.len(),
        "Received POST /v1/messages/count_tokens request"
    );

    let total_tokens = token::count_all_tokens(
        payload.model,
        payload.system,
        payload.messages,
        payload.tools,
    ) as i32;

    Json(CountTokensResponse {
        input_tokens: total_tokens.max(1) as i32,
    })
}

/// POST /cc/v1/messages
///
/// Claude Code 兼容端点，与 /v1/messages 的区别在于：
/// - 流式响应会等待 kiro 端返回 contextUsageEvent 后再发送 message_start
/// - message_start 中的 input_tokens 是从 contextUsageEvent 计算的准确值
pub async fn post_messages_cc(
    State(state): State<AppState>,
    JsonExtractor(mut payload): JsonExtractor<MessagesRequest>,
) -> Response {
    tracing::info!(
        model = %payload.model,
        max_tokens = %payload.max_tokens,
        stream = %payload.stream,
        message_count = %payload.messages.len(),
        "Received POST /cc/v1/messages request"
    );

    // 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
    override_thinking_from_model_name(&mut payload);

    // 检查 KiroProvider 是否可用
    let provider = match &state.kiro_provider {
        Some(p) => p.clone(),
        None => {
            tracing::error!("KiroProvider 未配置");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::new(
                    "service_unavailable",
                    "Kiro API provider not configured",
                )),
            )
                .into_response();
        }
    };

    // 检查是否为 WebSearch 请求
    if websearch::has_web_search_tool(&payload) {
        tracing::info!("检测到 WebSearch 工具，路由到 WebSearch 处理");

        // 估算输入 tokens
        let input_tokens = token::count_all_tokens(
            payload.model.clone(),
            payload.system.clone(),
            payload.messages.clone(),
            payload.tools.clone(),
        ) as i32;

        return websearch::handle_websearch_request(provider, &payload, input_tokens).await;
    }

    // 转换请求
    let conversion_result = match convert_request(&payload) {
        Ok(result) => result,
        Err(e) => {
            let (error_type, message) = match &e {
                ConversionError::UnsupportedModel(model) => {
                    ("invalid_request_error", format!("模型不支持: {}", model))
                }
                ConversionError::EmptyMessages => {
                    ("invalid_request_error", "消息列表为空".to_string())
                }
            };
            tracing::warn!("请求转换失败: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(error_type, message)),
            )
                .into_response();
        }
    };

    // 构建 Kiro 请求（profile_arn 由 provider 层根据实际凭据注入）
    let kiro_request = KiroRequest {
        conversation_state: conversion_result.conversation_state,
        profile_arn: None,
    };

    let request_body = match serde_json::to_string(&kiro_request) {
        Ok(body) => body,
        Err(e) => {
            tracing::error!("序列化请求失败: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(
                    "internal_error",
                    format!("序列化请求失败: {}", e),
                )),
            )
                .into_response();
        }
    };

    tracing::debug!("Kiro request body: {}", request_body);

    // 估算输入 tokens
    let input_tokens = token::count_all_tokens(
        payload.model.clone(),
        payload.system,
        payload.messages,
        payload.tools,
    ) as i32;

    // 检查是否启用了thinking
    let thinking_enabled = payload
        .thinking
        .as_ref()
        .map(|t| t.is_enabled())
        .unwrap_or(false);

    let tool_name_map = conversion_result.tool_name_map;

    if payload.stream {
        // 流式响应（缓冲模式）
        handle_stream_request_buffered(
            provider,
            &request_body,
            &payload.model,
            input_tokens,
            thinking_enabled,
            tool_name_map,
        )
        .await
    } else {
        // 非流式响应：仅在配置开启时提取 thinking 块
        let extract_thinking = state.extract_thinking && thinking_enabled;
        handle_non_stream_request(
            provider,
            &request_body,
            &payload.model,
            input_tokens,
            extract_thinking,
            tool_name_map,
        )
        .await
    }
}

/// 处理流式请求（缓冲版本）
///
/// 与 `handle_stream_request` 不同，此函数会缓冲所有事件直到流结束，
/// 然后用从 contextUsageEvent 计算的正确 input_tokens 生成 message_start 事件。
async fn handle_stream_request_buffered(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &str,
    model: &str,
    estimated_input_tokens: i32,
    thinking_enabled: bool,
    tool_name_map: std::collections::HashMap<String, String>,
) -> Response {
    // 调用 Kiro API（支持多凭据故障转移）
    let response = match provider.call_api_stream(request_body).await {
        Ok(resp) => resp,
        Err(e) => return map_provider_error(e),
    };

    if response_content_type_is_json(&response) {
        tracing::info!("缓冲流模式下上游返回 JSON，切换到 JSON 回退流式解析");
        return handle_stream_json_fallback_response(
            response,
            model,
            estimated_input_tokens,
            thinking_enabled,
            tool_name_map,
        )
        .await;
    }

    // 创建缓冲流处理上下文
    let ctx = BufferedStreamContext::new(
        model,
        estimated_input_tokens,
        thinking_enabled,
        tool_name_map,
    );

    // 创建缓冲 SSE 流
    let stream = create_buffered_sse_stream(response, ctx);

    // 返回 SSE 响应
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// 创建缓冲 SSE 事件流
///
/// 工作流程：
/// 1. 等待上游流完成，期间只发送 ping 保活信号
/// 2. 使用 StreamContext 的事件处理逻辑处理所有 Kiro 事件，结果缓存
/// 3. 流结束后，用正确的 input_tokens 更正 message_start 事件
/// 4. 一次性发送所有事件
fn create_buffered_sse_stream(
    response: reqwest::Response,
    ctx: BufferedStreamContext,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    let body_stream = response.bytes_stream();

    stream::unfold(
        (
            body_stream,
            ctx,
            EventStreamDecoder::new(),
            false,
            interval(Duration::from_secs(PING_INTERVAL_SECS)),
        ),
        |(mut body_stream, mut ctx, mut decoder, finished, mut ping_interval)| async move {
            if finished {
                return None;
            }

            loop {
                tokio::select! {
                    // 使用 biased 模式，优先检查 ping 定时器
                    // 避免在上游 chunk 密集时 ping 被"饿死"
                    biased;

                    // 优先检查 ping 保活（等待期间唯一发送的数据）
                    _ = ping_interval.tick() => {
                        tracing::trace!("发送 ping 保活事件（缓冲模式）");
                        let bytes: Vec<Result<Bytes, Infallible>> = vec![Ok(create_ping_sse())];
                        return Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval)));
                    }

                    // 然后处理数据流
                    chunk_result = body_stream.next() => {
                        match chunk_result {
                            Some(Ok(chunk)) => {
                                // 解码事件
                                if let Err(e) = decoder.feed(&chunk) {
                                    tracing::warn!("缓冲区溢出: {}", e);
                                }

                                for result in decoder.decode_iter() {
                                    match result {
                                        Ok(frame) => {
                                            if let Ok(event) = Event::from_frame(frame) {
                                                // 缓冲事件（复用 StreamContext 的处理逻辑）
                                                ctx.process_and_buffer(&event);
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!("解码事件失败: {}", e);
                                        }
                                    }
                                }
                                // 继续读取下一个 chunk，不发送任何数据
                            }
                            Some(Err(e)) => {
                                tracing::error!("读取响应流失败: {}", e);
                                // 发生错误，完成处理并返回所有事件
                                let all_events = ctx.finish_and_get_all_events();
                                let bytes: Vec<Result<Bytes, Infallible>> = all_events
                                    .into_iter()
                                    .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                    .collect();
                                return Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval)));
                            }
                            None => {
                                // 流结束，完成处理并返回所有事件（已更正 input_tokens）
                                let all_events = ctx.finish_and_get_all_events();
                                let bytes: Vec<Result<Bytes, Infallible>> = all_events
                                    .into_iter()
                                    .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                    .collect();
                                return Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval)));
                            }
                        }
                    }
                }
            }
        },
    )
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json_body_fallback_assistant_response_event() {
        let body = br#"{
            "assistantResponseEvent": {"content": "hello from glm"},
            "usage": {"input_tokens": 12, "output_tokens": 4}
        }"#;

        let parsed = parse_json_body_fallback(body).expect("should parse");
        assert_eq!(parsed.text_content, "hello from glm");
        assert_eq!(parsed.input_tokens, Some(12));
        assert_eq!(parsed.output_tokens, Some(4));
        assert!(parsed.tool_uses.is_empty());
    }

    #[test]
    fn test_parse_json_body_fallback_conversation_state() {
        let body = br#"{
            "conversationState": {
                "currentMessage": {
                    "assistantResponseMessage": {
                        "content": "conversation payload text"
                    }
                }
            }
        }"#;

        let parsed = parse_json_body_fallback(body).expect("should parse");
        assert_eq!(parsed.text_content, "conversation payload text");
    }

    #[test]
    fn test_parse_json_body_fallback_content_blocks_and_tool_use() {
        let body = br#"{
            "content": [
                {"type": "text", "text": "answer"},
                {"type": "tool_use", "id": "toolu_123", "name": "web_search", "input": {"q": "kiro"}}
            ],
            "usage": {"prompt_tokens": 7, "completion_tokens": 3}
        }"#;

        let parsed = parse_json_body_fallback(body).expect("should parse");
        assert_eq!(parsed.text_content, "answer");
        assert_eq!(parsed.input_tokens, Some(7));
        assert_eq!(parsed.output_tokens, Some(3));
        assert_eq!(parsed.tool_uses.len(), 1);
        assert_eq!(parsed.tool_uses[0].name, "web_search");
        assert_eq!(parsed.tool_uses[0].tool_use_id, "toolu_123");
        assert_eq!(parsed.tool_uses[0].input, r#"{"q":"kiro"}"#);
        assert!(parsed.tool_uses[0].stop);
    }
}
