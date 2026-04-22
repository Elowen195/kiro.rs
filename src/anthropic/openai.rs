//! OpenAI Chat Completions 兼容端点
//!
//! 提供与 OpenAI `/v1/chat/completions` API 兼容的 HTTP 接口，内部将请求
//! 转换为 Anthropic `MessagesRequest` 走现有转换链路，再把响应/事件流
//! 转换回 OpenAI 格式。
//!
//! # 支持
//! - 非流式响应
//! - 流式响应（SSE, `data: {...}\n\n` + `data: [DONE]`）
//! - Function calling（`tools` / `tool_choice`）
//! - `system` 角色自动搬运到 Anthropic `system` 字段
//! - `tool` 角色自动转成 Anthropic `tool_result` 块

use std::convert::Infallible;

use axum::{
    Json as JsonExtractor,
    body::Body,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Json, Response},
};
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::interval;
use uuid::Uuid;

use crate::kiro::model::events::Event;
use crate::kiro::model::requests::kiro::KiroRequest;
use crate::kiro::parser::decoder::EventStreamDecoder;
use crate::token;

use super::converter::{ConversionError, convert_request, get_context_window_size};
use super::handlers::override_thinking_from_model_name;
use super::middleware::AppState;
use super::models::fetch_upstream_models;
use super::types::{
    ErrorResponse, Message as AnthropicMessage, MessagesRequest, SystemMessage, Thinking,
    Tool as AnthropicTool,
};

// === OpenAI 请求类型 ===

#[derive(Debug, Deserialize)]
pub struct ChatCompletionsRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: bool,
    pub max_tokens: Option<i32>,
    pub max_completion_tokens: Option<i32>,
    pub tools: Option<Vec<ChatTool>>,
    pub tool_choice: Option<Value>,
    /// 保留字段（OpenAI 兼容），本服务忽略
    #[allow(dead_code)]
    pub temperature: Option<f64>,
    #[allow(dead_code)]
    pub top_p: Option<f64>,
    pub reasoning_effort: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChatMessage {
    pub role: String,
    /// string 或 content parts 数组
    #[serde(default)]
    pub content: Value,
    /// assistant 消息的 tool_calls
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ChatToolCall>>,
    /// tool 角色的 tool_call_id
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    /// OpenAI 兼容的 name 字段（如 system/user 的 name）
    #[allow(dead_code)]
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChatToolCall {
    pub id: String,
    #[serde(default = "default_tool_call_type")]
    #[allow(dead_code)]
    pub r#type: String,
    pub function: ChatFunctionCall,
}

fn default_tool_call_type() -> String {
    "function".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChatFunctionCall {
    pub name: String,
    /// JSON 字符串形式的参数
    #[serde(default)]
    pub arguments: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChatTool {
    #[serde(default = "default_tool_type")]
    #[allow(dead_code)]
    pub r#type: String,
    pub function: ChatFunctionSpec,
}

fn default_tool_type() -> String {
    "function".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChatFunctionSpec {
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// JSON Schema
    #[serde(default)]
    pub parameters: Value,
}

// === OpenAI 响应类型 ===

#[derive(Debug, Serialize)]
pub struct ChatCompletion {
    pub id: String,
    pub object: &'static str,
    pub created: i64,
    pub model: String,
    pub choices: Vec<ChatChoice>,
    pub usage: ChatUsage,
}

#[derive(Debug, Serialize)]
pub struct ChatChoice {
    pub index: i32,
    pub message: ChatResponseMessage,
    pub finish_reason: String,
}

#[derive(Debug, Serialize)]
pub struct ChatResponseMessage {
    pub role: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ChatResponseToolCall>>,
}

#[derive(Debug, Serialize)]
pub struct ChatResponseToolCall {
    pub id: String,
    pub r#type: &'static str,
    pub function: ChatResponseFunction,
    pub index: i32,
}

#[derive(Debug, Serialize)]
pub struct ChatResponseFunction {
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Serialize)]
pub struct ChatUsage {
    pub prompt_tokens: i32,
    pub completion_tokens: i32,
    pub total_tokens: i32,
}

#[derive(Debug, Serialize)]
pub struct OpenAIModelsResponse {
    pub object: String,
    pub data: Vec<OpenAIModel>,
}

#[derive(Debug, Serialize)]
pub struct OpenAIModel {
    pub id: String,
    pub object: &'static str,
    pub created: i64,
    pub owned_by: String,
    pub display_name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_input_types: Vec<String>,
    pub max_input_tokens: i32,
    pub max_output_tokens: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_multiplier: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_unit: Option<String>,
}

// === Handler ===

/// GET /v1/models
pub async fn get_models(State(state): State<AppState>) -> Response {
    tracing::info!("Received GET /v1/models request");

    let models = match fetch_upstream_models(&state).await {
        Ok(models) => models,
        Err(response) => return response,
    };

    let data = models
        .into_iter()
        .map(|model| OpenAIModel {
            id: model.id,
            object: "model",
            created: 0,
            owned_by: model.owned_by,
            display_name: model.display_name,
            supported_input_types: model.supported_input_types,
            max_input_tokens: model.max_input_tokens,
            max_output_tokens: model.max_output_tokens,
            rate_multiplier: model.rate_multiplier,
            rate_unit: model.rate_unit,
        })
        .collect();

    Json(OpenAIModelsResponse {
        object: "list".to_string(),
        data,
    })
    .into_response()
}

/// POST /v1/chat/completions
pub async fn post_chat_completions(
    State(state): State<AppState>,
    JsonExtractor(req): JsonExtractor<ChatCompletionsRequest>,
) -> Response {
    tracing::info!(
        model = %req.model,
        stream = %req.stream,
        message_count = %req.messages.len(),
        "Received POST /v1/chat/completions request"
    );

    let provider = match &state.kiro_provider {
        Some(p) => p.clone(),
        None => {
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

    let stream = req.stream;
    let model = req.model.clone();

    // 1. OpenAI → 内部 MessagesRequest
    let mut payload = match openai_to_anthropic(req) {
        Ok(p) => p,
        Err(msg) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new("invalid_request_error", msg)),
            )
                .into_response();
        }
    };

    // thinking / model 后缀覆写
    override_thinking_from_model_name(&mut payload);

    // 2. 转换为 Kiro 请求
    let conversion_result = match convert_request(&payload) {
        Ok(r) => r,
        Err(e) => {
            let msg = match &e {
                ConversionError::UnsupportedModel(m) => format!("模型不支持: {}", m),
                ConversionError::EmptyMessages => "消息列表为空".to_string(),
            };
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new("invalid_request_error", msg)),
            )
                .into_response();
        }
    };

    let kiro_request = KiroRequest {
        conversation_state: conversion_result.conversation_state,
        profile_arn: None,
    };
    let tool_name_map = conversion_result.tool_name_map;

    let body = match serde_json::to_string(&kiro_request) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("OpenAI handler 序列化请求失败: {}", e);
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

    tracing::debug!("Kiro request body (openai): {}", body);

    // 估算 input_tokens
    let input_tokens = token::count_all_tokens(
        payload.model.clone(),
        payload.system.clone(),
        payload.messages.clone(),
        payload.tools.clone(),
    ) as i32;

    if stream {
        handle_stream(provider, body, model, input_tokens, tool_name_map).await
    } else {
        handle_non_stream(provider, body, model, input_tokens, tool_name_map).await
    }
}

// === 请求转换：OpenAI → Anthropic 内部 MessagesRequest ===

fn openai_to_anthropic(req: ChatCompletionsRequest) -> Result<MessagesRequest, String> {
    let mut system_texts = Vec::<String>::new();
    let mut anthropic_msgs = Vec::<AnthropicMessage>::new();
    let mut tool_call_id_mapper = ToolCallIdMapper::default();

    for msg in req.messages {
        match msg.role.as_str() {
            "system" | "developer" => {
                if let Some(s) = string_content(&msg.content) {
                    if !s.is_empty() {
                        system_texts.push(s);
                    }
                }
            }
            "user" => {
                anthropic_msgs.push(AnthropicMessage {
                    role: "user".to_string(),
                    content: convert_user_content(&msg.content),
                });
            }
            "assistant" => {
                anthropic_msgs.push(AnthropicMessage {
                    role: "assistant".to_string(),
                    content: convert_assistant_content(&msg, &mut tool_call_id_mapper),
                });
            }
            "tool" => {
                // 转成 user 的 tool_result block
                let original_tool_call_id = msg
                    .tool_call_id
                    .ok_or_else(|| "tool 角色的消息必须提供 tool_call_id".to_string())?;
                let tool_call_id = tool_call_id_mapper
                    .consume(&original_tool_call_id)
                    .unwrap_or_else(|| {
                        tracing::warn!(
                            original_tool_call_id = %original_tool_call_id,
                            "tool 结果未匹配到前序 assistant.tool_calls，回退使用原始 id"
                        );
                        original_tool_call_id
                    });
                let content_text = string_content(&msg.content).unwrap_or_default();
                anthropic_msgs.push(AnthropicMessage {
                    role: "user".to_string(),
                    content: json!([{
                        "type": "tool_result",
                        "tool_use_id": tool_call_id,
                        "content": content_text,
                    }]),
                });
            }
            other => {
                tracing::warn!("忽略未知 role: {}", other);
            }
        }
    }

    let system = if system_texts.is_empty() {
        None
    } else {
        Some(
            system_texts
                .into_iter()
                .map(|text| SystemMessage { text })
                .collect::<Vec<_>>(),
        )
    };

    let tools = req.tools.map(|tools| {
        tools
            .into_iter()
            .map(|t| AnthropicTool {
                tool_type: None,
                name: t.function.name,
                description: t.function.description,
                input_schema: normalize_schema_to_map(t.function.parameters),
                max_uses: None,
            })
            .collect::<Vec<_>>()
    });

    // reasoning_effort → thinking
    let thinking = req.reasoning_effort.as_deref().and_then(|e| match e {
        "low" | "medium" | "high" => Some(Thinking {
            thinking_type: "adaptive".to_string(),
            budget_tokens: 20000,
        }),
        _ => None,
    });

    let max_tokens = req.max_tokens.or(req.max_completion_tokens).unwrap_or(4096);

    Ok(MessagesRequest {
        model: req.model,
        max_tokens,
        messages: anthropic_msgs,
        stream: req.stream,
        system,
        tools,
        tool_choice: req.tool_choice,
        thinking,
        output_config: None,
        metadata: None,
    })
}

fn string_content(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Array(arr) => {
            let mut parts = Vec::new();
            for item in arr {
                if let Some(t) = item.get("text").and_then(|x| x.as_str()) {
                    parts.push(t.to_string());
                }
            }
            if parts.is_empty() {
                None
            } else {
                Some(parts.join("\n"))
            }
        }
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

fn convert_user_content(v: &Value) -> Value {
    match v {
        Value::String(s) => Value::String(s.clone()),
        Value::Array(arr) => {
            let mut blocks = Vec::<Value>::new();
            for item in arr {
                let ty = item.get("type").and_then(|x| x.as_str()).unwrap_or("");
                match ty {
                    "text" => {
                        if let Some(t) = item.get("text").and_then(|x| x.as_str()) {
                            blocks.push(json!({"type":"text","text": t}));
                        }
                    }
                    "image_url" => {
                        // OpenAI image_url: {"url":"data:image/png;base64,xxx"} 或 "https://..."
                        let url = item
                            .get("image_url")
                            .and_then(|iu| iu.get("url"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if let Some(base64) = url.strip_prefix("data:") {
                            if let Some((meta, data)) = base64.split_once(',') {
                                let media_type =
                                    meta.split(';').next().unwrap_or("image/png").to_string();
                                blocks.push(json!({
                                    "type":"image",
                                    "source":{
                                        "type":"base64",
                                        "media_type": media_type,
                                        "data": data,
                                    }
                                }));
                            }
                        }
                        // 远程 URL 暂不支持
                    }
                    _ => {}
                }
            }
            if blocks.is_empty() {
                Value::String(String::new())
            } else {
                Value::Array(blocks)
            }
        }
        _ => Value::String(String::new()),
    }
}

#[derive(Default)]
struct ToolCallIdMapper {
    pending: std::collections::HashMap<String, std::collections::VecDeque<String>>,
}

impl ToolCallIdMapper {
    fn register(&mut self, original_id: &str) -> String {
        let mapped_id = format!("toolu_{}", Uuid::new_v4().to_string().replace('-', ""));
        self.pending
            .entry(original_id.to_string())
            .or_default()
            .push_back(mapped_id.clone());
        mapped_id
    }

    fn consume(&mut self, original_id: &str) -> Option<String> {
        let queue = self.pending.get_mut(original_id)?;
        let mapped = queue.pop_front();
        if queue.is_empty() {
            self.pending.remove(original_id);
        }
        mapped
    }
}

fn convert_assistant_content(msg: &ChatMessage, tool_call_id_mapper: &mut ToolCallIdMapper) -> Value {
    let mut blocks = Vec::<Value>::new();
    if let Some(text) = string_content(&msg.content) {
        if !text.is_empty() {
            blocks.push(json!({"type":"text","text": text}));
        }
    }
    if let Some(tool_calls) = &msg.tool_calls {
        for tc in tool_calls {
            let mapped_id = tool_call_id_mapper.register(&tc.id);
            let input: Value = if tc.function.arguments.trim().is_empty() {
                json!({})
            } else {
                serde_json::from_str(&tc.function.arguments).unwrap_or_else(|_| json!({}))
            };
            blocks.push(json!({
                "type":"tool_use",
                "id": mapped_id,
                "name": tc.function.name,
                "input": input,
            }));
        }
    }
    if blocks.is_empty() {
        Value::String(String::new())
    } else {
        Value::Array(blocks)
    }
}

fn normalize_schema_to_map(schema: Value) -> std::collections::HashMap<String, Value> {
    match schema {
        Value::Object(map) => map.into_iter().collect(),
        _ => {
            let mut m = std::collections::HashMap::new();
            m.insert("type".to_string(), Value::String("object".to_string()));
            m.insert("properties".to_string(), json!({}));
            m
        }
    }
}

// === 非流式响应 ===

async fn handle_non_stream(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: String,
    model: String,
    estimated_input_tokens: i32,
    tool_name_map: std::collections::HashMap<String, String>,
) -> Response {
    let response = match provider.call_api(&request_body).await {
        Ok(r) => r,
        Err(e) => return map_provider_error(e, Some(&request_body)),
    };

    let body_bytes = match response.bytes().await {
        Ok(b) => b,
        Err(e) => {
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

    let mut decoder = EventStreamDecoder::new();
    let _ = decoder.feed(&body_bytes);

    let mut text = String::new();
    let mut tool_calls = Vec::<ChatResponseToolCall>::new();
    let mut tool_buffers: std::collections::HashMap<String, (String, String, String)> =
        std::collections::HashMap::new(); // tool_use_id -> (name, args_buffer, id)
    let mut tool_order: Vec<String> = Vec::new();
    let mut has_tool_use = false;
    let mut stop_reason_openai = "stop".to_string();
    let mut context_input_tokens: Option<i32> = None;

    for r in decoder.decode_iter() {
        let Ok(frame) = r else { continue };
        let Ok(event) = Event::from_frame(frame) else {
            continue;
        };
        match event {
            Event::AssistantResponse(resp) => text.push_str(&resp.content),
            Event::ToolUse(tu) => {
                has_tool_use = true;
                let entry = tool_buffers
                    .entry(tu.tool_use_id.clone())
                    .or_insert_with(|| {
                        tool_order.push(tu.tool_use_id.clone());
                        let original_name = tool_name_map
                            .get(&tu.name)
                            .cloned()
                            .unwrap_or(tu.name.clone());
                        (original_name, String::new(), tu.tool_use_id.clone())
                    });
                entry.1.push_str(&tu.input);
            }
            Event::ContextUsage(cu) => {
                let window = get_context_window_size(&model);
                let actual = (cu.context_usage_percentage * (window as f64) / 100.0) as i32;
                context_input_tokens = Some(actual);
                if cu.context_usage_percentage >= 100.0 {
                    stop_reason_openai = "length".to_string();
                }
            }
            Event::Exception { exception_type, .. } => {
                if exception_type == "ContentLengthExceededException" {
                    stop_reason_openai = "length".to_string();
                }
            }
            _ => {}
        }
    }

    // 构造 tool_calls（保持出现顺序）
    for (idx, tool_use_id) in tool_order.iter().enumerate() {
        if let Some((name, args, id)) = tool_buffers.remove(tool_use_id) {
            let parsed: Value = if args.is_empty() {
                json!({})
            } else {
                serde_json::from_str(&args).unwrap_or_else(|_| json!({}))
            };
            tool_calls.push(ChatResponseToolCall {
                id,
                r#type: "function",
                function: ChatResponseFunction {
                    name,
                    arguments: serde_json::to_string(&parsed).unwrap_or_else(|_| "{}".to_string()),
                },
                index: idx as i32,
            });
        }
    }

    if has_tool_use && stop_reason_openai == "stop" {
        stop_reason_openai = "tool_calls".to_string();
    }

    // 剥离 <thinking>...</thinking>（OpenAI 没等价概念，直接丢弃）
    let (_, visible_text) = super::stream::extract_thinking_from_complete_text(&text);

    let output_content = [json!({"type":"text","text":visible_text})];
    let output_tokens = token::estimate_output_tokens(&output_content);
    let input_tokens = context_input_tokens.unwrap_or(estimated_input_tokens);

    let completion = ChatCompletion {
        id: format!("chatcmpl-{}", Uuid::new_v4().to_string().replace('-', "")),
        object: "chat.completion",
        created: chrono_now_secs(),
        model,
        choices: vec![ChatChoice {
            index: 0,
            message: ChatResponseMessage {
                role: "assistant",
                content: if visible_text.is_empty() && !tool_calls.is_empty() {
                    None
                } else {
                    Some(visible_text)
                },
                tool_calls: if tool_calls.is_empty() {
                    None
                } else {
                    Some(tool_calls)
                },
            },
            finish_reason: stop_reason_openai,
        }],
        usage: ChatUsage {
            prompt_tokens: input_tokens,
            completion_tokens: output_tokens,
            total_tokens: input_tokens + output_tokens,
        },
    };

    (StatusCode::OK, Json(completion)).into_response()
}

// === 流式响应 ===

const PING_INTERVAL_SECS: u64 = 25;

async fn handle_stream(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: String,
    model: String,
    estimated_input_tokens: i32,
    tool_name_map: std::collections::HashMap<String, String>,
) -> Response {
    let response = match provider.call_api_stream(&request_body).await {
        Ok(r) => r,
        Err(e) => return map_provider_error(e, Some(&request_body)),
    };

    let id = format!("chatcmpl-{}", Uuid::new_v4().to_string().replace('-', ""));
    let created = chrono_now_secs();

    let stream = create_openai_sse_stream(
        response,
        id,
        created,
        model,
        estimated_input_tokens,
        tool_name_map,
    );

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(stream))
        .unwrap()
}

struct OpenAIStreamState {
    id: String,
    created: i64,
    model: String,
    /// 是否已经发过初始 role=assistant chunk
    started: bool,
    /// 正在处理中的工具调用：tool_use_id -> index（出现顺序）
    tool_index: std::collections::HashMap<String, i32>,
    /// tool_use_id -> 原始工具名（首个 partial 里确定）
    tool_emitted_name: std::collections::HashSet<String>,
    next_index: i32,
    has_tool_use: bool,
    finish_reason: String,
    tool_name_map: std::collections::HashMap<String, String>,
    input_tokens: i32,
    context_input_tokens: Option<i32>,
    output_tokens: i32,
    finished: bool,
}

impl OpenAIStreamState {
    fn new(
        id: String,
        created: i64,
        model: String,
        input_tokens: i32,
        tool_name_map: std::collections::HashMap<String, String>,
    ) -> Self {
        Self {
            id,
            created,
            model,
            started: false,
            tool_index: std::collections::HashMap::new(),
            tool_emitted_name: std::collections::HashSet::new(),
            next_index: 0,
            has_tool_use: false,
            finish_reason: "stop".to_string(),
            tool_name_map,
            input_tokens,
            context_input_tokens: None,
            output_tokens: 0,
            finished: false,
        }
    }

    fn wrap_chunk(&self, choices: Value) -> String {
        let chunk = json!({
            "id": self.id.clone(),
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model.clone(),
            "choices": choices,
        });
        format!(
            "data: {}\n\n",
            serde_json::to_string(&chunk).unwrap_or_else(|_| "{}".to_string())
        )
    }

    fn start_chunk(&mut self) -> Option<String> {
        if self.started {
            return None;
        }
        self.started = true;
        Some(self.wrap_chunk(json!([
            {"index":0, "delta": {"role":"assistant", "content":""}, "finish_reason": null}
        ])))
    }

    /// 处理 Kiro 事件，返回应发送的 SSE 字符串（可能多个合并）
    fn on_event(&mut self, event: &Event) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(s) = self.start_chunk() {
            out.push(s);
        }
        match event {
            Event::AssistantResponse(resp) => {
                self.output_tokens += (resp.content.len() / 4).max(1) as i32;
                out.push(self.wrap_chunk(json!([
                    {"index":0, "delta": {"content": resp.content.clone()}, "finish_reason": null}
                ])));
            }
            Event::ToolUse(tu) => {
                self.has_tool_use = true;
                let idx = if let Some(i) = self.tool_index.get(&tu.tool_use_id) {
                    *i
                } else {
                    let i = self.next_index;
                    self.next_index += 1;
                    self.tool_index.insert(tu.tool_use_id.clone(), i);
                    i
                };
                let original_name = self
                    .tool_name_map
                    .get(&tu.name)
                    .cloned()
                    .unwrap_or_else(|| tu.name.clone());

                // 首次出现时发送 id + function.name，后续只发 arguments delta
                if !self.tool_emitted_name.contains(&tu.tool_use_id) {
                    self.tool_emitted_name.insert(tu.tool_use_id.clone());
                    out.push(self.wrap_chunk(json!([
                        {
                            "index":0,
                            "delta": {
                                "tool_calls": [{
                                    "index": idx,
                                    "id": tu.tool_use_id.clone(),
                                    "type": "function",
                                    "function": {"name": original_name, "arguments": tu.input.clone()},
                                }]
                            },
                            "finish_reason": null
                        }
                    ])));
                } else if !tu.input.is_empty() {
                    out.push(self.wrap_chunk(json!([
                        {
                            "index":0,
                            "delta": {
                                "tool_calls": [{
                                    "index": idx,
                                    "function": {"arguments": tu.input.clone()},
                                }]
                            },
                            "finish_reason": null
                        }
                    ])));
                }
            }
            Event::ContextUsage(cu) => {
                let window = get_context_window_size(&self.model);
                let actual = (cu.context_usage_percentage * (window as f64) / 100.0) as i32;
                self.context_input_tokens = Some(actual);
                if cu.context_usage_percentage >= 100.0 {
                    self.finish_reason = "length".to_string();
                }
            }
            Event::Exception { exception_type, .. } => {
                if exception_type == "ContentLengthExceededException" {
                    self.finish_reason = "length".to_string();
                }
            }
            _ => {}
        }
        out
    }

    /// 生成最终 chunk + DONE
    fn final_chunks(&mut self) -> Vec<String> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        let mut out = Vec::new();
        if !self.started {
            if let Some(s) = self.start_chunk() {
                out.push(s);
            }
        }
        if self.has_tool_use && self.finish_reason == "stop" {
            self.finish_reason = "tool_calls".to_string();
        }
        let prompt_tokens = self.context_input_tokens.unwrap_or(self.input_tokens);
        let usage = json!({
            "prompt_tokens": prompt_tokens,
            "completion_tokens": self.output_tokens,
            "total_tokens": prompt_tokens + self.output_tokens,
        });
        let chunk = json!({
            "id": self.id.clone(),
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model.clone(),
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": self.finish_reason.clone(),
            }],
            "usage": usage,
        });
        out.push(format!(
            "data: {}\n\n",
            serde_json::to_string(&chunk).unwrap_or_else(|_| "{}".to_string())
        ));
        out.push("data: [DONE]\n\n".to_string());
        out
    }
}

fn create_openai_sse_stream(
    response: reqwest::Response,
    id: String,
    created: i64,
    model: String,
    input_tokens: i32,
    tool_name_map: std::collections::HashMap<String, String>,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    let state = OpenAIStreamState::new(id, created, model, input_tokens, tool_name_map);
    let body_stream = response.bytes_stream();

    stream::unfold(
        (
            body_stream,
            state,
            EventStreamDecoder::new(),
            false,
            interval(Duration::from_secs(PING_INTERVAL_SECS)),
        ),
        |(mut body_stream, mut state, mut decoder, finished, mut ping_interval)| async move {
            if finished {
                return None;
            }

            tokio::select! {
                chunk_result = body_stream.next() => {
                    match chunk_result {
                        Some(Ok(chunk)) => {
                            let _ = decoder.feed(&chunk);
                            let mut out_strings: Vec<String> = Vec::new();
                            for r in decoder.decode_iter() {
                                let Ok(frame) = r else { continue; };
                                let Ok(event) = Event::from_frame(frame) else { continue; };
                                out_strings.extend(state.on_event(&event));
                            }
                            let bytes: Vec<Result<Bytes, Infallible>> = out_strings
                                .into_iter()
                                .map(|s| Ok(Bytes::from(s)))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, state, decoder, false, ping_interval)))
                        }
                        Some(Err(e)) => {
                            tracing::error!("OpenAI 流读取失败: {}", e);
                            let final_strings = state.final_chunks();
                            let bytes: Vec<Result<Bytes, Infallible>> = final_strings
                                .into_iter()
                                .map(|s| Ok(Bytes::from(s)))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, state, decoder, true, ping_interval)))
                        }
                        None => {
                            let final_strings = state.final_chunks();
                            let bytes: Vec<Result<Bytes, Infallible>> = final_strings
                                .into_iter()
                                .map(|s| Ok(Bytes::from(s)))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, state, decoder, true, ping_interval)))
                        }
                    }
                }
                _ = ping_interval.tick() => {
                    // OpenAI 兼容：发送 SSE comment 保活（不影响客户端）
                    let bytes: Vec<Result<Bytes, Infallible>> = vec![Ok(Bytes::from(": keep-alive\n\n"))];
                    Some((stream::iter(bytes), (body_stream, state, decoder, false, ping_interval)))
                }
            }
        },
    )
    .flatten()
}

fn chrono_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn format_request_body_for_log(request_body: &str) -> String {
    serde_json::from_str::<Value>(request_body)
        .and_then(|value| serde_json::to_string_pretty(&value))
        .unwrap_or_else(|_| request_body.to_string())
}

fn map_provider_error(err: anyhow::Error, request_body: Option<&str>) -> Response {
    let err_str = err.to_string();
    if err_str.contains("CONTENT_LENGTH_EXCEEDS_THRESHOLD") {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "Context window is full. Reduce conversation history.",
            )),
        )
            .into_response();
    }
    if err_str.contains("Input is too long") {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "Input is too long.",
            )),
        )
            .into_response();
    }
    if err_str.contains("Improperly formed request") {
        if let Some(request_body) = request_body {
            tracing::error!(
                "OpenAI 转换后的 Kiro 请求体（上游判定 Improperly formed request）:\n{}",
                format_request_body_for_log(request_body)
            );
        }
    }
    tracing::error!("Kiro API 调用失败 (openai): {}", err);
    (
        StatusCode::BAD_GATEWAY,
        Json(ErrorResponse::new(
            "api_error",
            format!("上游 API 调用失败: {}", err),
        )),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extract_single_tool_use_id(message: &AnthropicMessage) -> String {
        message
            .content
            .as_array()
            .and_then(|blocks| blocks.iter().find(|block| block.get("type") == Some(&Value::String("tool_use".to_string()))))
            .and_then(|block| block.get("id"))
            .and_then(|id| id.as_str())
            .expect("assistant message should contain tool_use id")
            .to_string()
    }

    fn extract_single_tool_result_id(message: &AnthropicMessage) -> String {
        message
            .content
            .as_array()
            .and_then(|blocks| blocks.iter().find(|block| block.get("type") == Some(&Value::String("tool_result".to_string()))))
            .and_then(|block| block.get("tool_use_id"))
            .and_then(|id| id.as_str())
            .expect("user message should contain tool_result id")
            .to_string()
    }

    #[test]
    fn test_openai_to_anthropic_remaps_reused_tool_call_ids_per_round() {
        let req = ChatCompletionsRequest {
            model: "glm-5".to_string(),
            messages: vec![
                ChatMessage {
                    role: "user".to_string(),
                    content: Value::String("first".to_string()),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
                ChatMessage {
                    role: "assistant".to_string(),
                    content: Value::Null,
                    tool_calls: Some(vec![ChatToolCall {
                        id: "call_shared".to_string(),
                        r#type: "function".to_string(),
                        function: ChatFunctionCall {
                            name: "glob".to_string(),
                            arguments: r#"{"pattern":"**/package.json"}"#.to_string(),
                        },
                    }]),
                    tool_call_id: None,
                    name: None,
                },
                ChatMessage {
                    role: "tool".to_string(),
                    content: Value::String("result-1".to_string()),
                    tool_calls: None,
                    tool_call_id: Some("call_shared".to_string()),
                    name: None,
                },
                ChatMessage {
                    role: "assistant".to_string(),
                    content: Value::Null,
                    tool_calls: Some(vec![ChatToolCall {
                        id: "call_shared".to_string(),
                        r#type: "function".to_string(),
                        function: ChatFunctionCall {
                            name: "grep".to_string(),
                            arguments: r#"{"pattern":"TODO"}"#.to_string(),
                        },
                    }]),
                    tool_call_id: None,
                    name: None,
                },
                ChatMessage {
                    role: "tool".to_string(),
                    content: Value::String("result-2".to_string()),
                    tool_calls: None,
                    tool_call_id: Some("call_shared".to_string()),
                    name: None,
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: Value::String("continue".to_string()),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
            ],
            stream: true,
            max_tokens: Some(128),
            max_completion_tokens: None,
            tools: None,
            tool_choice: None,
            temperature: None,
            top_p: None,
            reasoning_effort: None,
        };

        let payload = openai_to_anthropic(req).expect("conversion should succeed");

        assert_eq!(payload.messages.len(), 6);

        let first_tool_use_id = extract_single_tool_use_id(&payload.messages[1]);
        let first_tool_result_id = extract_single_tool_result_id(&payload.messages[2]);
        let second_tool_use_id = extract_single_tool_use_id(&payload.messages[3]);
        let second_tool_result_id = extract_single_tool_result_id(&payload.messages[4]);

        assert_eq!(first_tool_use_id, first_tool_result_id);
        assert_eq!(second_tool_use_id, second_tool_result_id);
        assert_ne!(first_tool_use_id, second_tool_use_id);
        assert!(first_tool_use_id.starts_with("toolu_"));
        assert!(second_tool_use_id.starts_with("toolu_"));
    }
}
