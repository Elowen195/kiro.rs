//! Kiro CLI 端点
//!
//! 对应 Amazon Q CLI (kiroCLI / `q chat`) 使用的 AWS CodeWhisperer 端点：
//! - API: `https://q.{api_region}.amazonaws.com/`（通过 `x-amz-target` header 区分动作）
//! - MCP: `https://q.{api_region}.amazonaws.com/mcp`
//!
//! 与 IDE 端点的差异（均源自真实抓包 extracted-chat-requests.log）:
//! - URL 统一为 `/`，不带 `/generateAssistantResponse` 路径
//! - content-type: `application/x-amz-json-1.0`
//! - x-amz-target: `AmazonCodeWhispererStreamingService.GenerateAssistantResponse`
//! - user-agent: `aws-sdk-rust/... app/AmazonQ-For-CLI`
//! - body: 不携带 `modelId`/`agentContinuationId`/`chatTriggerType`/`agentTaskType`/`profileArn`；
//!         `currentMessage.userInputMessage` 不携带 `origin`；
//!         所有 `userInputMessageContext` 必须带 `envState`；
//!         history 中 `userInputMessage` 的 `origin` 必须为 `KIRO_CLI`。

use reqwest::RequestBuilder;
use uuid::Uuid;

use super::{KiroEndpoint, RequestContext};

/// Kiro CLI 端点名称
pub const CLI_ENDPOINT_NAME: &str = "cli";

/// aws-sdk-rust 版本标识
const SDK_VERSION: &str = "aws-sdk-rust/1.3.14";
/// codewhispererstreaming 服务版本
const STREAMING_API_VERSION: &str = "api/codewhispererstreaming/0.1.14474";
/// codewhispererruntime 服务版本（用于 ListAvailableModels 等非流式 API）
const RUNTIME_API_VERSION: &str = "api/codewhispererruntime/0.1.14474";

/// ListAvailableModels 上游规格
pub struct ListModelsSpec {
    pub url: String,
    pub x_amz_target: &'static str,
    pub body: String,
}

/// Kiro CLI 端点
pub struct CliEndpoint;

impl CliEndpoint {
    pub fn new() -> Self {
        Self
    }

    fn api_region<'a>(&self, ctx: &'a RequestContext<'_>) -> &'a str {
        ctx.credentials.effective_api_region(ctx.config)
    }

    fn host(&self, ctx: &RequestContext<'_>) -> String {
        format!("q.{}.amazonaws.com", self.api_region(ctx))
    }

    /// CLI streaming API 的 user-agent
    fn streaming_user_agent(&self, ctx: &RequestContext<'_>) -> String {
        format!(
            "{} ua/2.1 {} os/linux lang/rust/1.92.0 md/appVersion-{} app/AmazonQ-For-CLI",
            SDK_VERSION, STREAMING_API_VERSION, ctx.config.kiro_version
        )
    }

    /// CLI streaming API 的 x-amz-user-agent
    fn streaming_x_amz_user_agent(&self) -> String {
        format!(
            "{} ua/2.1 {} os/linux lang/rust/1.92.0 m/F app/AmazonQ-For-CLI",
            SDK_VERSION, STREAMING_API_VERSION
        )
    }

    /// CLI runtime API 的 user-agent（用于 ListAvailableModels 等）
    pub fn runtime_user_agent(&self, ctx: &RequestContext<'_>) -> String {
        format!(
            "{} ua/2.1 {} os/linux lang/rust/1.92.0 md/appVersion-{} app/AmazonQ-For-CLI",
            SDK_VERSION, RUNTIME_API_VERSION, ctx.config.kiro_version
        )
    }

    /// CLI runtime API 的 x-amz-user-agent
    pub fn runtime_x_amz_user_agent(&self) -> String {
        format!(
            "{} ua/2.1 {} os/linux lang/rust/1.92.0 m/F,C app/AmazonQ-For-CLI",
            SDK_VERSION, RUNTIME_API_VERSION
        )
    }

    /// 生成 envState 对象（本机 os / cwd）
    fn env_state() -> serde_json::Value {
        let os = if cfg!(target_os = "windows") {
            "windows"
        } else if cfg!(target_os = "macos") {
            "macos"
        } else {
            "linux"
        };
        let cwd = std::env::current_dir()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| "/".to_string());
        serde_json::json!({
            "operatingSystem": os,
            "currentWorkingDirectory": cwd,
        })
    }

    /// 针对 ListAvailableModels API 的请求规格
    pub fn list_models_spec(&self, ctx: &RequestContext<'_>) -> ListModelsSpec {
        let url = format!(
            "https://q.{}.amazonaws.com/?origin=KIRO_CLI",
            self.api_region(ctx)
        );
        ListModelsSpec {
            url,
            x_amz_target: "AmazonCodeWhispererService.ListAvailableModels",
            body: r#"{"origin":"KIRO_CLI"}"#.to_string(),
        }
    }

    /// 装饰 ListAvailableModels 请求的 header
    pub fn decorate_list_models(
        &self,
        req: RequestBuilder,
        ctx: &RequestContext<'_>,
    ) -> RequestBuilder {
        let mut req = req
            .header("content-type", "application/x-amz-json-1.0")
            .header(
                "x-amz-target",
                "AmazonCodeWhispererService.ListAvailableModels",
            )
            .header("x-amz-user-agent", self.runtime_x_amz_user_agent())
            .header("user-agent", self.runtime_user_agent(ctx))
            .header("x-amzn-codewhisperer-optout", "false")
            .header("host", self.host(ctx))
            .header("amz-sdk-invocation-id", Uuid::new_v4().to_string())
            .header("amz-sdk-request", "attempt=1; max=3")
            .header("Authorization", format!("Bearer {}", ctx.token))
            .header("accept", "*/*")
            .header("accept-encoding", "gzip");
        if ctx.credentials.is_api_key_credential() {
            req = req.header("tokentype", "API_KEY");
        }
        req
    }
}

impl Default for CliEndpoint {
    fn default() -> Self {
        Self::new()
    }
}

impl KiroEndpoint for CliEndpoint {
    fn name(&self) -> &'static str {
        CLI_ENDPOINT_NAME
    }

    fn api_url(&self, ctx: &RequestContext<'_>) -> String {
        // CLI 端点统一走根路径 `/`，通过 x-amz-target 区分动作
        format!("https://q.{}.amazonaws.com/", self.api_region(ctx))
    }

    fn mcp_url(&self, ctx: &RequestContext<'_>) -> String {
        format!("https://q.{}.amazonaws.com/mcp", self.api_region(ctx))
    }

    fn decorate_api(&self, req: RequestBuilder, ctx: &RequestContext<'_>) -> RequestBuilder {
        // 显式覆盖 content-type（provider 已设置 application/json）
        let mut req = req
            .header("content-type", "application/x-amz-json-1.0")
            .header(
                "x-amz-target",
                "AmazonCodeWhispererStreamingService.GenerateAssistantResponse",
            )
            .header("x-amzn-codewhisperer-optout", "false")
            .header("x-amz-user-agent", self.streaming_x_amz_user_agent())
            .header("user-agent", self.streaming_user_agent(ctx))
            .header("host", self.host(ctx))
            .header("amz-sdk-invocation-id", Uuid::new_v4().to_string())
            .header("amz-sdk-request", "attempt=1; max=3")
            .header("Authorization", format!("Bearer {}", ctx.token))
            .header("accept", "*/*")
            .header("accept-encoding", "gzip");
        if ctx.credentials.is_api_key_credential() {
            req = req.header("tokentype", "API_KEY");
        }
        req
    }

    fn decorate_mcp(&self, req: RequestBuilder, ctx: &RequestContext<'_>) -> RequestBuilder {
        let mut req = req
            .header("content-type", "application/json")
            .header("x-amz-user-agent", self.streaming_x_amz_user_agent())
            .header("user-agent", self.streaming_user_agent(ctx))
            .header("host", self.host(ctx))
            .header("amz-sdk-invocation-id", Uuid::new_v4().to_string())
            .header("amz-sdk-request", "attempt=1; max=3")
            .header("Authorization", format!("Bearer {}", ctx.token));
        if ctx.credentials.is_api_key_credential() {
            req = req.header("tokentype", "API_KEY");
        }
        req
    }

    fn transform_api_body(&self, body: &str, _ctx: &RequestContext<'_>) -> String {
        adapt_body_for_cli(body)
    }
}

/// 把 IDE 格式的 Kiro 请求体改造为 CLI 格式（不修改 converter.rs）
///
/// 操作（基于 kiroCLI 真实抓包）：
/// - 删除 `conversationState` 顶层：`agentContinuationId` / `agentTaskType` / `chatTriggerType`
/// - 删除根对象 `profileArn`（IDE 端点的 transform 才注入）
/// - `currentMessage.userInputMessage`：删除 `modelId` 和 `origin`，注入 `envState`
/// - `history[].userInputMessage`：删除 `modelId`，强制 `origin = "KIRO_CLI"`，注入 `envState`
fn adapt_body_for_cli(body: &str) -> String {
    let Ok(mut json) = serde_json::from_str::<serde_json::Value>(body) else {
        return body.to_string();
    };

    // 根对象：不要 profileArn（CLI 端点不发）
    if let serde_json::Value::Object(ref mut root) = json {
        root.remove("profileArn");
    }

    let Some(state) = json.get_mut("conversationState") else {
        return serde_json::to_string(&json).unwrap_or_else(|_| body.to_string());
    };

    if let serde_json::Value::Object(ref mut state_obj) = state {
        state_obj.remove("agentContinuationId");
        state_obj.remove("agentTaskType");
        state_obj.remove("chatTriggerType");
    }

    // currentMessage.userInputMessage
    if let Some(current) = state
        .get_mut("currentMessage")
        .and_then(|v| v.get_mut("userInputMessage"))
    {
        if let serde_json::Value::Object(ref mut obj) = current {
            obj.remove("modelId");
            obj.remove("origin");
            inject_env_state(obj);
        }
    }

    // history[]: 为每个 user 消息做同样处理（但保留 origin=KIRO_CLI）
    if let Some(history) = state.get_mut("history").and_then(|v| v.as_array_mut()) {
        for msg in history.iter_mut() {
            let Some(user) = msg.get_mut("userInputMessage") else {
                continue;
            };
            if let serde_json::Value::Object(ref mut obj) = user {
                obj.remove("modelId");
                obj.insert(
                    "origin".to_string(),
                    serde_json::Value::String("KIRO_CLI".to_string()),
                );
                inject_env_state(obj);
            }
        }
    }

    serde_json::to_string(&json).unwrap_or_else(|_| body.to_string())
}

/// 在 userInputMessage 对象里确保 `userInputMessageContext.envState` 存在
fn inject_env_state(obj: &mut serde_json::Map<String, serde_json::Value>) {
    let ctx = obj
        .entry("userInputMessageContext".to_string())
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    if let serde_json::Value::Object(ref mut ctx_obj) = ctx {
        if !ctx_obj.contains_key("envState") {
            ctx_obj.insert("envState".to_string(), CliEndpoint::env_state());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adapt_body_strips_ide_fields() {
        let input = r#"{
            "profileArn": "arn:aws:codewhisperer:us-east-1:123:profile/X",
            "conversationState": {
                "agentContinuationId": "abc",
                "agentTaskType": "vibe",
                "chatTriggerType": "MANUAL",
                "conversationId": "c1",
                "currentMessage": {
                    "userInputMessage": {
                        "content": "hi",
                        "modelId": "claude-sonnet-4.5",
                        "origin": "AI_EDITOR"
                    }
                },
                "history": []
            }
        }"#;
        let out = adapt_body_for_cli(input);
        let json: serde_json::Value = serde_json::from_str(&out).unwrap();

        // 根对象 profileArn 被移除
        assert!(json.get("profileArn").is_none());
        // 顶层字段被移除
        let state = &json["conversationState"];
        assert!(state.get("agentContinuationId").is_none());
        assert!(state.get("agentTaskType").is_none());
        assert!(state.get("chatTriggerType").is_none());
        // currentMessage 的 modelId/origin 被移除
        let cur = &state["currentMessage"]["userInputMessage"];
        assert!(cur.get("modelId").is_none());
        assert!(cur.get("origin").is_none());
        // envState 被注入
        assert!(cur["userInputMessageContext"]["envState"].is_object());
        assert_eq!(state["conversationId"], "c1");
    }

    #[test]
    fn test_adapt_body_history_user_origin_forced_to_cli() {
        let input = r#"{
            "conversationState": {
                "conversationId": "c1",
                "currentMessage": {
                    "userInputMessage": {
                        "content": "q",
                        "modelId": "claude-sonnet-4.5"
                    }
                },
                "history": [
                    {
                        "userInputMessage": {
                            "content": "sys",
                            "modelId": "claude-sonnet-4.5",
                            "origin": "AI_EDITOR"
                        }
                    },
                    {
                        "assistantResponseMessage": {"content": "ok"}
                    }
                ]
            }
        }"#;
        let out = adapt_body_for_cli(input);
        let json: serde_json::Value = serde_json::from_str(&out).unwrap();
        let hist_user = &json["conversationState"]["history"][0]["userInputMessage"];
        assert_eq!(hist_user["origin"], "KIRO_CLI");
        assert!(hist_user.get("modelId").is_none());
        assert!(hist_user["userInputMessageContext"]["envState"].is_object());
        // assistant 消息不被改动
        assert_eq!(
            json["conversationState"]["history"][1]["assistantResponseMessage"]["content"],
            "ok"
        );
    }

    #[test]
    fn test_adapt_body_invalid_json_passthrough() {
        let input = "not-valid-json";
        let out = adapt_body_for_cli(input);
        assert_eq!(out, "not-valid-json");
    }

    #[test]
    fn test_env_state_shape() {
        let env = CliEndpoint::env_state();
        assert!(env.is_object());
        assert!(env["operatingSystem"].is_string());
        assert!(env["currentWorkingDirectory"].is_string());
    }
}
