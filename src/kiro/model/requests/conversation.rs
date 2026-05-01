//! 对话类型定义
//!
//! 定义 Kiro API 中对话相关的类型，包括消息、历史记录等

use serde::{Deserialize, Serialize};

use super::tool::{Tool, ToolResult, ToolUseEntry};

/// 对话状态
///
/// Kiro API 请求中的核心结构，包含当前消息和历史记录。
///
/// 字段顺序与官方 kiro-cli 实际发送的请求保持一致：
/// `conversationId, history, currentMessage, chatTriggerType, agentContinuationId, agentTaskType`。
/// JSON 在标准下不依赖字段顺序，但部分 AWS 服务对字段顺序敏感，对齐能减少潜在的 ValidationException 风险。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationState {
    /// 会话 ID
    pub conversation_id: String,
    /// 历史消息列表
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<Message>,
    /// 当前消息
    pub current_message: CurrentMessage,
    /// 聊天触发类型（"MANUAL" 或 "AUTO"）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_trigger_type: Option<String>,
    /// 代理延续 ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_continuation_id: Option<String>,
    /// 代理任务类型（通常为 "vibe"）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_task_type: Option<String>,
}

impl ConversationState {
    /// 创建新的对话状态
    pub fn new(conversation_id: impl Into<String>) -> Self {
        Self {
            conversation_id: conversation_id.into(),
            history: Vec::new(),
            current_message: CurrentMessage::default(),
            chat_trigger_type: None,
            agent_continuation_id: None,
            agent_task_type: None,
        }
    }

    /// 设置代理延续 ID
    pub fn with_agent_continuation_id(mut self, id: impl Into<String>) -> Self {
        self.agent_continuation_id = Some(id.into());
        self
    }

    /// 设置代理任务类型
    pub fn with_agent_task_type(mut self, task_type: impl Into<String>) -> Self {
        self.agent_task_type = Some(task_type.into());
        self
    }

    /// 设置聊天触发类型
    pub fn with_chat_trigger_type(mut self, trigger_type: impl Into<String>) -> Self {
        self.chat_trigger_type = Some(trigger_type.into());
        self
    }

    /// 设置当前消息
    pub fn with_current_message(mut self, message: CurrentMessage) -> Self {
        self.current_message = message;
        self
    }

    /// 添加历史消息
    pub fn with_history(mut self, history: Vec<Message>) -> Self {
        self.history = history;
        self
    }
}

/// 当前消息容器
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentMessage {
    /// 用户输入消息
    pub user_input_message: UserInputMessage,
}

impl CurrentMessage {
    /// 创建新的当前消息
    pub fn new(user_input_message: UserInputMessage) -> Self {
        Self { user_input_message }
    }
}

/// 用户输入消息
///
/// 字段顺序对齐官方 kiro-cli：`content, userInputMessageContext, origin, modelId`。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInputMessage {
    /// 消息内容
    pub content: String,
    /// 用户输入消息上下文
    pub user_input_message_context: UserInputMessageContext,
    /// 消息来源（通常为 "AI_EDITOR" 或 "KIRO_CLI"）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    /// 模型 ID（CLI 端点下可能留空）
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub model_id: String,
    /// 图片列表
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<KiroImage>,
}

impl UserInputMessage {
    /// 创建新的用户输入消息
    pub fn new(content: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            user_input_message_context: UserInputMessageContext::default(),
            origin: Some("AI_EDITOR".to_string()),
            model_id: model_id.into(),
            images: Vec::new(),
        }
    }

    /// 设置消息上下文
    pub fn with_context(mut self, context: UserInputMessageContext) -> Self {
        self.user_input_message_context = context;
        self
    }

    /// 添加图片
    pub fn with_images(mut self, images: Vec<KiroImage>) -> Self {
        self.images = images;
        self
    }

    /// 设置来源
    pub fn with_origin(mut self, origin: impl Into<String>) -> Self {
        self.origin = Some(origin.into());
        self
    }
}

/// 环境状态
///
/// CLI 端点下 userInputMessageContext 必须携带 envState，
/// 缺失会导致上游返回 400 "Improperly formed request"。
/// 字段顺序对齐 kiro-cli：`operatingSystem, currentWorkingDirectory`。
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EnvState {
    /// 操作系统（linux / macos / windows）
    pub operating_system: String,
    /// 当前工作目录
    pub current_working_directory: String,
}

/// 用户输入消息上下文
///
/// 包含工具定义和工具执行结果。字段顺序对齐 kiro-cli：
/// `envState, tools, toolResults`（kiro-cli 真实请求里通常没有 toolResults，所以放在最后）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInputMessageContext {
    /// 环境状态（CLI 端点下必需）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_state: Option<EnvState>,
    /// 可用工具列表
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<Tool>,
    /// 工具执行结果列表
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_results: Vec<ToolResult>,
}

impl UserInputMessageContext {
    /// 创建新的消息上下文
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置工具列表
    pub fn with_tools(mut self, tools: Vec<Tool>) -> Self {
        self.tools = tools;
        self
    }

    /// 设置工具结果
    pub fn with_tool_results(mut self, results: Vec<ToolResult>) -> Self {
        self.tool_results = results;
        self
    }

    /// 设置环境状态
    pub fn with_env_state(mut self, env_state: EnvState) -> Self {
        self.env_state = Some(env_state);
        self
    }
}

/// Kiro 图片
///
/// API 中使用的图片格式
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KiroImage {
    /// 图片格式（"jpeg", "png", "gif", "webp"）
    pub format: String,
    /// 图片数据源
    pub source: KiroImageSource,
}

impl KiroImage {
    /// 从 base64 数据创建图片
    pub fn from_base64(format: impl Into<String>, data: impl Into<String>) -> Self {
        Self {
            format: format.into(),
            source: KiroImageSource { bytes: data.into() },
        }
    }
}

/// Kiro 图片数据源
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KiroImageSource {
    /// base64 编码的图片数据
    pub bytes: String,
}

/// 历史消息
///
/// 可以是用户消息或助手消息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message {
    /// 用户消息
    User(HistoryUserMessage),
    /// 助手消息
    Assistant(HistoryAssistantMessage),
}

/// 历史用户消息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryUserMessage {
    /// 用户输入消息
    pub user_input_message: UserMessage,
}

impl HistoryUserMessage {
    /// 创建新的历史用户消息
    pub fn new(content: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self {
            user_input_message: UserMessage::new(content, model_id),
        }
    }
}

/// 用户消息（历史记录中使用）
///
/// 字段顺序对齐官方 kiro-cli：`content, userInputMessageContext, origin, modelId`。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserMessage {
    /// 消息内容
    pub content: String,
    /// 用户输入消息上下文
    #[serde(default, skip_serializing_if = "is_default_context")]
    pub user_input_message_context: UserInputMessageContext,
    /// 消息来源
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    /// 模型 ID（CLI 端点下可能留空）
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub model_id: String,
    /// 图片列表
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<KiroImage>,
}

fn is_default_context(ctx: &UserInputMessageContext) -> bool {
    ctx.tools.is_empty() && ctx.tool_results.is_empty() && ctx.env_state.is_none()
}

impl UserMessage {
    /// 创建新的用户消息
    pub fn new(content: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            user_input_message_context: UserInputMessageContext::default(),
            origin: Some("AI_EDITOR".to_string()),
            model_id: model_id.into(),
            images: Vec::new(),
        }
    }

    /// 设置图片
    pub fn with_images(mut self, images: Vec<KiroImage>) -> Self {
        self.images = images;
        self
    }

    /// 设置上下文
    pub fn with_context(mut self, context: UserInputMessageContext) -> Self {
        self.user_input_message_context = context;
        self
    }
}

/// 历史助手消息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryAssistantMessage {
    /// 助手响应消息
    pub assistant_response_message: AssistantMessage,
}

impl HistoryAssistantMessage {
    /// 创建新的历史助手消息
    pub fn new(content: impl Into<String>) -> Self {
        Self {
            assistant_response_message: AssistantMessage::new(content),
        }
    }
}

/// 助手消息（历史记录中使用）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssistantMessage {
    /// 响应内容
    pub content: String,
    /// 工具使用列表
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_uses: Option<Vec<ToolUseEntry>>,
}

impl AssistantMessage {
    /// 创建新的助手消息
    pub fn new(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            tool_uses: None,
        }
    }

    /// 设置工具使用
    pub fn with_tool_uses(mut self, tool_uses: Vec<ToolUseEntry>) -> Self {
        self.tool_uses = Some(tool_uses);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conversation_state_new() {
        let state = ConversationState::new("conv-123")
            .with_agent_task_type("vibe")
            .with_chat_trigger_type("MANUAL");

        assert_eq!(state.conversation_id, "conv-123");
        assert_eq!(state.agent_task_type, Some("vibe".to_string()));
        assert_eq!(state.chat_trigger_type, Some("MANUAL".to_string()));
    }

    #[test]
    fn test_user_input_message() {
        let msg = UserInputMessage::new("Hello", "claude-3-5-sonnet").with_origin("AI_EDITOR");

        assert_eq!(msg.content, "Hello");
        assert_eq!(msg.model_id, "claude-3-5-sonnet");
        assert_eq!(msg.origin, Some("AI_EDITOR".to_string()));
    }

    #[test]
    fn test_history_serialize() {
        let history = vec![
            Message::User(HistoryUserMessage::new("Hello", "claude-3-5-sonnet")),
            Message::Assistant(HistoryAssistantMessage::new("Hi! How can I help you?")),
        ];

        let json = serde_json::to_string(&history).unwrap();
        assert!(json.contains("userInputMessage"));
        assert!(json.contains("assistantResponseMessage"));
    }

    #[test]
    fn test_conversation_state_serialize() {
        let state = ConversationState::new("conv-123")
            .with_agent_task_type("vibe")
            .with_current_message(CurrentMessage::new(UserInputMessage::new(
                "Hello",
                "claude-3-5-sonnet",
            )));

        let json = serde_json::to_string(&state).unwrap();
        assert!(json.contains("\"conversationId\":\"conv-123\""));
        assert!(json.contains("\"agentTaskType\":\"vibe\""));
        assert!(json.contains("\"content\":\"Hello\""));
    }
}
