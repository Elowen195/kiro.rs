//! Anthropic API 路由配置

use axum::{
    Router,
    extract::DefaultBodyLimit,
    middleware,
    routing::{get, post},
};

use crate::kiro::provider::KiroProvider;

use super::{
    handlers::{count_tokens, get_models as get_cc_models, post_messages_cc},
    middleware::{AppState, auth_middleware, cors_layer},
    openai::{get_models as get_openai_models, post_chat_completions, post_plain_chat},
};

/// 请求体最大大小限制 (50MB)
const MAX_BODY_SIZE: usize = 50 * 1024 * 1024;

/// 创建 Anthropic API 路由
///
/// # 端点
/// - `GET /models` - 获取可用模型列表（OpenAI 兼容，无 /v1 别名）
/// - `POST /chat` - 纯聊天端点（不注入 CLI agent 与内建工具）
/// - `POST /chat/completions` - 纯聊天 OpenAI Chat Completions 兼容端点
/// - `GET /v1/models` - 获取可用模型列表（OpenAI 兼容）
/// - `POST /v1/chat/completions` - OpenAI Chat Completions
///
/// # 认证
/// 所有 `/v1` 路径需要 API Key 认证，支持：
/// - `x-api-key` header
/// - `Authorization: Bearer <token>` header
///
/// # 参数
/// - `api_key`: API 密钥，用于验证客户端请求
/// - `kiro_provider`: 可选的 KiroProvider，用于调用上游 API

/// 创建带有 KiroProvider 的 Anthropic API 路由
pub fn create_router_with_provider(
    api_key: impl Into<String>,
    kiro_provider: Option<KiroProvider>,
    extract_thinking: bool,
) -> Router {
    let mut state = AppState::new(api_key, extract_thinking);
    if let Some(provider) = kiro_provider {
        state = state.with_kiro_provider(provider);
    }

    // 需要认证的 /v1 路由
    let v1_routes = Router::new()
        .route("/models", get(get_openai_models))
        .route("/chat/completions", post(post_chat_completions))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    // 需要认证的 /cc/v1 路由（Anthropic / Claude Code 兼容端点）
    let cc_v1_routes = Router::new()
        .route("/models", get(get_cc_models))
        .route("/messages", post(post_messages_cc))
        .route("/messages/count_tokens", post(count_tokens))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    // 纯聊天路由：面向写作/审稿/JSON 生成，不注入 Kiro CLI agent 和内建工具。
    let plain_chat_routes = Router::new()
        .route("/models", get(get_openai_models))
        .route("/chat", post(post_plain_chat))
        .route("/chat/completions", post(post_plain_chat))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    Router::new()
        .merge(plain_chat_routes)
        .nest("/v1", v1_routes)
        .nest("/cc/v1", cc_v1_routes)
        .layer(cors_layer())
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
        .with_state(state)
}
