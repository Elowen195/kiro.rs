//! Kiro API Provider
//!
//! 核心组件，负责与 Kiro API 通信
//! 支持流式和非流式请求
//! 支持多凭据故障转移和重试
//! 支持按凭据级 endpoint 切换不同 Kiro API 端点

use reqwest::Client;
use reqwest::header::HeaderMap;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::{Mutex as StdMutex, OnceLock};
use std::time::Duration;
use tokio::time::sleep;

use crate::http_client::{ProxyConfig, build_client};
use crate::kiro::endpoint::cli::{CLI_ENDPOINT_NAME, CliEndpoint};
use crate::kiro::endpoint::{KiroEndpoint, RequestContext};
use crate::kiro::machine_id;
use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::token_manager::{MultiTokenManager, TransientAvoidanceExhaustedError};
use crate::model::config::TlsBackend;
use parking_lot::Mutex;

/// 每个凭据的最大重试次数
const MAX_RETRIES_PER_CREDENTIAL: usize = 3;

/// 总重试次数硬上限（避免无限重试）
const MAX_TOTAL_RETRIES: usize = 9;
const UPSTREAM_DEBUG_ENV: &str = "KIRO_UPSTREAM_DEBUG";
const UPSTREAM_DEBUG_FILE_ENV: &str = "KIRO_UPSTREAM_DEBUG_FILE";
const DEFAULT_UPSTREAM_DEBUG_FILE: &str = "kiro-upstream-debug.ndjson";

static UPSTREAM_DEBUG_PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
static UPSTREAM_DEBUG_FILE_LOCK: StdMutex<()> = StdMutex::new(());
static UPSTREAM_DEBUG_PATH_ANNOUNCED: OnceLock<()> = OnceLock::new();

/// Kiro API Provider
///
/// 核心组件，负责与 Kiro API 通信
/// 支持多凭据故障转移和重试机制
/// 按凭据 `endpoint` 字段选择 [`KiroEndpoint`] 实现
pub struct KiroProvider {
    token_manager: Arc<MultiTokenManager>,
    /// 全局代理配置（用于凭据无自定义代理时的回退）
    global_proxy: Option<ProxyConfig>,
    /// Client 缓存：key = effective proxy config, value = reqwest::Client
    /// 不同代理配置的凭据使用不同的 Client，共享相同代理的凭据复用 Client
    client_cache: Mutex<HashMap<Option<ProxyConfig>, Client>>,
    /// TLS 后端配置
    tls_backend: TlsBackend,
    /// 端点实现注册表（key: endpoint 名称）
    endpoints: HashMap<String, Arc<dyn KiroEndpoint>>,
    /// 默认端点名称（凭据未指定 endpoint 时使用）
    default_endpoint: String,
}

fn is_truthy_env(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn resolve_upstream_debug_path(
    enabled: Option<&str>,
    file: Option<&str>,
    cwd: Option<&Path>,
) -> Option<PathBuf> {
    let cwd = cwd.unwrap_or_else(|| Path::new("."));

    if let Some(file) = file.map(str::trim).filter(|value| !value.is_empty()) {
        let path = PathBuf::from(file);
        return Some(if path.is_absolute() {
            path
        } else {
            cwd.join(path)
        });
    }

    if enabled.is_some_and(is_truthy_env) {
        return Some(cwd.join(DEFAULT_UPSTREAM_DEBUG_FILE));
    }

    None
}

fn upstream_debug_path() -> Option<PathBuf> {
    UPSTREAM_DEBUG_PATH
        .get_or_init(|| {
            let cwd = env::current_dir().ok();
            resolve_upstream_debug_path(
                env::var(UPSTREAM_DEBUG_ENV).ok().as_deref(),
                env::var(UPSTREAM_DEBUG_FILE_ENV).ok().as_deref(),
                cwd.as_deref(),
            )
        })
        .clone()
}

fn upstream_debug_enabled() -> bool {
    upstream_debug_path().is_some()
}

fn sorted_credential_ids(ids: &HashSet<u64>) -> Vec<u64> {
    let mut sorted: Vec<u64> = ids.iter().copied().collect();
    sorted.sort_unstable();
    sorted
}

fn serialize_headers(headers: &HeaderMap) -> Value {
    let mut serialized = serde_json::Map::new();
    for (name, value) in headers {
        serialized.insert(
            name.as_str().to_string(),
            Value::String(value.to_str().unwrap_or("<non-utf8>").to_string()),
        );
    }
    Value::Object(serialized)
}

fn append_upstream_debug_record(record: Value) {
    let Some(path) = upstream_debug_path() else {
        return;
    };

    if UPSTREAM_DEBUG_PATH_ANNOUNCED.set(()).is_ok() {
        tracing::warn!(
            path = %path.display(),
            "已启用上游诊断落盘；失败请求将追加写入该文件"
        );
    }

    let _guard = match UPSTREAM_DEBUG_FILE_LOCK.lock() {
        Ok(guard) => guard,
        Err(_) => return,
    };

    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        if let Err(err) = fs::create_dir_all(parent) {
            tracing::warn!(
                path = %path.display(),
                error = %err,
                "创建上游诊断目录失败"
            );
            return;
        }
    }

    let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(file) => file,
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                error = %err,
                "打开上游诊断文件失败"
            );
            return;
        }
    };

    let line = match serde_json::to_string(&record) {
        Ok(line) => line,
        Err(err) => {
            tracing::warn!(error = %err, "序列化上游诊断记录失败");
            return;
        }
    };

    if let Err(err) = writeln!(file, "{}", line) {
        tracing::warn!(
            path = %path.display(),
            error = %err,
            "写入上游诊断文件失败"
        );
    }
}

struct UpstreamDebugAttempt<'a> {
    api_type: &'a str,
    attempt: usize,
    max_retries: usize,
    credential_id: u64,
    endpoint_name: &'a str,
    url: &'a str,
    is_stream: bool,
    model: Option<&'a str>,
    avoided_credentials: &'a HashSet<u64>,
    request_body: Option<&'a str>,
}

fn dump_upstream_send_error(attempt: &UpstreamDebugAttempt<'_>, error: &str) {
    append_upstream_debug_record(json!({
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "kind": "request_send_error",
        "api_type": attempt.api_type,
        "attempt": attempt.attempt,
        "max_retries": attempt.max_retries,
        "credential_id": attempt.credential_id,
        "endpoint": attempt.endpoint_name,
        "url": attempt.url,
        "is_stream": attempt.is_stream,
        "model": attempt.model,
        "avoided_credentials": sorted_credential_ids(attempt.avoided_credentials),
        "request_body_bytes": attempt.request_body.map(|body| body.len()),
        "request_body": attempt.request_body,
        "error": error,
    }));
}

fn dump_upstream_response_error(
    attempt: &UpstreamDebugAttempt<'_>,
    classification: &str,
    status: reqwest::StatusCode,
    headers: &HeaderMap,
    response_body: &str,
) {
    append_upstream_debug_record(json!({
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "kind": "response_error",
        "classification": classification,
        "api_type": attempt.api_type,
        "attempt": attempt.attempt,
        "max_retries": attempt.max_retries,
        "credential_id": attempt.credential_id,
        "endpoint": attempt.endpoint_name,
        "url": attempt.url,
        "is_stream": attempt.is_stream,
        "model": attempt.model,
        "avoided_credentials": sorted_credential_ids(attempt.avoided_credentials),
        "request_body_bytes": attempt.request_body.map(|body| body.len()),
        "request_body": attempt.request_body,
        "response_status": status.as_u16(),
        "response_headers": serialize_headers(headers),
        "response_body": response_body,
    }));
}

impl KiroProvider {
    /// 创建带代理配置和端点注册表的 KiroProvider 实例
    ///
    /// # Arguments
    /// * `token_manager` - 多凭据 Token 管理器
    /// * `proxy` - 全局代理配置
    /// * `endpoints` - 端点名 → 实现的注册表（至少包含 `default_endpoint` 对应条目）
    /// * `default_endpoint` - 凭据未显式指定 endpoint 时使用的名称
    pub fn with_proxy(
        token_manager: Arc<MultiTokenManager>,
        proxy: Option<ProxyConfig>,
        endpoints: HashMap<String, Arc<dyn KiroEndpoint>>,
        default_endpoint: String,
    ) -> Self {
        assert!(
            endpoints.contains_key(&default_endpoint),
            "默认端点 {} 未在 endpoints 注册表中",
            default_endpoint
        );
        let tls_backend = token_manager.config().tls_backend;
        // 预热：构建全局代理对应的 Client
        let initial_client =
            build_client(proxy.as_ref(), 720, tls_backend).expect("创建 HTTP 客户端失败");
        let mut cache = HashMap::new();
        cache.insert(proxy.clone(), initial_client);

        Self {
            token_manager,
            global_proxy: proxy,
            client_cache: Mutex::new(cache),
            tls_backend,
            endpoints,
            default_endpoint,
        }
    }

    /// 根据凭据的代理配置获取（或创建并缓存）对应的 reqwest::Client
    fn client_for(&self, credentials: &KiroCredentials) -> anyhow::Result<Client> {
        let effective = credentials.effective_proxy(self.global_proxy.as_ref());
        let mut cache = self.client_cache.lock();
        if let Some(client) = cache.get(&effective) {
            return Ok(client.clone());
        }
        let client = build_client(effective.as_ref(), 720, self.tls_backend)?;
        cache.insert(effective, client.clone());
        Ok(client)
    }

    /// 根据凭据选择 endpoint 实现
    fn endpoint_for(&self, credentials: &KiroCredentials) -> anyhow::Result<Arc<dyn KiroEndpoint>> {
        let name = credentials
            .endpoint
            .as_deref()
            .unwrap_or(&self.default_endpoint);
        self.endpoints
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("未知端点: {}", name))
    }

    /// 发送非流式 API 请求
    ///
    /// 支持多凭据故障转移（见 [`Self::call_api_with_retry`]）
    pub async fn call_api(&self, request_body: &str) -> anyhow::Result<reqwest::Response> {
        self.call_api_with_retry(request_body, false).await
    }

    /// 发送流式 API 请求
    pub async fn call_api_stream(&self, request_body: &str) -> anyhow::Result<reqwest::Response> {
        self.call_api_with_retry(request_body, true).await
    }

    /// 发送 MCP API 请求（WebSearch 等工具调用）
    pub async fn call_mcp(&self, request_body: &str) -> anyhow::Result<reqwest::Response> {
        self.call_mcp_with_retry(request_body).await
    }

    /// 调用 CLI 端点的 ListAvailableModels 接口
    ///
    /// 需要 `cli` 端点已注册（`main.rs` 里默认注册了）。
    /// 返回上游原始 JSON 响应（含 `models` 数组）。
    ///
    /// 注意：无论凭据 `endpoint` 字段是什么，这里都用 CliEndpoint 发起请求，
    /// 因为 IDE 端点没有公开的 list models API。
    pub async fn call_list_models(&self) -> anyhow::Result<serde_json::Value> {
        // 要求 CLI 端点已注册；因为 CliEndpoint 无状态，直接实例化使用即可
        if !self.endpoints.contains_key(CLI_ENDPOINT_NAME) {
            anyhow::bail!("CLI 端点未注册，无法调用 ListAvailableModels");
        }
        let cli = CliEndpoint::new();

        let ctx = self.token_manager.acquire_context(None).await?;
        let config = self.token_manager.config();
        let machine_id = machine_id::generate_from_credentials(&ctx.credentials, config);

        let rctx = RequestContext {
            credentials: &ctx.credentials,
            token: &ctx.token,
            machine_id: &machine_id,
            config,
        };

        let spec = cli.list_models_spec(&rctx);
        let base = self
            .client_for(&ctx.credentials)?
            .post(&spec.url)
            .body(spec.body.clone())
            .header("Connection", "close");
        let request = cli.decorate_list_models(base, &rctx);

        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();

        if !status.is_success() {
            anyhow::bail!("ListAvailableModels 调用失败: {} {}", status, body);
        }

        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| anyhow::anyhow!("解析 ListAvailableModels 响应失败: {}: {}", e, body))?;

        self.token_manager.report_success(ctx.id);
        Ok(json)
    }

    /// 内部方法：带重试逻辑的 MCP API 调用
    async fn call_mcp_with_retry(&self, request_body: &str) -> anyhow::Result<reqwest::Response> {
        let total_credentials = self.token_manager.total_count();
        let max_retries = (total_credentials * MAX_RETRIES_PER_CREDENTIAL).min(MAX_TOTAL_RETRIES);
        let mut last_error: Option<anyhow::Error> = None;
        let mut force_refreshed: HashSet<u64> = HashSet::new();

        for attempt in 0..max_retries {
            // MCP 调用（WebSearch 等工具）不涉及模型选择，无需按模型过滤凭据
            let ctx = match self.token_manager.acquire_context(None).await {
                Ok(c) => c,
                Err(e) => {
                    last_error = Some(e);
                    continue;
                }
            };

            let config = self.token_manager.config();
            let machine_id = machine_id::generate_from_credentials(&ctx.credentials, config);

            let endpoint = match self.endpoint_for(&ctx.credentials) {
                Ok(e) => e,
                Err(e) => {
                    last_error = Some(e);
                    // endpoint 解析失败：记为失败，换下一张凭据
                    self.token_manager.report_failure(ctx.id);
                    continue;
                }
            };

            let rctx = RequestContext {
                credentials: &ctx.credentials,
                token: &ctx.token,
                machine_id: &machine_id,
                config,
            };

            let url = endpoint.mcp_url(&rctx);
            let body = endpoint.transform_mcp_body(request_body, &rctx);

            let base = self
                .client_for(&ctx.credentials)?
                .post(&url)
                .body(body)
                .header("content-type", "application/json")
                .header("Connection", "close");
            let request = endpoint.decorate_mcp(base, &rctx);

            let response = match request.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::warn!(
                        "MCP 请求发送失败（尝试 {}/{}）: {}",
                        attempt + 1,
                        max_retries,
                        e
                    );
                    last_error = Some(e.into());
                    if attempt + 1 < max_retries {
                        sleep(Self::retry_delay(attempt)).await;
                    }
                    continue;
                }
            };

            let status = response.status();

            // 成功响应
            if status.is_success() {
                self.token_manager.report_success(ctx.id);
                return Ok(response);
            }

            // 失败响应
            let body = response.text().await.unwrap_or_default();

            // 402 额度用尽
            if status.as_u16() == 402 && endpoint.is_monthly_request_limit(&body) {
                let has_available = self.token_manager.report_quota_exhausted(ctx.id);
                if !has_available {
                    anyhow::bail!("MCP 请求失败（所有凭据已用尽）: {} {}", status, body);
                }
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
                continue;
            }

            // 400 Bad Request
            if status.as_u16() == 400 {
                anyhow::bail!("MCP 请求失败: {} {}", status, body);
            }

            // 401/403 凭据问题
            if matches!(status.as_u16(), 401 | 403) {
                // token 被上游失效：先尝试 force-refresh，每凭据仅一次机会
                if endpoint.is_bearer_token_invalid(&body) && !force_refreshed.contains(&ctx.id) {
                    force_refreshed.insert(ctx.id);
                    tracing::info!("凭据 #{} token 疑似被上游失效，尝试强制刷新", ctx.id);
                    if self
                        .token_manager
                        .force_refresh_token_for(ctx.id)
                        .await
                        .is_ok()
                    {
                        tracing::info!("凭据 #{} token 强制刷新成功，重试请求", ctx.id);
                        continue;
                    }
                    tracing::warn!("凭据 #{} token 强制刷新失败，计入失败", ctx.id);
                }

                let has_available = self.token_manager.report_failure(ctx.id);
                if !has_available {
                    anyhow::bail!("MCP 请求失败（所有凭据已用尽）: {} {}", status, body);
                }
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
                continue;
            }

            // 瞬态错误
            if matches!(status.as_u16(), 408 | 429) || status.is_server_error() {
                tracing::warn!(
                    "MCP 请求失败（上游瞬态错误，尝试 {}/{}）: {} {}",
                    attempt + 1,
                    max_retries,
                    status,
                    body
                );
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
                if attempt + 1 < max_retries {
                    sleep(Self::retry_delay(attempt)).await;
                }
                continue;
            }

            // 其他 4xx
            if status.is_client_error() {
                anyhow::bail!("MCP 请求失败: {} {}", status, body);
            }

            // 兜底
            last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
            if attempt + 1 < max_retries {
                sleep(Self::retry_delay(attempt)).await;
            }
        }

        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!("MCP 请求失败：已达到最大重试次数（{}次）", max_retries)
        }))
    }

    /// 内部方法：带重试逻辑的 API 调用
    ///
    /// 重试策略：
    /// - 每个凭据最多重试 MAX_RETRIES_PER_CREDENTIAL 次
    /// - 总重试次数 = min(凭据数量 × 每凭据重试次数, MAX_TOTAL_RETRIES)
    /// - 硬上限 9 次，避免无限重试
    async fn call_api_with_retry(
        &self,
        request_body: &str,
        is_stream: bool,
    ) -> anyhow::Result<reqwest::Response> {
        let total_credentials = self.token_manager.total_count();
        let max_retries = (total_credentials * MAX_RETRIES_PER_CREDENTIAL).min(MAX_TOTAL_RETRIES);
        let mut last_error: Option<anyhow::Error> = None;
        let mut force_refreshed: HashSet<u64> = HashSet::new();
        let mut transient_avoided_ids: HashSet<u64> = HashSet::new();
        let api_type = if is_stream { "流式" } else { "非流式" };

        // 尝试从请求体中提取模型信息
        let model = Self::extract_model_from_request(request_body);

        for attempt in 0..max_retries {
            // 获取调用上下文（绑定 index、credentials、token）
            let ctx = match if transient_avoided_ids.is_empty() {
                self.token_manager.acquire_context(model.as_deref()).await
            } else {
                self.token_manager
                    .acquire_context_excluding(model.as_deref(), &transient_avoided_ids)
                    .await
            } {
                Ok(c) => c,
                Err(e) => {
                    if e.downcast_ref::<TransientAvoidanceExhaustedError>()
                        .is_some()
                    {
                        tracing::info!(
                            avoided_credentials = ?transient_avoided_ids,
                            "本轮瞬态错误避让已覆盖所有候选凭据，清空避让列表后继续重试"
                        );
                        transient_avoided_ids.clear();
                        last_error = Some(e);
                        if attempt + 1 < max_retries {
                            sleep(Self::retry_delay(attempt)).await;
                        }
                        continue;
                    }
                    last_error = Some(e);
                    continue;
                }
            };

            let config = self.token_manager.config();
            let machine_id = machine_id::generate_from_credentials(&ctx.credentials, config);

            let endpoint = match self.endpoint_for(&ctx.credentials) {
                Ok(e) => e,
                Err(e) => {
                    last_error = Some(e);
                    self.token_manager.report_failure(ctx.id);
                    continue;
                }
            };

            let rctx = RequestContext {
                credentials: &ctx.credentials,
                token: &ctx.token,
                machine_id: &machine_id,
                config,
            };

            let url = endpoint.api_url(&rctx);
            let body = endpoint.transform_api_body(request_body, &rctx);
            let debug_request_body = upstream_debug_enabled().then(|| body.clone());
            tracing::debug!(
                endpoint = endpoint.name(),
                credential_id = ctx.id,
                is_stream = is_stream,
                url = %url,
                "发送 Kiro API 请求"
            );

            let base = self
                .client_for(&ctx.credentials)?
                .post(&url)
                .body(body)
                .header("Connection", "close");
            let request = endpoint.decorate_api(base, &rctx);

            let response = match request.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    dump_upstream_send_error(
                        &UpstreamDebugAttempt {
                            api_type,
                            attempt: attempt + 1,
                            max_retries,
                            credential_id: ctx.id,
                            endpoint_name: endpoint.name(),
                            url: &url,
                            is_stream,
                            model: model.as_deref(),
                            avoided_credentials: &transient_avoided_ids,
                            request_body: debug_request_body.as_deref(),
                        },
                        &e.to_string(),
                    );
                    tracing::warn!(
                        credential_id = ctx.id,
                        "API 请求发送失败（尝试 {}/{}）: {}",
                        attempt + 1,
                        max_retries,
                        e
                    );
                    // 网络错误通常是上游/链路瞬态问题，不应导致"禁用凭据"或"切换凭据"
                    // （否则一段时间网络抖动会把所有凭据都误禁用，需要重启才能恢复）
                    last_error = Some(e.into());
                    if attempt + 1 < max_retries {
                        sleep(Self::retry_delay(attempt)).await;
                    }
                    continue;
                }
            };

            let status = response.status();
            let response_headers = response.headers().clone();

            // 成功响应
            if status.is_success() {
                self.token_manager.report_success(ctx.id);
                return Ok(response);
            }

            // 失败响应：读取 body 用于日志/错误信息
            let body = response.text().await.unwrap_or_default();

            // 402 Payment Required 且额度用尽：禁用凭据并故障转移
            if status.as_u16() == 402 && endpoint.is_monthly_request_limit(&body) {
                dump_upstream_response_error(
                    &UpstreamDebugAttempt {
                        api_type,
                        attempt: attempt + 1,
                        max_retries,
                        credential_id: ctx.id,
                        endpoint_name: endpoint.name(),
                        url: &url,
                        is_stream,
                        model: model.as_deref(),
                        avoided_credentials: &transient_avoided_ids,
                        request_body: debug_request_body.as_deref(),
                    },
                    "quota_exhausted",
                    status,
                    &response_headers,
                    &body,
                );
                tracing::warn!(
                    "API 请求失败（额度已用尽，禁用凭据并切换，尝试 {}/{}）: {} {}",
                    attempt + 1,
                    max_retries,
                    status,
                    body
                );

                let has_available = self.token_manager.report_quota_exhausted(ctx.id);
                if !has_available {
                    anyhow::bail!(
                        "{} API 请求失败（所有凭据已用尽）: {} {}",
                        api_type,
                        status,
                        body
                    );
                }

                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {} {}",
                    api_type,
                    status,
                    body
                ));
                continue;
            }

            // 400 Bad Request - 请求问题，重试/切换凭据无意义
            if status.as_u16() == 400 {
                dump_upstream_response_error(
                    &UpstreamDebugAttempt {
                        api_type,
                        attempt: attempt + 1,
                        max_retries,
                        credential_id: ctx.id,
                        endpoint_name: endpoint.name(),
                        url: &url,
                        is_stream,
                        model: model.as_deref(),
                        avoided_credentials: &transient_avoided_ids,
                        request_body: debug_request_body.as_deref(),
                    },
                    "bad_request",
                    status,
                    &response_headers,
                    &body,
                );
                anyhow::bail!("{} API 请求失败: {} {}", api_type, status, body);
            }

            // 401/403 - 更可能是凭据/权限问题：计入失败并允许故障转移
            if matches!(status.as_u16(), 401 | 403) {
                dump_upstream_response_error(
                    &UpstreamDebugAttempt {
                        api_type,
                        attempt: attempt + 1,
                        max_retries,
                        credential_id: ctx.id,
                        endpoint_name: endpoint.name(),
                        url: &url,
                        is_stream,
                        model: model.as_deref(),
                        avoided_credentials: &transient_avoided_ids,
                        request_body: debug_request_body.as_deref(),
                    },
                    "credential_error",
                    status,
                    &response_headers,
                    &body,
                );
                tracing::warn!(
                    credential_id = ctx.id,
                    "API 请求失败（可能为凭据错误，尝试 {}/{}）: {} {}",
                    attempt + 1,
                    max_retries,
                    status,
                    body
                );

                // token 被上游失效：先尝试 force-refresh，每凭据仅一次机会
                if endpoint.is_bearer_token_invalid(&body) && !force_refreshed.contains(&ctx.id) {
                    force_refreshed.insert(ctx.id);
                    tracing::info!("凭据 #{} token 疑似被上游失效，尝试强制刷新", ctx.id);
                    if self
                        .token_manager
                        .force_refresh_token_for(ctx.id)
                        .await
                        .is_ok()
                    {
                        tracing::info!("凭据 #{} token 强制刷新成功，重试请求", ctx.id);
                        continue;
                    }
                    tracing::warn!("凭据 #{} token 强制刷新失败，计入失败", ctx.id);
                }

                let has_available = self.token_manager.report_failure(ctx.id);
                if !has_available {
                    anyhow::bail!(
                        "{} API 请求失败（所有凭据已用尽）: {} {}",
                        api_type,
                        status,
                        body
                    );
                }

                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {} {}",
                    api_type,
                    status,
                    body
                ));
                continue;
            }

            // 429/408/5xx - 瞬态上游错误：不累计失败、不禁用，
            // 但在当前请求的后续重试中临时避让这张凭据，减少连续撞同一卡的概率
            if matches!(status.as_u16(), 408 | 429) || status.is_server_error() {
                transient_avoided_ids.insert(ctx.id);
                dump_upstream_response_error(
                    &UpstreamDebugAttempt {
                        api_type,
                        attempt: attempt + 1,
                        max_retries,
                        credential_id: ctx.id,
                        endpoint_name: endpoint.name(),
                        url: &url,
                        is_stream,
                        model: model.as_deref(),
                        avoided_credentials: &transient_avoided_ids,
                        request_body: debug_request_body.as_deref(),
                    },
                    "transient_upstream_error",
                    status,
                    &response_headers,
                    &body,
                );
                tracing::warn!(
                    credential_id = ctx.id,
                    avoided_credentials = ?sorted_credential_ids(&transient_avoided_ids),
                    "API 请求失败（上游瞬态错误，尝试 {}/{}）: {} {}",
                    attempt + 1,
                    max_retries,
                    status,
                    body
                );
                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {} {}",
                    api_type,
                    status,
                    body
                ));
                if attempt + 1 < max_retries {
                    sleep(Self::retry_delay(attempt)).await;
                }
                continue;
            }

            // 其他 4xx - 通常为请求/配置问题：直接返回，不计入凭据失败
            if status.is_client_error() {
                dump_upstream_response_error(
                    &UpstreamDebugAttempt {
                        api_type,
                        attempt: attempt + 1,
                        max_retries,
                        credential_id: ctx.id,
                        endpoint_name: endpoint.name(),
                        url: &url,
                        is_stream,
                        model: model.as_deref(),
                        avoided_credentials: &transient_avoided_ids,
                        request_body: debug_request_body.as_deref(),
                    },
                    "client_error",
                    status,
                    &response_headers,
                    &body,
                );
                anyhow::bail!("{} API 请求失败: {} {}", api_type, status, body);
            }

            // 兜底：当作可重试的瞬态错误处理（不切换凭据）
            dump_upstream_response_error(
                &UpstreamDebugAttempt {
                    api_type,
                    attempt: attempt + 1,
                    max_retries,
                    credential_id: ctx.id,
                    endpoint_name: endpoint.name(),
                    url: &url,
                    is_stream,
                    model: model.as_deref(),
                    avoided_credentials: &transient_avoided_ids,
                    request_body: debug_request_body.as_deref(),
                },
                "unknown_error",
                status,
                &response_headers,
                &body,
            );
            tracing::warn!(
                "API 请求失败（未知错误，尝试 {}/{}）: {} {}",
                attempt + 1,
                max_retries,
                status,
                body
            );
            last_error = Some(anyhow::anyhow!(
                "{} API 请求失败: {} {}",
                api_type,
                status,
                body
            ));
            if attempt + 1 < max_retries {
                sleep(Self::retry_delay(attempt)).await;
            }
        }

        // 所有重试都失败
        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!(
                "{} API 请求失败：已达到最大重试次数（{}次）",
                api_type,
                max_retries
            )
        }))
    }

    /// 从请求体中提取模型信息
    ///
    /// 尝试解析 JSON 请求体，提取 conversationState.currentMessage.userInputMessage.modelId
    fn extract_model_from_request(request_body: &str) -> Option<String> {
        use serde_json::Value;

        let json: Value = serde_json::from_str(request_body).ok()?;

        json.get("conversationState")?
            .get("currentMessage")?
            .get("userInputMessage")?
            .get("modelId")?
            .as_str()
            .map(|s| s.to_string())
    }

    fn retry_delay(attempt: usize) -> Duration {
        // 指数退避 + 少量抖动，避免上游抖动时放大故障
        const BASE_MS: u64 = 200;
        const MAX_MS: u64 = 2_000;
        let exp = BASE_MS.saturating_mul(2u64.saturating_pow(attempt.min(6) as u32));
        let backoff = exp.min(MAX_MS);
        let jitter_max = (backoff / 4).max(1);
        let jitter = fastrand::u64(0..=jitter_max);
        Duration::from_millis(backoff.saturating_add(jitter))
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_UPSTREAM_DEBUG_FILE, resolve_upstream_debug_path};
    use std::path::Path;

    #[test]
    fn test_resolve_upstream_debug_path_disabled() {
        let path = resolve_upstream_debug_path(Some("0"), None, Some(Path::new("/workspace")));
        assert!(path.is_none());
    }

    #[test]
    fn test_resolve_upstream_debug_path_enabled_with_default_file() {
        let path = resolve_upstream_debug_path(Some("true"), None, Some(Path::new("/workspace")))
            .expect("debug path should exist");
        assert_eq!(
            path,
            Path::new("/workspace").join(DEFAULT_UPSTREAM_DEBUG_FILE)
        );
    }

    #[test]
    fn test_resolve_upstream_debug_path_prefers_explicit_file() {
        let path = resolve_upstream_debug_path(
            Some("0"),
            Some("logs/upstream.ndjson"),
            Some(Path::new("/workspace")),
        )
        .expect("explicit file should enable debug dump");
        assert_eq!(path, Path::new("/workspace/logs/upstream.ndjson"));
    }
}
