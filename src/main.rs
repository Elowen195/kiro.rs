mod admin;
mod admin_ui;
mod anthropic;
mod common;
mod http_client;
mod kiro;
mod model;
pub mod token;

use std::collections::HashMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use kiro::endpoint::{CliEndpoint, IdeEndpoint, KiroEndpoint};
use kiro::model::credentials::{CredentialsConfig, KiroCredentials};
use kiro::provider::KiroProvider;
use kiro::token_manager::MultiTokenManager;
use model::arg::Args;
use model::config::Config;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const DEBUG_LOG_ENV: &str = "KIRO_DEBUG_LOG";
const DEBUG_LOG_FILE_ENV: &str = "KIRO_DEBUG_LOG_FILE";
const DEFAULT_DEBUG_LOG_FILE: &str = "kiro-debug.log";

fn is_truthy_env(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn resolve_debug_log_path(
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
        return Some(cwd.join(DEFAULT_DEBUG_LOG_FILE));
    }

    None
}

#[derive(Clone)]
struct FileMakeWriter {
    path: PathBuf,
}

struct FileLogWriter {
    file: std::fs::File,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for FileMakeWriter {
    type Writer = FileLogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .unwrap_or_else(|err| {
                panic!("打开调试日志文件失败: {} ({})", self.path.display(), err)
            });
        FileLogWriter { file }
    }
}

impl Write for FileLogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn init_tracing() {
    let debug_log_path = {
        let cwd = env::current_dir().ok();
        resolve_debug_log_path(
            env::var(DEBUG_LOG_ENV).ok().as_deref(),
            env::var(DEBUG_LOG_FILE_ENV).ok().as_deref(),
            cwd.as_deref(),
        )
    };

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        let default = if debug_log_path.is_some() {
            "debug"
        } else {
            "info"
        };
        tracing_subscriber::EnvFilter::new(default)
    });

    if let Some(path) = debug_log_path {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).unwrap_or_else(|err| {
                panic!("创建调试日志目录失败: {} ({})", parent.display(), err)
            });
        }

        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(FileMakeWriter { path: path.clone() }),
            )
            .init();

        tracing::warn!(
            path = %path.display(),
            "已启用全局调试日志文件输出；所有 tracing 日志将同时写入该文件"
        );
        return;
    }

    tracing_subscriber::fmt().with_env_filter(env_filter).init();
}

#[tokio::main]
async fn main() {
    // 解析命令行参数
    let args = Args::parse();

    // 初始化日志
    init_tracing();

    // 加载配置
    let config_path = args
        .config
        .unwrap_or_else(|| Config::default_config_path().to_string());
    let config = Config::load(&config_path).unwrap_or_else(|e| {
        tracing::error!("加载配置失败: {}", e);
        std::process::exit(1);
    });

    // 加载凭证（支持单对象或数组格式）
    let credentials_path = args
        .credentials
        .unwrap_or_else(|| KiroCredentials::default_credentials_path().to_string());
    let credentials_config = CredentialsConfig::load(&credentials_path).unwrap_or_else(|e| {
        tracing::error!("加载凭证失败: {}", e);
        std::process::exit(1);
    });

    // 判断是否为多凭据格式（用于刷新后回写）
    let is_multiple_format = credentials_config.is_multiple();

    // 转换为按优先级排序的凭据列表
    let mut credentials_list = credentials_config.into_sorted_credentials();

    // 检查 KIRO_API_KEY 环境变量，自动创建 API Key 凭据
    if let Ok(kiro_api_key) = std::env::var("KIRO_API_KEY") {
        if kiro_api_key.is_empty() {
            tracing::warn!("KIRO_API_KEY 环境变量已设置但为空，视为未配置");
        } else {
            tracing::info!("检测到 KIRO_API_KEY 环境变量，添加 API Key 凭据（最高优先级）");
            let api_key_cred = KiroCredentials {
                kiro_api_key: Some(kiro_api_key),
                auth_method: Some("api_key".to_string()),
                priority: 0,
                ..Default::default()
            };
            credentials_list.insert(0, api_key_cred);
        }
    }

    tracing::info!("已加载 {} 个凭据配置", credentials_list.len());

    // 获取第一个凭据用于日志显示
    let first_credentials = credentials_list.first().cloned().unwrap_or_default();
    tracing::debug!("主凭证: {:?}", first_credentials);

    // 获取 API Key
    let api_key = config.api_key.clone().unwrap_or_else(|| {
        tracing::error!("配置文件中未设置 apiKey");
        std::process::exit(1);
    });

    // 构建代理配置
    let proxy_config = config.proxy_url.as_ref().map(|url| {
        let mut proxy = http_client::ProxyConfig::new(url);
        if let (Some(username), Some(password)) = (&config.proxy_username, &config.proxy_password) {
            proxy = proxy.with_auth(username, password);
        }
        proxy
    });

    if proxy_config.is_some() {
        tracing::info!("已配置 HTTP 代理: {}", config.proxy_url.as_ref().unwrap());
    }

    // 构建端点注册表
    let mut endpoints: HashMap<String, Arc<dyn KiroEndpoint>> = HashMap::new();
    {
        let ide = IdeEndpoint::new();
        endpoints.insert(ide.name().to_string(), Arc::new(ide));
        let cli = CliEndpoint::new();
        endpoints.insert(cli.name().to_string(), Arc::new(cli));
    }

    // 校验默认端点存在
    if !endpoints.contains_key(&config.default_endpoint) {
        tracing::error!("默认端点 \"{}\" 未注册", config.default_endpoint);
        std::process::exit(1);
    }

    // 校验所有凭据声明的端点都已注册
    for cred in &credentials_list {
        let name = cred.endpoint.as_deref().unwrap_or(&config.default_endpoint);
        if !endpoints.contains_key(name) {
            tracing::error!(
                "凭据 id={:?} 指定了未知端点 \"{}\"（已注册: {:?}）",
                cred.id,
                name,
                endpoints.keys().collect::<Vec<_>>()
            );
            std::process::exit(1);
        }
    }

    let endpoint_names: Vec<String> = endpoints.keys().cloned().collect();

    // 创建 MultiTokenManager 和 KiroProvider
    let token_manager = MultiTokenManager::new(
        config.clone(),
        credentials_list,
        proxy_config.clone(),
        Some(credentials_path.into()),
        is_multiple_format,
    )
    .unwrap_or_else(|e| {
        tracing::error!("创建 Token 管理器失败: {}", e);
        std::process::exit(1);
    });
    let token_manager = Arc::new(token_manager);
    let kiro_provider = KiroProvider::with_proxy(
        token_manager.clone(),
        proxy_config.clone(),
        endpoints,
        config.default_endpoint.clone(),
    );

    // 初始化 count_tokens 配置
    token::init_config(token::CountTokensConfig {
        api_url: config.count_tokens_api_url.clone(),
        api_key: config.count_tokens_api_key.clone(),
        auth_type: config.count_tokens_auth_type.clone(),
        proxy: proxy_config,
        tls_backend: config.tls_backend,
    });

    // 构建 Anthropic API 路由（profile_arn 由 provider 层根据实际凭据动态注入）
    let anthropic_app = anthropic::create_router_with_provider(
        &api_key,
        Some(kiro_provider),
        config.extract_thinking,
    );

    // 构建 Admin API 路由（如果配置了非空的 admin_api_key）
    // 安全检查：空字符串被视为未配置，防止空 key 绕过认证
    let admin_key_valid = config
        .admin_api_key
        .as_ref()
        .map(|k| !k.trim().is_empty())
        .unwrap_or(false);

    let app = if let Some(admin_key) = &config.admin_api_key {
        if admin_key.trim().is_empty() {
            tracing::warn!("admin_api_key 配置为空，Admin API 未启用");
            anthropic_app
        } else {
            let admin_service =
                admin::AdminService::new(token_manager.clone(), endpoint_names.clone());
            let admin_state = admin::AdminState::new(admin_key, admin_service);
            let admin_app = admin::create_admin_router(admin_state);

            // 创建 Admin UI 路由
            let admin_ui_app = admin_ui::create_admin_ui_router();

            tracing::info!("Admin API 已启用");
            tracing::info!("Admin UI 已启用: /admin");
            anthropic_app
                .nest("/api/admin", admin_app)
                .nest("/admin", admin_ui_app)
        }
    } else {
        anthropic_app
    };

    // 启动服务器
    let addr = format!("{}:{}", config.host, config.port);
    tracing::info!("启动 API 端点: {}", addr);
    tracing::info!("API Key: {}***", &api_key[..(api_key.len() / 2)]);
    tracing::info!("可用 API:");
    tracing::info!("  GET  /v1/models");
    tracing::info!("  POST /v1/chat/completions");
    tracing::info!("  GET  /cc/v1/models");
    tracing::info!("  POST /cc/v1/messages");
    tracing::info!("  POST /cc/v1/messages/count_tokens");
    if admin_key_valid {
        tracing::info!("Admin API:");
        tracing::info!("  GET  /api/admin/credentials");
        tracing::info!("  POST /api/admin/credentials/:index/disabled");
        tracing::info!("  POST /api/admin/credentials/:index/priority");
        tracing::info!("  POST /api/admin/credentials/:index/reset");
        tracing::info!("  GET  /api/admin/credentials/:index/balance");
        tracing::info!("Admin UI:");
        tracing::info!("  GET  /admin");
    }

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_DEBUG_LOG_FILE, resolve_debug_log_path};
    use std::path::Path;

    #[test]
    fn test_resolve_debug_log_path_disabled() {
        let path = resolve_debug_log_path(Some("0"), None, Some(Path::new("/workspace")));
        assert!(path.is_none());
    }

    #[test]
    fn test_resolve_debug_log_path_uses_default_file() {
        let path = resolve_debug_log_path(Some("true"), None, Some(Path::new("/workspace")))
            .expect("debug path should exist");
        assert_eq!(path, Path::new("/workspace").join(DEFAULT_DEBUG_LOG_FILE));
    }

    #[test]
    fn test_resolve_debug_log_path_prefers_explicit_file() {
        let path = resolve_debug_log_path(
            Some("0"),
            Some("logs/kiro-debug.log"),
            Some(Path::new("/workspace")),
        )
        .expect("explicit path should enable debug log");
        assert_eq!(path, Path::new("/workspace/logs/kiro-debug.log"));
    }
}
