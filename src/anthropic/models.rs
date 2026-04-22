use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use serde_json::Value;

use super::{
    middleware::AppState,
    types::{ErrorResponse, Model, ModelsResponse},
};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct UpstreamModel {
    pub id: String,
    pub display_name: String,
    pub owned_by: String,
    pub supported_input_types: Vec<String>,
    pub max_input_tokens: i32,
    pub max_output_tokens: i32,
    pub rate_multiplier: Option<f64>,
    pub rate_unit: Option<String>,
}

pub(crate) async fn fetch_upstream_models(state: &AppState) -> Result<Vec<UpstreamModel>, Response> {
    let provider = match state.kiro_provider.as_ref() {
        Some(provider) => provider,
        None => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::new(
                    "service_unavailable",
                    "Kiro API provider not configured",
                )),
            )
                .into_response());
        }
    };

    let upstream = match provider.call_list_models().await {
        Ok(upstream) => upstream,
        Err(err) => {
            tracing::warn!("获取上游模型列表失败: {}", err);
            return Err((
                StatusCode::BAD_GATEWAY,
                Json(ErrorResponse::new(
                    "api_error",
                    format!("获取上游模型列表失败: {}", err),
                )),
            )
                .into_response());
        }
    };

    let models = parse_upstream_models(&upstream);
    if models.is_empty() {
        tracing::warn!("上游 ListAvailableModels 返回空列表");
        return Err((
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse::new(
                "api_error",
                "上游模型列表为空或格式不正确",
            )),
        )
            .into_response());
    }

    Ok(models)
}

pub(crate) fn parse_upstream_models(upstream: &Value) -> Vec<UpstreamModel> {
    let Some(models) = upstream.get("models").and_then(Value::as_array) else {
        return Vec::new();
    };

    models
        .iter()
        .filter_map(|model| {
            let id = model.get("modelId").and_then(Value::as_str)?.trim();
            if id.is_empty() {
                return None;
            }

            let display_name = model
                .get("modelName")
                .and_then(Value::as_str)
                .unwrap_or(id)
                .to_string();
            let token_limits = model.get("tokenLimits");
            let max_input_tokens = token_limits
                .and_then(|value| value.get("maxInputTokens"))
                .and_then(Value::as_i64)
                .unwrap_or(200_000) as i32;
            let max_output_tokens = token_limits
                .and_then(|value| value.get("maxOutputTokens"))
                .and_then(Value::as_i64)
                .unwrap_or(64_000) as i32;
            let supported_input_types = model
                .get("supportedInputTypes")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            Some(UpstreamModel {
                id: id.to_string(),
                display_name,
                owned_by: infer_owned_by(id).to_string(),
                supported_input_types,
                max_input_tokens,
                max_output_tokens,
                rate_multiplier: model.get("rateMultiplier").and_then(Value::as_f64),
                rate_unit: model
                    .get("rateUnit")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
            })
        })
        .collect()
}

pub(crate) fn to_anthropic_models(models: &[UpstreamModel]) -> ModelsResponse {
    ModelsResponse {
        object: "list".to_string(),
        data: models
            .iter()
            .map(|model| Model {
                id: model.id.clone(),
                object: "model".to_string(),
                created: 0,
                owned_by: model.owned_by.clone(),
                display_name: model.display_name.clone(),
                model_type: "chat".to_string(),
                max_tokens: model.max_input_tokens,
            })
            .collect(),
    }
}

fn infer_owned_by(id: &str) -> &'static str {
    let id = id.to_ascii_lowercase();
    if id.contains("claude") {
        "anthropic"
    } else if id.contains("glm") {
        "zhipu"
    } else if id.contains("qwen") {
        "alibaba"
    } else if id.contains("deepseek") {
        "deepseek"
    } else if id.contains("minimax") {
        "minimax"
    } else {
        "amazon"
    }
}

#[cfg(test)]
mod tests {
    use super::parse_upstream_models;
    use serde_json::json;

    #[test]
    fn parse_upstream_models_keeps_dynamic_models() {
        let upstream = json!({
            "models": [
                {
                    "modelId": "auto",
                    "modelName": "auto",
                    "supportedInputTypes": ["TEXT", "IMAGE"],
                    "tokenLimits": {
                        "maxInputTokens": 1000000,
                        "maxOutputTokens": 64000
                    }
                },
                {
                    "modelId": "glm-5",
                    "modelName": "glm-5",
                    "supportedInputTypes": ["TEXT"],
                    "tokenLimits": {
                        "maxInputTokens": 200000,
                        "maxOutputTokens": 64000
                    },
                    "rateMultiplier": 0.5,
                    "rateUnit": "Credit"
                }
            ]
        });

        let models = parse_upstream_models(&upstream);
        assert_eq!(models.len(), 2);
        assert_eq!(models[0].id, "auto");
        assert_eq!(models[0].owned_by, "amazon");
        assert_eq!(models[1].id, "glm-5");
        assert_eq!(models[1].owned_by, "zhipu");
        assert_eq!(models[1].max_output_tokens, 64_000);
        assert_eq!(models[1].supported_input_types, vec!["TEXT".to_string()]);
    }
}
