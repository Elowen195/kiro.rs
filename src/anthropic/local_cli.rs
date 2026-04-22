use std::{process::Stdio, time::Duration};

use anyhow::{Context, bail};
use tokio::{io::AsyncWriteExt, process::Command, time::timeout};

use super::types::MessagesRequest;

const DEFAULT_CLI_BIN: &str = "kiro-cli";
const DEFAULT_TIMEOUT_SECS: u64 = 300;

pub fn supports_model(model: &str) -> bool {
    model.trim().eq_ignore_ascii_case("glm-5")
}

pub async fn complete(request: &MessagesRequest) -> anyhow::Result<String> {
    let cli_bin = std::env::var("KIRO_CLI_BIN").unwrap_or_else(|_| DEFAULT_CLI_BIN.to_string());
    let timeout_secs = std::env::var("KIRO_CLI_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_TIMEOUT_SECS);

    let mut command = Command::new(&cli_bin);
    command
        .arg("--classic")
        .arg("chat")
        .arg("--non-interactive")
        .arg("--model")
        .arg(request.model.trim())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    if let Ok(agent) = std::env::var("KIRO_CLI_AGENT") {
        let agent = agent.trim();
        if !agent.is_empty() {
            command.arg("--agent").arg(agent);
        }
    }

    if std::env::var("KIRO_CLI_TRUST_ALL_TOOLS")
        .ok()
        .as_deref()
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
    {
        command.arg("--trust-all-tools");
    }

    let mut child = command
        .spawn()
        .with_context(|| format!("启动本地 kiro-cli 失败: {}", cli_bin))?;

    let prompt = render_prompt(request);
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(prompt.as_bytes())
            .await
            .context("写入本地 kiro-cli 提示词失败")?;
        stdin
            .write_all(b"\n")
            .await
            .context("写入本地 kiro-cli 换行失败")?;
    }

    let output = timeout(Duration::from_secs(timeout_secs), child.wait_with_output())
        .await
        .with_context(|| format!("等待本地 kiro-cli 响应超时（{}s）", timeout_secs))?
        .context("等待本地 kiro-cli 输出失败")?;

    let stdout = strip_control_sequences(&String::from_utf8_lossy(&output.stdout));
    let stderr = strip_control_sequences(&String::from_utf8_lossy(&output.stderr));

    if !output.status.success() {
        let detail = first_non_empty(&[&stderr, &stdout]).unwrap_or("unknown error");
        bail!("本地 kiro-cli 执行失败: {}", detail);
    }

    let reply = extract_reply_text(&stdout);
    if reply.is_empty() {
        let detail = first_non_empty(&[&stdout, &stderr]).unwrap_or("empty output");
        bail!("本地 kiro-cli 未返回可解析内容: {}", detail);
    }

    Ok(reply)
}

pub(crate) fn render_prompt(request: &MessagesRequest) -> String {
    let mut prompt = String::from(
        "You are the local Kiro CLI fallback for an Anthropic-compatible bridge.\n\
Respond to the latest user request using the conversation transcript below.\n\
Use your native Kiro CLI tools when needed.\n\
Return only the assistant reply.\n",
    );

    if let Some(system) = &request.system {
        let system_text = system
            .iter()
            .map(|message| message.text.trim())
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n\n");
        if !system_text.is_empty() {
            prompt.push_str("\n[System Instructions]\n");
            prompt.push_str(&system_text);
            prompt.push('\n');
        }
    }

    if request
        .tools
        .as_ref()
        .is_some_and(|tools| !tools.is_empty())
    {
        prompt.push_str(
            "\n[Bridge Note]\n\
The original client sent tool definitions. Do not emit tool_use JSON back to the bridge. \
Use your own native Kiro CLI tools directly when helpful.\n",
        );
    }

    prompt.push_str("\n[Conversation]\n");
    for message in &request.messages {
        prompt.push('[');
        prompt.push_str(&message.role.to_uppercase());
        prompt.push_str("]\n");
        let content = render_message_content(&message.content);
        if content.is_empty() {
            prompt.push_str("(empty)\n\n");
        } else {
            prompt.push_str(&content);
            prompt.push_str("\n\n");
        }
    }

    prompt.push_str("[Task]\nReply to the latest user request.\n");
    prompt
}

fn render_message_content(content: &serde_json::Value) -> String {
    match content {
        serde_json::Value::String(text) => text.trim().to_string(),
        serde_json::Value::Array(items) => items
            .iter()
            .filter_map(render_content_block)
            .filter(|text| !text.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        _ => serde_json::to_string_pretty(content).unwrap_or_default(),
    }
}

fn render_content_block(item: &serde_json::Value) -> Option<String> {
    let block_type = item
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    match block_type {
        "" => serde_json::to_string_pretty(item).ok(),
        "text" => item
            .get("text")
            .and_then(|value| value.as_str())
            .map(|value| value.trim().to_string()),
        "thinking" => None,
        "tool_use" => {
            let name = item
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or("tool");
            let input = item
                .get("input")
                .map(|value| {
                    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
                })
                .unwrap_or_else(|| "{}".to_string());
            Some(format!("Tool call `{}`:\n{}", name, input))
        }
        "tool_result" => {
            let tool_use_id = item
                .get("tool_use_id")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let content = item
                .get("content")
                .map(|value| match value {
                    serde_json::Value::String(text) => text.to_string(),
                    _ => serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string()),
                })
                .unwrap_or_default();
            Some(format!("Tool result `{}`:\n{}", tool_use_id, content))
        }
        "image" => Some("[image omitted]".to_string()),
        _ => serde_json::to_string_pretty(item).ok(),
    }
}

fn strip_control_sequences(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            match chars.peek().copied() {
                Some('[') => {
                    chars.next();
                    for c in chars.by_ref() {
                        if ('@'..='~').contains(&c) {
                            break;
                        }
                    }
                }
                Some(']') => {
                    chars.next();
                    while let Some(c) = chars.next() {
                        if c == '\u{7}' {
                            break;
                        }
                        if c == '\u{1b}' && chars.peek().copied() == Some('\\') {
                            chars.next();
                            break;
                        }
                    }
                }
                _ => {}
            }
            continue;
        }

        if ch == '\r' {
            continue;
        }

        if ch.is_control() && ch != '\n' && ch != '\t' {
            continue;
        }

        output.push(ch);
    }

    output
}

fn extract_reply_text(clean_output: &str) -> String {
    let output = clean_output.trim();
    if output.is_empty() {
        return String::new();
    }

    let Some(start) = output.find("> ") else {
        return output.to_string();
    };

    let tail = &output[start + 2..];
    let end = ["\n ▸ Credits:", "\n▸ Credits:"]
        .into_iter()
        .filter_map(|marker| tail.find(marker))
        .min()
        .unwrap_or(tail.len());

    tail[..end].trim().to_string()
}

fn first_non_empty<'a>(values: &[&'a str]) -> Option<&'a str> {
    values
        .iter()
        .map(|value| value.trim())
        .find(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::anthropic::types::{Message, MessagesRequest, SystemMessage};

    fn sample_request() -> MessagesRequest {
        MessagesRequest {
            model: "glm-5".to_string(),
            max_tokens: 16000,
            messages: vec![
                Message {
                    role: "user".to_string(),
                    content: json!([
                        {"type": "text", "text": "first question"},
                        {"type": "tool_result", "tool_use_id": "toolu_1", "content": "done"}
                    ]),
                },
                Message {
                    role: "assistant".to_string(),
                    content: json!("intermediate answer"),
                },
            ],
            stream: false,
            system: Some(vec![SystemMessage {
                text: "follow the repo style".to_string(),
            }]),
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        }
    }

    #[test]
    fn test_supports_model() {
        assert!(supports_model("glm-5"));
        assert!(supports_model(" GLM-5 "));
        assert!(!supports_model("claude-sonnet-4.5"));
    }

    #[test]
    fn test_render_prompt_contains_transcript() {
        let prompt = render_prompt(&sample_request());
        assert!(prompt.contains("[System Instructions]"));
        assert!(prompt.contains("follow the repo style"));
        assert!(prompt.contains("[USER]"));
        assert!(prompt.contains("first question"));
        assert!(prompt.contains("Tool result `toolu_1`"));
        assert!(prompt.contains("[ASSISTANT]"));
        assert!(prompt.contains("intermediate answer"));
    }

    #[test]
    fn test_strip_control_sequences_and_extract_reply() {
        let output =
            "\u{1b}[38;5;141m> \u{1b}[0mA\nB\n\u{1b}[38;5;8m\n ▸ Credits: 0.17 • Time: 6s\n";
        let clean = strip_control_sequences(output);
        assert_eq!(extract_reply_text(&clean), "A\nB");
    }
}
