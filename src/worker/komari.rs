//! komari worker：WS 长连接（v2 JSON-RPC）+ 周期上报。
//!
//! 安全纪律：不实现任何服务端下行方法——但**友好回绝**而非干等：
//! - agent.exec → POST /api/clients/task/result 回 "Remote control is disabled."（exit_code -1，
//!   与官方探针关闭远控时的行为一致），面板任务立即完结
//! - agent.terminal.request → 拨 terminal WS 发一句说明后关闭，浏览器立即看到提示
//!   （否则面板要空转 30s 超时）
//! - agent.ping → 回 agent.pingResult value=-1（与官方"unsupported ping type"一致）
//!
//! 我们从不调用 agent.pull 声明能力，正常服务端不会排队这些事件。
//! 断线只保留最新报告（komari 无 ts/批量语义，旧数据无重发价值）。

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

use crate::buffer::Buffers;

/// scheduler → worker 的输出：最新报告 / 最新 basicInfo（各自只留最新一份）
#[derive(Debug, Clone, Default)]
pub struct KomariOut {
    pub report: Option<Value>,
    pub basic_info: Option<Value>,
}

const RECONNECT: Duration = Duration::from_secs(5);
/// komari 服务端的读超时是 11s（web/api/client readWait），且只有**数据帧**能续期
/// （gorilla 内部吞 ping）。所以心跳必须每 ~5s 重发一次最新 report 文本帧——
/// 与官方 agent 的高频上报保活行为一致
const HEARTBEAT: Duration = Duration::from_secs(5);

pub fn spawn(
    endpoint: String,
    token: String,
    mut out_rx: watch::Receiver<KomariOut>,
    buffers: Arc<Buffers>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match run_session(&endpoint, &token, &mut out_rx).await {
                Ok(()) => tracing::warn!("komari WS 连接结束，5s 后重连"),
                Err(e) => {
                    buffers.push_error("komari", format!("WS 连接失败: {e}"));
                    // 不打 URL：query 里有 token，防泄漏进 journald
                    tracing::warn!(error = %e, "komari WS 断开，5s 后重连");
                }
            }
            tokio::time::sleep(RECONNECT).await;
        }
    })
}

fn url_token(token: &str) -> String {
    url::form_urlencoded::byte_serialize(token.as_bytes()).collect()
}

fn to_ws_url(endpoint: &str, token: &str) -> String {
    let base = endpoint.trim_end_matches('/');
    let ws_base = base.replacen("http", "ws", 1);
    format!("{ws_base}/api/clients/v2/rpc?token={}", url_token(token))
}

async fn run_session(
    endpoint: &str,
    token: &str,
    out_rx: &mut watch::Receiver<KomariOut>,
) -> anyhow::Result<()> {
    let (mut ws, _) = tokio_tungstenite::connect_async(to_ws_url(endpoint, token)).await?;
    tracing::info!("komari WS 已连接");
    // 连上先发 basicInfo + 最新 report（如果有）；先取快照再 await（watch::Ref 不是 Send）
    let initial = out_rx.borrow().clone();
    if let Some(bi) = initial.basic_info {
        ws.send(Message::Text(
            crate::reporter_komari::basic_info_frame(bi).into(),
        ))
        .await?;
    }
    if let Some(rep) = initial.report {
        ws.send(Message::Text(
            crate::reporter_komari::report_frame(rep).into(),
        ))
        .await?;
    }
    let mut heartbeat = tokio::time::interval(HEARTBEAT);
    heartbeat.tick().await; // 跳过立即触发
    loop {
        tokio::select! {
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(t))) => {
                        handle_downstream(&t, endpoint, token).await;
                    }
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Ok(_)) => {} // ping/pong/binary
                    Some(Err(e)) => return Err(e.into()),
                }
            }
            r = out_rx.changed() => {
                if r.is_err() { return Ok(()); }
                let out = out_rx.borrow().clone();
                if let Some(bi) = out.basic_info {
                    ws.send(Message::Text(crate::reporter_komari::basic_info_frame(bi).into())).await?;
                }
                if let Some(rep) = out.report {
                    ws.send(Message::Text(crate::reporter_komari::report_frame(rep).into())).await?;
                }
            }
            _ = heartbeat.tick() => {
                // 重发最新 report 作数据帧心跳（WS Ping 续不了 komari 的读超时）
                let latest = out_rx.borrow().report.clone();
                if let Some(rep) = latest {
                    ws.send(Message::Text(crate::reporter_komari::report_frame(rep).into())).await?;
                }
            }
        }
    }
}

/// 下行帧：只识别 v2 事件与 v1 遗留格式，全部友好回绝，绝不执行
async fn handle_downstream(text: &str, endpoint: &str, token: &str) {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return;
    };
    let method = v.get("method").and_then(Value::as_str).unwrap_or("");
    let params = v.get("params").cloned().unwrap_or(Value::Null);
    let s = |k: &str| {
        params
            .get(k)
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    };

    // v1 遗留：{"message":"terminal","request_id":...}
    let legacy_msg = v.get("message").and_then(Value::as_str).unwrap_or("");
    let is_terminal = method == "agent.terminal.request"
        || legacy_msg == "terminal"
        || v.get("request_id").is_some();
    let is_exec = method == "agent.exec" || legacy_msg == "exec";
    let is_ping = method == "agent.ping" || v.get("ping_task_id").is_some();

    if is_exec {
        let task_id = s("task_id");
        if task_id.is_empty() {
            return;
        }
        tracing::info!(task_id, "komari exec 任务已回绝（未实现远程执行）");
        let (ep, tk) = (endpoint.to_string(), token.to_string());
        tokio::spawn(async move { reject_exec(&ep, &tk, &task_id).await });
    } else if is_terminal {
        let request_id = if s("request_id").is_empty() {
            v.get("request_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        } else {
            s("request_id")
        };
        if request_id.is_empty() {
            return;
        }
        tracing::info!(request_id, "komari 终端请求已回绝（未实现远程终端）");
        let (ep, tk) = (endpoint.to_string(), token.to_string());
        tokio::spawn(async move { reject_terminal(&ep, &tk, &request_id).await });
    } else if is_ping {
        let task_id = params
            .get("ping_task_id")
            .and_then(Value::as_u64)
            .or_else(|| v.get("ping_task_id").and_then(Value::as_u64))
            .unwrap_or(0);
        if task_id == 0 {
            return;
        }
        let ping_type = if s("ping_type").is_empty() {
            v.get("ping_type")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        } else {
            s("ping_type")
        };
        tracing::info!(task_id, "komari ping 任务已回绝（未实现服务端调度探测）");
        let (ep, tk) = (endpoint.to_string(), token.to_string());
        tokio::spawn(async move { reject_ping(&ep, &tk, task_id, &ping_type).await });
    } else {
        tracing::debug!(frame = %text.chars().take(120).collect::<String>(), "komari 下行帧（忽略）");
    }
}

/// exec 回绝：POST 任务结果（对齐官方探针 DisableWebSsh 时的回执）
async fn reject_exec(endpoint: &str, token: &str, task_id: &str) {
    let url = format!(
        "{}/api/clients/task/result?token={}",
        endpoint.trim_end_matches('/'),
        url_token(token)
    );
    let body = serde_json::json!({
        "task_id": task_id,
        "result": "Remote control is disabled. (probe-rs 不支持远程执行)",
        "exit_code": -1,
        "finished_at": chrono::Local::now().to_rfc3339(),
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_default();
    let _ = client.post(url).json(&body).send().await;
}

/// terminal 回绝：拨通终端 WS 发一句说明即关闭（浏览器立即可见，不再空转 30s）
async fn reject_terminal(endpoint: &str, token: &str, request_id: &str) {
    let base = endpoint.trim_end_matches('/').replacen("http", "ws", 1);
    let url = format!(
        "{base}/api/clients/terminal?token={}&id={request_id}",
        url_token(token)
    );
    if let Ok((mut ws, _)) = tokio_tungstenite::connect_async(url).await {
        let _ = ws
            .send(Message::Text(
                "该探针不支持远程终端（安全策略）\nRemote terminal is disabled by probe-rs.\n"
                    .into(),
            ))
            .await;
        let _ = ws.close(None).await;
    }
}

/// ping 回绝：value=-1（与官方"unsupported ping type"行为一致，任务完结标记失败）
async fn reject_ping(endpoint: &str, token: &str, task_id: u64, ping_type: &str) {
    let url = format!(
        "{}/api/clients/v2/rpc?token={}",
        endpoint.trim_end_matches('/'),
        url_token(token)
    );
    let frame = crate::reporter_komari::notification(
        "agent.pingResult",
        serde_json::json!({
            "task_id": task_id,
            "ping_type": ping_type,
            "value": -1,
            "finished_at": chrono::Local::now().to_rfc3339(),
        }),
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_default();
    let _ = client.post(url).body(frame).send().await;
}
