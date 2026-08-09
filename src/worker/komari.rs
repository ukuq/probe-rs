//! komari worker：WS 长连接（v2 JSON-RPC）+ 周期上报。
//!
//! 安全纪律：不实现远程执行类下行方法——但**友好回绝**而非干等：
//! - agent.exec → POST /api/clients/task/result 回 "Remote control is disabled."（exit_code -1，
//!   与官方探针关闭远控时的行为一致），面板任务立即完结
//! - agent.terminal.request → 拨 terminal WS 发一句说明后关闭，浏览器立即看到提示
//!   （否则面板要空转 30s 超时）
//! - agent.ping → 最多学习 5 个目标，由全局 Ping worker 异步采集；这里只读缓存回执
//!
//! 我们从不调用 agent.pull 声明能力，正常服务端不会排队这些事件。
//! 断线只保留最新报告（komari 无 ts/批量语义，旧数据无重发价值）。

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

use crate::buffer::Buffers;
use crate::config::SharedConfig;
use crate::model::PingKind;
use crate::worker::ping::PingSnapshot;

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
    reporter_id: String,
    endpoint: String,
    token: String,
    mut out_rx: watch::Receiver<KomariOut>,
    buffers: Arc<Buffers>,
    cfg: Arc<SharedConfig>,
    ping_rx: watch::Receiver<PingSnapshot>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match run_session(
                &reporter_id,
                &endpoint,
                &token,
                &mut out_rx,
                &buffers,
                &cfg,
                &ping_rx,
            )
            .await
            {
                Ok(()) => tracing::warn!("komari WS 连接结束，5s 后重连"),
                Err(e) => {
                    buffers.push_error(
                        format!("reporter:{reporter_id}"),
                        format!("Komari WS 连接失败: {e}"),
                    );
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
    reporter_id: &str,
    endpoint: &str,
    token: &str,
    out_rx: &mut watch::Receiver<KomariOut>,
    buffers: &Buffers,
    cfg: &SharedConfig,
    ping_rx: &watch::Receiver<PingSnapshot>,
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
                        if let Some(frame) = handle_downstream(
                            &t,
                            reporter_id,
                            endpoint,
                            token,
                            buffers,
                            cfg,
                            ping_rx,
                        ) {
                            ws.send(Message::Text(frame.into())).await?;
                        }
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

/// 下行帧：远程执行类请求友好回绝；Ping 只登记本地采集需求并读取缓存。
fn handle_downstream(
    text: &str,
    reporter_id: &str,
    endpoint: &str,
    token: &str,
    buffers: &Buffers,
    cfg: &SharedConfig,
    ping_rx: &watch::Receiver<PingSnapshot>,
) -> Option<String> {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return None;
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
            return None;
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
            return None;
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
            return None;
        }
        let ping_type = if s("ping_type").is_empty() {
            v.get("ping_type")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        } else {
            s("ping_type")
        };
        let ping_target = if s("ping_target").is_empty() {
            v.get("ping_target")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        } else {
            s("ping_target")
        };
        let now = crate::model::now_millis();
        let mut value = -1;
        let mut finished_at = now;
        let mut cache_hit = false;
        match prepare_ping_target(&ping_type, &ping_target)
            .and_then(|(kind, target)| cfg.learn_komari_ping(reporter_id, kind, &target, now))
        {
            Ok(registration) => {
                let max_age = registration
                    .interval
                    .saturating_mul(2)
                    .max(10)
                    .saturating_mul(1000)
                    .min(i64::MAX as u64) as i64;
                if let Some(record) = ping_rx.borrow().get(&registration.task_id) {
                    let age = now.saturating_sub(record.ts);
                    if (0..=max_age).contains(&age) {
                        value = record.rtt;
                        finished_at = record.ts;
                        cache_hit = true;
                    }
                }
                tracing::debug!(
                    reporter_id,
                    task_id,
                    cache_hit,
                    value,
                    "Komari Ping 已从本地采集缓存应答"
                );
            }
            Err(error) => {
                buffers.push_error(
                    format!("reporter:{reporter_id}"),
                    format!("Komari Ping 目标未接受: {error}"),
                );
                tracing::warn!(reporter_id, task_id, %error, "Komari Ping 目标未接受");
            }
        }
        return Some(ping_result_frame(task_id, &ping_type, value, finished_at));
    } else {
        tracing::debug!(frame = %text.chars().take(120).collect::<String>(), "komari 下行帧（忽略）");
    }
    None
}

fn prepare_ping_target(ping_type: &str, target: &str) -> anyhow::Result<(PingKind, String)> {
    let kind = match ping_type.trim().to_ascii_lowercase().as_str() {
        "icmp" => PingKind::Icmp,
        "tcp" => PingKind::Tcp,
        "http" => PingKind::Http,
        other => anyhow::bail!("不支持的 Ping 类型: {other}"),
    };
    let target = target.trim();
    if target.is_empty() {
        anyhow::bail!("Ping target 不能为空");
    }
    let target = if kind == PingKind::Http {
        let lower = target.to_ascii_lowercase();
        if lower.starts_with("http://") || lower.starts_with("https://") {
            target.to_string()
        } else if target.parse::<IpAddr>().is_ok_and(|ip| ip.is_ipv6()) {
            format!("http://[{target}]")
        } else {
            format!("http://{target}")
        }
    } else {
        target.to_string()
    };
    Ok((kind, target))
}

fn ping_result_frame(task_id: u64, ping_type: &str, value: i64, finished_at: i64) -> String {
    let finished_at = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(finished_at)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339();
    crate::reporter_komari::notification(
        "agent.pingResult",
        serde_json::json!({
            "task_id": task_id,
            "ping_type": ping_type.trim().to_ascii_lowercase(),
            "value": value,
            "finished_at": finished_at,
        }),
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LocalConfig;
    use crate::model::PingRecord;

    #[test]
    fn komari_ping_learns_first_then_reads_collection_cache() {
        let dir = std::env::temp_dir().join(format!(
            "probe-rs-komari-worker-test-{}-{}",
            std::process::id(),
            crate::model::now_millis()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        let cfg: LocalConfig = toml::from_str(
            r#"
net_static_path = "C:/tmp/net.json"

[[reporters]]
id = "komari"
protocol = "komari"
server_id = "node"
secret = "token"
worker_url = "https://komari.example.com"
report_interval = 1
report_gpu = true

[reporters.intervals]
collect = 1
ping = 30
slow = 60
gpu = 60
ip = 600
diskio = 10
"#,
        )
        .unwrap();
        cfg.validate().unwrap();
        std::fs::write(&path, toml::to_string_pretty(&cfg).unwrap()).unwrap();
        let (shared, _intervals_rx, _config_rx) = SharedConfig::new(cfg, path);
        let buffers = Buffers::new();
        let (ping_tx, ping_rx) = watch::channel(PingSnapshot::new());
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "agent.ping",
            "params": {
                "ping_task_id": 7,
                "ping_type": "icmp",
                "ping_target": "EXAMPLE.com"
            }
        })
        .to_string();

        let first = handle_downstream(
            &request,
            "komari",
            "https://komari.example.com",
            "token",
            &buffers,
            &shared,
            &ping_rx,
        )
        .unwrap();
        let first: Value = serde_json::from_str(&first).unwrap();
        assert_eq!(first["method"], "agent.pingResult");
        assert_eq!(first["params"]["value"], -1);
        assert_eq!(shared.get().reporters[0].ext.komari.learned_pings.len(), 1);

        let ts = crate::model::now_millis();
        ping_tx.send_modify(|snapshot| {
            snapshot.insert(
                "icmp:example.com".into(),
                PingRecord {
                    ts,
                    name: "icmp:example.com".into(),
                    rtt: 42,
                    loss: 0,
                },
            );
        });
        let second = handle_downstream(
            &request,
            "komari",
            "https://komari.example.com",
            "token",
            &buffers,
            &shared,
            &ping_rx,
        )
        .unwrap();
        let second: Value = serde_json::from_str(&second).unwrap();
        assert_eq!(second["params"]["task_id"], 7);
        assert_eq!(second["params"]["ping_type"], "icmp");
        assert_eq!(second["params"]["value"], 42);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn komari_http_target_gets_default_scheme() {
        assert_eq!(
            prepare_ping_target("http", "example.com").unwrap(),
            (PingKind::Http, "http://example.com".to_string())
        );
        assert_eq!(
            prepare_ping_target("http", "2001:db8::1").unwrap(),
            (PingKind::Http, "http://[2001:db8::1]".to_string())
        );
        assert!(prepare_ping_target("udp", "example.com").is_err());
    }
}
