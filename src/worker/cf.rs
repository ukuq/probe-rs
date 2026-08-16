//! CF `/update` WebSocket transport.
//!
//! The socket actor owns reconnect/backoff and reads server-pushed config even
//! while the Reporter is sleeping. Metrics still originate in ReporterRunner,
//! so buffer acknowledgement remains tied to a real server ack.

use std::collections::VecDeque;
use std::fmt;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::USER_AGENT;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

use crate::reporter_cf::{
    cf_agent_version, parse_response_body, CfResponse, CfUpdate, CF_CONFIG_SCHEMA,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
const SEND_TIMEOUT: Duration = Duration::from_secs(12);
const RETRY_MIN: Duration = Duration::from_secs(60);
const RETRY_MAX: Duration = Duration::from_secs(300);
pub const POLICY_BACKOFF: Duration = Duration::from_secs(120);
const REPORT_MIN: Duration = Duration::from_secs(1);
const REPORT_MAX: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Control {
    enabled: bool,
    config_md5: String,
}

struct Command {
    payload: String,
    reply: oneshot::Sender<std::result::Result<CfWsAck, ReplyError>>,
}

#[derive(Debug, Clone)]
enum ReplyError {
    Transport(String),
    Policy(PolicyError),
}

impl fmt::Display for ReplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(reason) => f.write_str(reason),
            Self::Policy(error) => error.fmt(f),
        }
    }
}

#[derive(Debug, Clone)]
struct PolicyError {
    code: i64,
    reason: String,
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CF WSS server policy error code={} error={}",
            self.code, self.reason
        )
    }
}

impl std::error::Error for PolicyError {}

#[derive(Debug)]
pub struct CfWsAck {
    pub response: CfResponse,
    pub next_report_after: Option<Duration>,
}

#[derive(Debug)]
pub enum CfWsEvent {
    Connected,
    Disconnected(String),
    PolicyBackoff { reason: String, duration: Duration },
    ReportInterval(Duration),
    Config(CfResponse),
}

#[derive(Clone)]
pub struct CfWsSender {
    command_tx: mpsc::Sender<Command>,
    control_tx: watch::Sender<Control>,
    connected_tx: watch::Sender<bool>,
    connected_rx: watch::Receiver<bool>,
    invalidate_tx: watch::Sender<u64>,
}

impl CfWsSender {
    pub fn connected(&self) -> bool {
        *self.connected_rx.borrow()
    }

    pub fn set_config(&self, enabled: bool, config_md5: &str) {
        let desired = Control {
            enabled,
            config_md5: normalized_md5(config_md5),
        };
        self.control_tx.send_if_modified(|current| {
            if *current == desired {
                false
            } else {
                *current = desired;
                true
            }
        });
    }

    pub async fn send(&self, update: &CfUpdate) -> Result<CfWsAck> {
        if !self.connected() {
            bail!("WSS 尚未连接");
        }
        let payload = serde_json::to_string(update).context("序列化 CF WSS 上报失败")?;
        self.send_payload_with_timeout(payload, SEND_TIMEOUT).await
    }

    async fn send_payload_with_timeout(
        &self,
        payload: String,
        timeout: Duration,
    ) -> Result<CfWsAck> {
        let (reply, response) = oneshot::channel();
        tokio::time::timeout(timeout, self.command_tx.send(Command { payload, reply }))
            .await
            .context("提交 CF WSS 上报超时")?
            .map_err(|_| anyhow::anyhow!("CF WSS worker 已退出"))?;
        match tokio::time::timeout(timeout, response).await {
            Ok(Ok(Ok(ack))) => Ok(ack),
            Ok(Ok(Err(ReplyError::Transport(reason)))) => Err(anyhow::Error::msg(reason)),
            Ok(Ok(Err(ReplyError::Policy(error)))) => Err(anyhow::Error::new(error)),
            Ok(Err(_)) => {
                self.invalidate_session();
                bail!("CF WSS ack 通道已关闭")
            }
            Err(_) => {
                // The actor still owns this command's reply sender. Force the
                // current socket generation to end so the orphaned pending
                // entry cannot consume a later ACK or grow without bound.
                self.invalidate_session();
                bail!("等待 CF WSS ack 超时")
            }
        }
    }

    fn invalidate_session(&self) {
        self.connected_tx.send_replace(false);
        self.invalidate_tx
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

pub fn policy_backoff(error: &anyhow::Error) -> Option<Duration> {
    error.downcast_ref::<PolicyError>().map(|_| POLICY_BACKOFF)
}

pub fn spawn(
    reporter_id: String,
    endpoint: String,
    agent_version: String,
    enabled: bool,
    config_md5: String,
) -> (
    CfWsSender,
    mpsc::Receiver<CfWsEvent>,
    tokio::task::JoinHandle<()>,
) {
    let (command_tx, command_rx) = mpsc::channel(4);
    let (event_tx, event_rx) = mpsc::channel(32);
    let (control_tx, control_rx) = watch::channel(Control {
        enabled,
        config_md5: normalized_md5(&config_md5),
    });
    let (connected_tx, connected_rx) = watch::channel(false);
    let (invalidate_tx, invalidate_rx) = watch::channel(0_u64);
    let sender = CfWsSender {
        command_tx,
        control_tx,
        connected_tx: connected_tx.clone(),
        connected_rx,
        invalidate_tx,
    };
    let task = tokio::spawn(run_actor(
        reporter_id,
        endpoint,
        agent_version,
        command_rx,
        event_tx,
        control_rx,
        connected_tx,
        invalidate_rx,
    ));
    (sender, event_rx, task)
}

#[allow(clippy::too_many_arguments)]
async fn run_actor(
    reporter_id: String,
    endpoint: String,
    agent_version: String,
    mut command_rx: mpsc::Receiver<Command>,
    event_tx: mpsc::Sender<CfWsEvent>,
    mut control_rx: watch::Receiver<Control>,
    connected_tx: watch::Sender<bool>,
    mut invalidate_rx: watch::Receiver<u64>,
) {
    let mut retry = RETRY_MIN;
    loop {
        if !control_rx.borrow().enabled {
            if !wait_disabled(&mut control_rx, &mut command_rx).await {
                return;
            }
            retry = RETRY_MIN;
            continue;
        }

        // ACK timeouts that raced with a previous disconnect must not poison
        // the next socket generation.
        invalidate_rx.borrow_and_update();
        let config_md5 = control_rx.borrow().config_md5.clone();
        match connect(&endpoint, &agent_version, &config_md5).await {
            Ok(ws) => {
                retry = RETRY_MIN;
                connected_tx.send_replace(true);
                let _ = event_tx.send(CfWsEvent::Connected).await;
                tracing::info!(reporter_id, "CF WSS connected");
                let result = run_session(
                    ws,
                    &mut command_rx,
                    &event_tx,
                    &mut control_rx,
                    &mut invalidate_rx,
                )
                .await;
                connected_tx.send_replace(false);
                fail_queued(&mut command_rx, "CF WSS disconnected");
                match result {
                    Ok(()) if !control_rx.borrow().enabled => {
                        tracing::info!(reporter_id, "CF WSS disabled");
                        continue;
                    }
                    Ok(()) => {
                        let reason = "CF WSS connection closed".to_string();
                        let _ = event_tx.send(CfWsEvent::Disconnected(reason)).await;
                    }
                    Err(error) => {
                        let reason = error.to_string();
                        tracing::warn!(reporter_id, error = %reason, "CF WSS disconnected");
                        if error.downcast_ref::<PolicyError>().is_some() {
                            retry = retry.max(POLICY_BACKOFF);
                            let _ = event_tx
                                .send(CfWsEvent::PolicyBackoff {
                                    reason,
                                    duration: POLICY_BACKOFF,
                                })
                                .await;
                        } else {
                            let _ = event_tx.send(CfWsEvent::Disconnected(reason)).await;
                        }
                    }
                }
            }
            Err(error) => {
                connected_tx.send_replace(false);
                let reason = error.to_string();
                tracing::warn!(reporter_id, error = %reason, retry_secs = retry.as_secs(), "CF WSS connect failed");
                let _ = event_tx.send(CfWsEvent::Disconnected(reason)).await;
            }
        }

        if !wait_retry(retry, &mut control_rx, &mut command_rx).await {
            return;
        }
        retry = retry.saturating_mul(2).min(RETRY_MAX);
    }
}

async fn wait_disabled(
    control_rx: &mut watch::Receiver<Control>,
    command_rx: &mut mpsc::Receiver<Command>,
) -> bool {
    loop {
        tokio::select! {
            changed = control_rx.changed() => {
                if changed.is_err() { return false; }
                if control_rx.borrow().enabled { return true; }
            }
            command = command_rx.recv() => match command {
                Some(command) => { let _ = command.reply.send(Err(ReplyError::Transport("CF WSS disabled".into()))); }
                None => return false,
            }
        }
    }
}

async fn wait_retry(
    duration: Duration,
    control_rx: &mut watch::Receiver<Control>,
    command_rx: &mut mpsc::Receiver<Command>,
) -> bool {
    let sleep = tokio::time::sleep(duration);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = &mut sleep => return true,
            changed = control_rx.changed() => {
                if changed.is_err() { return false; }
                if !control_rx.borrow().enabled { return true; }
            }
            command = command_rx.recv() => match command {
                Some(command) => { let _ = command.reply.send(Err(ReplyError::Transport("CF WSS reconnecting".into()))); }
                None => return false,
            }
        }
    }
}

fn fail_queued(command_rx: &mut mpsc::Receiver<Command>, reason: &str) {
    while let Ok(command) = command_rx.try_recv() {
        let _ = command
            .reply
            .send(Err(ReplyError::Transport(reason.to_string())));
    }
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(endpoint: &str, agent_version: &str, config_md5: &str) -> Result<WsStream> {
    let url = websocket_url(endpoint, config_md5)?;
    let mut request = url
        .into_client_request()
        .context("构建 CF WSS 握手请求失败")?;
    let headers = request.headers_mut();
    headers.insert(USER_AGENT, HeaderValue::from_static("cfsm"));
    headers.insert(
        "X-Agent-Version",
        HeaderValue::from_str(&cf_agent_version(agent_version)).context("CF Agent 版本头非法")?,
    );
    headers.insert(
        "X-Agent-Config-Schema",
        HeaderValue::from_static(CF_CONFIG_SCHEMA),
    );
    headers.insert(
        "X-Agent-Config-Md5",
        HeaderValue::from_str(&normalized_md5(config_md5)).context("CF 配置 MD5 头非法")?,
    );

    let (mut ws, _) =
        tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(request))
            .await
            .context("CF WSS 握手超时")?
            .context("CF WSS 握手失败")?;
    let hello = tokio::time::timeout(HELLO_TIMEOUT, ws.next())
        .await
        .context("等待 CF WSS hello 超时")?
        .ok_or_else(|| anyhow::anyhow!("CF WSS 在 hello 前关闭"))?
        .context("读取 CF WSS hello 失败")?;
    let Message::Text(text) = hello else {
        bail!("CF WSS hello 不是文本帧");
    };
    let hello: HelloFrame =
        serde_json::from_str(text.as_ref()).context("解析 CF WSS hello 失败")?;
    if hello.kind != "hello" || hello.protocol != "update" {
        bail!(
            "CF WSS hello 非法: type={:?} protocol={:?}",
            hello.kind,
            hello.protocol
        );
    }
    Ok(ws)
}

async fn run_session(
    mut ws: WsStream,
    command_rx: &mut mpsc::Receiver<Command>,
    event_tx: &mpsc::Sender<CfWsEvent>,
    control_rx: &mut watch::Receiver<Control>,
    invalidate_rx: &mut watch::Receiver<u64>,
) -> Result<()> {
    let mut pending: VecDeque<oneshot::Sender<std::result::Result<CfWsAck, ReplyError>>> =
        VecDeque::new();
    loop {
        tokio::select! {
            changed = control_rx.changed() => {
                if changed.is_err() || !control_rx.borrow().enabled {
                    let _ = ws.close(None).await;
                    fail_pending(&mut pending, "CF WSS disabled");
                    return Ok(());
                }
            }
            changed = invalidate_rx.changed() => {
                let reason = if changed.is_err() {
                    "CF WSS sender closed"
                } else {
                    "CF WSS session invalidated after ACK timeout"
                };
                let _ = ws.close(None).await;
                fail_pending(&mut pending, reason);
                bail!(reason);
            }
            command = command_rx.recv() => {
                let Some(command) = command else {
                    fail_pending(&mut pending, "CF WSS worker stopped");
                    return Ok(());
                };
                if let Err(error) = ws.send(Message::Text(command.payload.into())).await {
                    let reason = format!("发送 CF WSS 报告失败: {error}");
                    let _ = command
                        .reply
                        .send(Err(ReplyError::Transport(reason.clone())));
                    fail_pending(&mut pending, &reason);
                    bail!(reason);
                }
                pending.push_back(command.reply);
            }
            message = ws.next() => {
                let Some(message) = message else {
                    fail_pending(&mut pending, "CF WSS connection closed");
                    bail!("CF WSS connection closed");
                };
                match message.context("读取 CF WSS 帧失败")? {
                    Message::Text(text) => {
                        handle_frame(text.as_ref(), &mut pending, event_tx).await?;
                    }
                    Message::Ping(payload) => ws.send(Message::Pong(payload)).await.context("发送 CF WSS pong 失败")?,
                    Message::Pong(_) => {}
                    Message::Close(frame) => {
                        fail_pending(&mut pending, "CF WSS server closed connection");
                        bail!("CF WSS server closed connection: {frame:?}");
                    }
                    Message::Binary(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

async fn handle_frame(
    text: &str,
    pending: &mut VecDeque<oneshot::Sender<std::result::Result<CfWsAck, ReplyError>>>,
    event_tx: &mpsc::Sender<CfWsEvent>,
) -> Result<()> {
    let frame: ServerFrame = serde_json::from_str(text).context("解析 CF WSS 服务端帧失败")?;
    match frame.kind.as_str() {
        "ack" => {
            let next_report_after = frame
                .next_wss_report_after_ms
                .and_then(clamped_report_interval);
            if let Some(duration) = next_report_after {
                // Interval hints are replaceable state. Never let a burst of
                // hints fill the event channel and block delivery of the ack
                // that the Reporter is currently waiting for.
                let _ = event_tx.try_send(CfWsEvent::ReportInterval(duration));
            }
            let response = frame_response(&frame);
            if frame.realtime_hint {
                if response.push.is_some() || response.correction.is_some() {
                    let _ = event_tx.send(CfWsEvent::Config(response)).await;
                }
                return Ok(());
            }
            if let Some(reply) = pending.pop_front() {
                let _ = reply.send(Ok(CfWsAck {
                    response,
                    next_report_after,
                }));
            } else if response.push.is_some() || response.correction.is_some() {
                let _ = event_tx.send(CfWsEvent::Config(response)).await;
            }
        }
        "config" | "remote_config" => {
            let response = frame_response(&frame);
            if response.push.is_some() || response.correction.is_some() {
                let _ = event_tx.send(CfWsEvent::Config(response)).await;
            }
        }
        "error" => {
            let error = PolicyError {
                code: frame.code,
                reason: frame.error.unwrap_or_else(|| "server_error".into()),
            };
            fail_pending_policy(pending, error.clone());
            return Err(error.into());
        }
        "hello" => {}
        _ => tracing::debug!(frame_type = %frame.kind, "ignored CF WSS frame"),
    }
    Ok(())
}

fn fail_pending(
    pending: &mut VecDeque<oneshot::Sender<std::result::Result<CfWsAck, ReplyError>>>,
    reason: &str,
) {
    while let Some(reply) = pending.pop_front() {
        let _ = reply.send(Err(ReplyError::Transport(reason.to_string())));
    }
}

fn fail_pending_policy(
    pending: &mut VecDeque<oneshot::Sender<std::result::Result<CfWsAck, ReplyError>>>,
    error: PolicyError,
) {
    while let Some(reply) = pending.pop_front() {
        let _ = reply.send(Err(ReplyError::Policy(error.clone())));
    }
}

fn frame_response(frame: &ServerFrame) -> CfResponse {
    let body = frame
        .body
        .as_deref()
        .filter(|body| !body.trim().is_empty())
        .or_else(|| {
            frame
                .config_body
                .as_deref()
                .filter(|body| !body.trim().is_empty())
        })
        .unwrap_or("");
    parse_response_body(body, frame.config_md5.as_deref())
}

fn clamped_report_interval(ms: i64) -> Option<Duration> {
    if ms <= 0 {
        return None;
    }
    Some(Duration::from_millis(ms as u64).clamp(REPORT_MIN, REPORT_MAX))
}

fn normalized_md5(value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        "none".to_string()
    } else {
        value.to_string()
    }
}

pub fn websocket_url(endpoint: &str, config_md5: &str) -> Result<String> {
    let mut url = url::Url::parse(endpoint).context("解析 CF worker_url 失败")?;
    match url.scheme() {
        "https" => url
            .set_scheme("wss")
            .map_err(|_| anyhow::anyhow!("无法生成 wss URL"))?,
        "http" => url
            .set_scheme("ws")
            .map_err(|_| anyhow::anyhow!("无法生成 ws URL"))?,
        scheme => bail!("CF WSS 不支持 URL scheme: {scheme}"),
    }
    let existing: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(key, _)| key != "config_schema" && key != "config_md5")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    url.set_query(None);
    let mut query = url.query_pairs_mut();
    for (key, value) in existing {
        query.append_pair(&key, &value);
    }
    query.append_pair("config_schema", CF_CONFIG_SCHEMA);
    query.append_pair("config_md5", &normalized_md5(config_md5));
    drop(query);
    Ok(url.into())
}

pub fn default_report_interval(report_interval_secs: u64) -> Duration {
    Duration::from_secs(report_interval_secs.max(1).div_ceil(15)).clamp(REPORT_MIN, REPORT_MAX)
}

#[derive(Debug, Deserialize)]
struct HelloFrame {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    protocol: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServerFrame {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    next_wss_report_after_ms: Option<i64>,
    #[serde(default)]
    realtime_hint: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    code: i64,
    #[serde(default)]
    body: Option<String>,
    #[serde(default, alias = "config_body")]
    config_body: Option<String>,
    #[serde(default, alias = "config_md5", alias = "md5")]
    config_md5: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ack_timeout_invalidates_the_current_socket_generation() {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let (control_tx, _control_rx) = watch::channel(Control {
            enabled: true,
            config_md5: "none".into(),
        });
        let (connected_tx, connected_rx) = watch::channel(true);
        let (invalidate_tx, mut invalidate_rx) = watch::channel(0_u64);
        let sender = CfWsSender {
            command_tx,
            control_tx,
            connected_tx,
            connected_rx,
            invalidate_tx,
        };
        let held_command = tokio::spawn(async move {
            let command = command_rx.recv().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            drop(command);
        });

        let error = sender
            .send_payload_with_timeout("{}".into(), Duration::from_millis(10))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("ack 超时"));
        assert!(!sender.connected());
        tokio::time::timeout(Duration::from_millis(100), invalidate_rx.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*invalidate_rx.borrow(), 1);
        held_command.abort();
    }

    #[tokio::test]
    async fn server_error_frames_mark_pending_reports_for_policy_backoff() {
        let (reply, response) = oneshot::channel();
        let mut pending = VecDeque::from([reply]);
        let (event_tx, _event_rx) = mpsc::channel(1);

        let error = handle_frame(
            r#"{"type":"error","code":401,"error":"Invalid secret"}"#,
            &mut pending,
            &event_tx,
        )
        .await
        .unwrap_err();

        assert_eq!(policy_backoff(&error), Some(POLICY_BACKOFF));
        assert!(pending.is_empty());
        match response.await.unwrap().unwrap_err() {
            ReplyError::Policy(error) => {
                assert_eq!(error.code, 401);
                assert_eq!(error.reason, "Invalid secret");
            }
            other => panic!("expected policy error, got {other:?}"),
        }
    }

    #[test]
    fn websocket_url_preserves_query_and_adds_config_state() {
        let url = websocket_url(
            "https://monitor.example/update?token=1&config_schema=3&config_md5=old",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.scheme(), "wss");
        assert_eq!(parsed.path(), "/update");
        let query: std::collections::HashMap<_, _> = parsed.query_pairs().collect();
        assert_eq!(query.get("token").map(|v| v.as_ref()), Some("1"));
        assert_eq!(
            query.get("config_schema").map(|v| v.as_ref()),
            Some(CF_CONFIG_SCHEMA)
        );
        assert_eq!(
            query.get("config_md5").map(|v| v.as_ref()),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        assert_eq!(query.len(), 3);
    }

    #[test]
    fn report_interval_matches_panel_divisor_and_limits() {
        assert_eq!(default_report_interval(60), Duration::from_secs(4));
        assert_eq!(default_report_interval(30), Duration::from_secs(2));
        assert_eq!(default_report_interval(1), Duration::from_secs(1));
    }

    #[test]
    fn server_frame_accepts_snake_case_config_fields() {
        let frame: ServerFrame = serde_json::from_str(
            r#"{"type":"ack","nextWssReportAfterMs":4000,"config_md5":"abc","config_body":"report_interval=60&connection_mode=http"}"#,
        )
        .unwrap();
        assert_eq!(frame.next_wss_report_after_ms, Some(4000));
        let response = frame_response(&frame);
        let push = response.push.unwrap();
        assert_eq!(push.report, Some(60));
        assert_eq!(
            push.connection_mode,
            Some(crate::model::CfConnectionMode::Http)
        );
    }
}
