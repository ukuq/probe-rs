//! CF `/update` WebSocket transport.
//!
//! The socket actor owns reconnect/backoff and reads server-pushed config even
//! while the Reporter is sleeping. Metrics are fire-and-forget: the Reporter
//! publishes the latest snapshot without waiting for a server ACK, while ACK
//! and config frames are handled independently by the actor.

use std::collections::VecDeque;
use std::fmt;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::USER_AGENT;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use crate::reporter_cf::{
    cf_agent_version, is_wss_schedule_inactive_reason, parse_response_body, CfResponse, CfUpdate,
    CF_CONFIG_SCHEMA, CF_WSS_MODE_HEADER, CF_WSS_REASON_HEADER,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const ACK_SILENCE_TIMEOUT: Duration = Duration::from_secs(15);
const RETRY_MIN: Duration = Duration::from_secs(60);
const RETRY_MAX: Duration = Duration::from_secs(300);
pub const POLICY_BACKOFF: Duration = Duration::from_secs(120);
pub const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(2);
const MIN_REPORT_INTERVAL: Duration = Duration::from_secs(1);
const MAX_REPORT_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Control {
    configured_enabled: bool,
    runtime_enabled: bool,
    config_md5: String,
}

impl Control {
    fn enabled(&self) -> bool {
        self.configured_enabled && self.runtime_enabled
    }
}

#[derive(Debug, Clone)]
struct Outbound {
    payload: String,
    through: u64,
    included_static: bool,
}

#[derive(Debug)]
struct PendingDelivery {
    through: u64,
    included_static: bool,
    deadline: tokio::time::Instant,
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

#[derive(Debug, Clone)]
struct ScheduleInactiveError {
    reason: String,
}

impl fmt::Display for ScheduleInactiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CF WSS schedule inactive: {}", self.reason)
    }
}

impl std::error::Error for ScheduleInactiveError {}

#[derive(Debug)]
pub enum CfWsEvent {
    Connected,
    Disconnected(String),
    ScheduleInactive { reason: String },
    ReportIntervalChanged(Duration),
    PolicyBackoff { reason: String, duration: Duration },
    Acknowledged { through: u64, included_static: bool },
    Config(Box<CfResponse>),
}

#[derive(Clone)]
pub struct CfWsSender {
    payload_tx: watch::Sender<Option<Outbound>>,
    control_tx: watch::Sender<Control>,
    connected_rx: watch::Receiver<bool>,
    in_flight_rx: watch::Receiver<Option<u64>>,
    report_interval_rx: watch::Receiver<Duration>,
}

struct ActorChannels {
    payload_rx: watch::Receiver<Option<Outbound>>,
    event_tx: mpsc::Sender<CfWsEvent>,
    control_rx: watch::Receiver<Control>,
    connected_tx: watch::Sender<bool>,
    in_flight_tx: watch::Sender<Option<u64>>,
    report_interval_tx: watch::Sender<Duration>,
}

impl CfWsSender {
    pub fn connected(&self) -> bool {
        *self.connected_rx.borrow()
    }

    /// Highest journal sequence claimed by a frame being written, or already
    /// written, on the current socket generation.
    pub fn in_flight_through(&self) -> Option<u64> {
        *self.in_flight_rx.borrow()
    }

    pub fn report_interval(&self) -> Duration {
        *self.report_interval_rx.borrow()
    }

    pub fn set_config(&self, enabled: bool, config_md5: &str) {
        let config_md5 = normalized_md5(config_md5);
        self.control_tx.send_if_modified(|current| {
            if current.configured_enabled == enabled && current.config_md5 == config_md5 {
                false
            } else {
                current.configured_enabled = enabled;
                current.config_md5.clone_from(&config_md5);
                true
            }
        });
    }

    /// Temporarily gate WSS without changing the persisted connection mode.
    pub fn set_runtime_enabled(&self, enabled: bool) -> bool {
        self.control_tx.send_if_modified(|current| {
            if current.runtime_enabled == enabled {
                false
            } else {
                current.runtime_enabled = enabled;
                true
            }
        })
    }

    /// Publish the latest report without waiting for a server ACK.
    ///
    /// The watch channel keeps only one payload. If the socket is temporarily
    /// slower than the 1s producer, fresh metrics replace stale ones instead of
    /// building an unbounded queue.
    pub fn send(&self, update: &CfUpdate, through: u64, included_static: bool) -> Result<()> {
        if !self.connected() {
            bail!("WSS 尚未连接");
        }
        let payload = serde_json::to_string(update).context("序列化 CF WSS 上报失败")?;
        self.payload_tx.send_replace(Some(Outbound {
            payload,
            through,
            included_static,
        }));
        Ok(())
    }
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
    let (payload_tx, payload_rx) = watch::channel(None);
    let (event_tx, event_rx) = mpsc::channel(32);
    let (control_tx, control_rx) = watch::channel(Control {
        configured_enabled: enabled,
        runtime_enabled: true,
        config_md5: normalized_md5(&config_md5),
    });
    let (connected_tx, connected_rx) = watch::channel(false);
    let (in_flight_tx, in_flight_rx) = watch::channel(None);
    let (report_interval_tx, report_interval_rx) = watch::channel(DEFAULT_REPORT_INTERVAL);
    let sender = CfWsSender {
        payload_tx,
        control_tx,
        connected_rx,
        in_flight_rx,
        report_interval_rx,
    };
    let task = tokio::spawn(run_actor(
        reporter_id,
        endpoint,
        agent_version,
        ActorChannels {
            payload_rx,
            event_tx,
            control_rx,
            connected_tx,
            in_flight_tx,
            report_interval_tx,
        },
    ));
    (sender, event_rx, task)
}

async fn run_actor(
    reporter_id: String,
    endpoint: String,
    agent_version: String,
    channels: ActorChannels,
) {
    let ActorChannels {
        mut payload_rx,
        event_tx,
        mut control_rx,
        connected_tx,
        in_flight_tx,
        report_interval_tx,
    } = channels;
    let mut retry = RETRY_MIN;
    loop {
        if !control_rx.borrow().enabled() {
            if !wait_disabled(&mut control_rx, &mut payload_rx).await {
                return;
            }
            retry = RETRY_MIN;
            continue;
        }

        let config_md5 = control_rx.borrow().config_md5.clone();
        match connect(&endpoint, &agent_version, &config_md5).await {
            Ok(mut ws) => {
                // Control can change while the handshake and hello exchange are
                // in progress. Do not advertise or use a connection that was
                // disabled before connect() completed.
                if !control_rx.borrow().enabled() {
                    let _ = tokio::time::timeout(SOCKET_WRITE_TIMEOUT, ws.close(None)).await;
                    continue;
                }
                retry = RETRY_MIN;
                // Drop the previous generation's queued snapshot before the
                // connected flag allows the Reporter to publish a fresh one.
                payload_rx.borrow_and_update();
                in_flight_tx.send_replace(None);
                report_interval_tx.send_replace(DEFAULT_REPORT_INTERVAL);
                connected_tx.send_replace(true);
                let _ = event_tx.send(CfWsEvent::Connected).await;
                tracing::info!(reporter_id, "CF WSS connected");
                let result = run_session(
                    ws,
                    &mut payload_rx,
                    &event_tx,
                    &mut control_rx,
                    &in_flight_tx,
                    &report_interval_tx,
                )
                .await;
                connected_tx.send_replace(false);
                in_flight_tx.send_replace(None);
                match result {
                    Ok(()) if !control_rx.borrow().enabled() => {
                        tracing::info!(reporter_id, "CF WSS disabled");
                        continue;
                    }
                    Ok(()) => {
                        let reason = "CF WSS connection closed".to_string();
                        let _ = event_tx.send(CfWsEvent::Disconnected(reason)).await;
                    }
                    Err(error) => {
                        if let Some(error) = error.downcast_ref::<ScheduleInactiveError>() {
                            tracing::info!(
                                reporter_id,
                                reason = %error.reason,
                                "CF WSS schedule inactive"
                            );
                            let _ = event_tx
                                .send(CfWsEvent::ScheduleInactive {
                                    reason: error.reason.clone(),
                                })
                                .await;
                        } else {
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
            }
            Err(error) => {
                connected_tx.send_replace(false);
                in_flight_tx.send_replace(None);
                if let Some(error) = error.downcast_ref::<ScheduleInactiveError>() {
                    tracing::info!(
                        reporter_id,
                        reason = %error.reason,
                        "CF WSS schedule inactive during handshake"
                    );
                    let _ = event_tx
                        .send(CfWsEvent::ScheduleInactive {
                            reason: error.reason.clone(),
                        })
                        .await;
                } else {
                    let reason = error.to_string();
                    tracing::warn!(reporter_id, error = %reason, retry_secs = retry.as_secs(), "CF WSS connect failed");
                    let _ = event_tx.send(CfWsEvent::Disconnected(reason)).await;
                }
            }
        }

        if !wait_retry(retry, &mut control_rx, &mut payload_rx).await {
            return;
        }
        retry = retry.saturating_mul(2).min(RETRY_MAX);
    }
}

async fn wait_disabled(
    control_rx: &mut watch::Receiver<Control>,
    payload_rx: &mut watch::Receiver<Option<Outbound>>,
) -> bool {
    loop {
        tokio::select! {
            changed = control_rx.changed() => {
                if changed.is_err() { return false; }
                if control_rx.borrow().enabled() { return true; }
            }
            changed = payload_rx.changed() => {
                if changed.is_err() { return false; }
                payload_rx.borrow_and_update();
            }
        }
    }
}

async fn wait_retry(
    duration: Duration,
    control_rx: &mut watch::Receiver<Control>,
    payload_rx: &mut watch::Receiver<Option<Outbound>>,
) -> bool {
    let sleep = tokio::time::sleep(duration);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = &mut sleep => return true,
            changed = control_rx.changed() => {
                if changed.is_err() { return false; }
                if !control_rx.borrow().enabled() { return true; }
            }
            changed = payload_rx.changed() => {
                if changed.is_err() { return false; }
                payload_rx.borrow_and_update();
            }
        }
    }
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn normalized_schedule_inactive_reason(reason: &str) -> Option<String> {
    let reason = reason.trim().to_ascii_lowercase();
    is_wss_schedule_inactive_reason(&reason).then_some(reason)
}

fn schedule_inactive_from_headers(mode: Option<&str>, reason: Option<&str>) -> Option<String> {
    let mode = mode.unwrap_or_default().trim().to_ascii_lowercase();
    if !mode.is_empty() && mode != "inactive" {
        return None;
    }
    normalized_schedule_inactive_reason(reason.unwrap_or_default())
}

fn schedule_inactive_from_handshake(error: &WsError) -> Option<String> {
    let WsError::Http(response) = error else {
        return None;
    };
    if response.status().as_u16() != 409 {
        return None;
    }
    let mode = response
        .headers()
        .get(CF_WSS_MODE_HEADER)
        .and_then(|value| value.to_str().ok());
    let reason = response
        .headers()
        .get(CF_WSS_REASON_HEADER)
        .and_then(|value| value.to_str().ok());
    if let Some(reason) = schedule_inactive_from_headers(mode, reason) {
        return Some(reason);
    }

    let body = response.body().as_deref()?;
    let body = std::str::from_utf8(body).ok()?.trim();
    if let Some(reason) = normalized_schedule_inactive_reason(body) {
        return Some(reason);
    }
    let payload: serde_json::Value = serde_json::from_str(body).ok()?;
    ["text", "error"].into_iter().find_map(|key| {
        payload
            .get(key)
            .and_then(serde_json::Value::as_str)
            .and_then(normalized_schedule_inactive_reason)
    })
}

fn schedule_inactive_from_close(code: u16, reason: &str) -> Option<String> {
    (code == 1013)
        .then(|| normalized_schedule_inactive_reason(reason))
        .flatten()
}

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

    let handshake =
        tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(request))
            .await
            .context("CF WSS 握手超时")?;
    let (mut ws, _) = match handshake {
        Ok(connected) => connected,
        Err(error) => {
            if let Some(reason) = schedule_inactive_from_handshake(&error) {
                return Err(ScheduleInactiveError { reason }.into());
            }
            return Err(error).context("CF WSS 握手失败");
        }
    };
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
    payload_rx: &mut watch::Receiver<Option<Outbound>>,
    event_tx: &mpsc::Sender<CfWsEvent>,
    control_rx: &mut watch::Receiver<Control>,
    in_flight_tx: &watch::Sender<Option<u64>>,
    report_interval_tx: &watch::Sender<Duration>,
) -> Result<()> {
    let mut pending = VecDeque::<PendingDelivery>::new();
    loop {
        tokio::select! {
            _ = wait_for_ack_deadline(pending.front().map(|delivery| delivery.deadline)) => {
                let _ = tokio::time::timeout(SOCKET_WRITE_TIMEOUT, ws.close(None)).await;
                bail!("CF WSS 连续 {} 秒未收到 ACK", ACK_SILENCE_TIMEOUT.as_secs());
            }
            changed = control_rx.changed() => {
                if changed.is_err() || !control_rx.borrow().enabled() {
                    let _ = ws.close(None).await;
                    return Ok(());
                }
            }
            changed = payload_rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                // Prefer the latest control state even when a report and a
                // runtime-disable notification become ready together.
                if !control_rx.borrow().enabled() {
                    let _ = ws.close(None).await;
                    return Ok(());
                }
                let outbound = payload_rx.borrow_and_update().clone();
                if let Some(outbound) = outbound {
                    let Outbound {
                        payload,
                        through,
                        included_static,
                    } = outbound;
                    // Claim the batch before the potentially slow socket
                    // write. A replacement that happens while this write is
                    // in progress must contain only records after this frame.
                    advance_in_flight(in_flight_tx, through);
                    tokio::time::timeout(
                        SOCKET_WRITE_TIMEOUT,
                        ws.send(Message::Text(payload.into())),
                    )
                        .await
                        .context("发送 CF WSS 报告超时")?
                        .context("发送 CF WSS 报告失败")?;
                    // ReporterRunner never waits for this ACK. Only compact
                    // cursor metadata is retained so the server response can
                    // confirm journal progress asynchronously.
                    pending.push_back(PendingDelivery {
                        through,
                        included_static,
                        deadline: tokio::time::Instant::now() + ACK_SILENCE_TIMEOUT,
                    });
                }
            }
            message = ws.next() => {
                let Some(message) = message else {
                    bail!("CF WSS connection closed");
                };
                match message.context("读取 CF WSS 帧失败")? {
                    Message::Text(text) => {
                        if handle_frame(text.as_ref(), event_tx, report_interval_tx).await? {
                            if let Some(delivery) = pending.pop_front() {
                                let _ = event_tx
                                    .send(CfWsEvent::Acknowledged {
                                        through: delivery.through,
                                        included_static: delivery.included_static,
                                    })
                                    .await;
                            } else {
                                tracing::debug!("ignored unsolicited CF WSS ACK");
                            }
                        }
                    }
                    Message::Ping(payload) => ws.send(Message::Pong(payload)).await.context("发送 CF WSS pong 失败")?,
                    Message::Pong(_) => {}
                    Message::Close(frame) => {
                        if let Some(reason) = frame.as_ref().and_then(|frame| {
                            schedule_inactive_from_close(u16::from(frame.code), frame.reason.as_ref())
                        }) {
                            return Err(ScheduleInactiveError { reason }.into());
                        }
                        bail!("CF WSS server closed connection: {frame:?}");
                    }
                    Message::Binary(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

fn advance_in_flight(in_flight_tx: &watch::Sender<Option<u64>>, through: u64) {
    in_flight_tx.send_if_modified(|current| {
        let next = Some(current.map_or(through, |sent| sent.max(through)));
        if *current == next {
            false
        } else {
            *current = next;
            true
        }
    });
}

async fn wait_for_ack_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Returns true when the frame proves that the server has acknowledged at
/// least one report. Config frames are still delivered independently.
async fn handle_frame(
    text: &str,
    event_tx: &mpsc::Sender<CfWsEvent>,
    report_interval_tx: &watch::Sender<Duration>,
) -> Result<bool> {
    let frame: ServerFrame = serde_json::from_str(text).context("解析 CF WSS 服务端帧失败")?;
    let acknowledged = match frame.kind.as_str() {
        "ack" => {
            if let Some(next) = frame
                .next_wss_report_after_ms
                .and_then(normalize_report_interval)
            {
                let changed = report_interval_tx.send_if_modified(|current| {
                    if *current == next {
                        false
                    } else {
                        *current = next;
                        true
                    }
                });
                if changed {
                    let _ = event_tx.send(CfWsEvent::ReportIntervalChanged(next)).await;
                }
            }
            let response = frame_response(&frame);
            if response.push.is_some() || response.correction.is_some() {
                let _ = event_tx.send(CfWsEvent::Config(Box::new(response))).await;
            }
            // A realtime hint reuses the ACK-shaped envelope but is not tied
            // to an outbound report and must not advance its journal cursor.
            !frame.realtime_hint
        }
        "config" | "remote_config" => {
            let response = frame_response(&frame);
            if response.push.is_some() || response.correction.is_some() {
                let _ = event_tx.send(CfWsEvent::Config(Box::new(response))).await;
            }
            false
        }
        "error" => {
            if frame.code == 409 {
                let reason = frame
                    .text
                    .as_deref()
                    .and_then(normalized_schedule_inactive_reason)
                    .or_else(|| {
                        frame
                            .error
                            .as_deref()
                            .and_then(normalized_schedule_inactive_reason)
                    });
                if let Some(reason) = reason {
                    return Err(ScheduleInactiveError { reason }.into());
                }
            }
            let error = PolicyError {
                code: frame.code,
                reason: frame.error.unwrap_or_else(|| "server_error".into()),
            };
            return Err(error.into());
        }
        "hello" => false,
        _ => {
            tracing::debug!(frame_type = %frame.kind, "ignored CF WSS frame");
            false
        }
    };
    Ok(acknowledged)
}

fn normalize_report_interval(milliseconds: i64) -> Option<Duration> {
    let milliseconds = u64::try_from(milliseconds)
        .ok()
        .filter(|value| *value > 0)?;
    Some(
        Duration::from_millis(milliseconds)
            .max(MIN_REPORT_INTERVAL)
            .min(MAX_REPORT_INTERVAL),
    )
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
    realtime_hint: bool,
    #[serde(default)]
    next_wss_report_after_ms: Option<i64>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    text: Option<String>,
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
    async fn sender_replaces_an_unsent_snapshot_without_waiting_for_ack() {
        let (payload_tx, mut payload_rx) = watch::channel(None);
        let (control_tx, _control_rx) = watch::channel(Control {
            configured_enabled: true,
            runtime_enabled: true,
            config_md5: "none".into(),
        });
        let (_connected_tx, connected_rx) = watch::channel(true);
        let (_in_flight_tx, in_flight_rx) = watch::channel(None);
        let (_report_interval_tx, report_interval_rx) = watch::channel(DEFAULT_REPORT_INTERVAL);
        let sender = CfWsSender {
            payload_tx,
            control_tx,
            connected_rx,
            in_flight_rx,
            report_interval_rx,
        };

        sender.payload_tx.send_replace(Some(Outbound {
            payload: "first".into(),
            through: 1,
            included_static: false,
        }));
        sender.payload_tx.send_replace(Some(Outbound {
            payload: "second".into(),
            through: 2,
            included_static: true,
        }));

        payload_rx.changed().await.unwrap();
        let latest = payload_rx.borrow_and_update().clone().unwrap();
        assert_eq!(latest.payload, "second");
        assert_eq!(latest.through, 2);
        assert!(latest.included_static);
    }

    #[test]
    fn runtime_gate_does_not_change_persisted_wss_configuration() {
        let (payload_tx, _payload_rx) = watch::channel(None);
        let (control_tx, control_rx) = watch::channel(Control {
            configured_enabled: true,
            runtime_enabled: true,
            config_md5: "none".into(),
        });
        let (_connected_tx, connected_rx) = watch::channel(false);
        let (_in_flight_tx, in_flight_rx) = watch::channel(None);
        let (_report_interval_tx, report_interval_rx) = watch::channel(DEFAULT_REPORT_INTERVAL);
        let sender = CfWsSender {
            payload_tx,
            control_tx,
            connected_rx,
            in_flight_rx,
            report_interval_rx,
        };

        assert!(control_rx.borrow().enabled());
        assert!(sender.set_runtime_enabled(false));
        assert!(!control_rx.borrow().enabled());
        assert!(control_rx.borrow().configured_enabled);

        sender.set_config(false, "abc");
        assert!(sender.set_runtime_enabled(true));
        assert!(!control_rx.borrow().enabled());
        sender.set_config(true, "abc");
        assert!(control_rx.borrow().enabled());
    }

    #[tokio::test]
    async fn runtime_gate_wakes_retry_and_disabled_waits() {
        let (control_tx, control_rx) = watch::channel(Control {
            configured_enabled: true,
            runtime_enabled: true,
            config_md5: "none".into(),
        });
        let (_payload_tx, payload_rx) = watch::channel(None);
        let retry_wait = tokio::spawn(async move {
            let mut control_rx = control_rx;
            let mut payload_rx = payload_rx;
            wait_retry(Duration::from_secs(60), &mut control_rx, &mut payload_rx).await
        });
        tokio::task::yield_now().await;
        control_tx.send_modify(|control| control.runtime_enabled = false);
        assert!(tokio::time::timeout(Duration::from_secs(1), retry_wait)
            .await
            .expect("runtime disable did not wake retry wait")
            .unwrap());

        let (control_tx, control_rx) = watch::channel(Control {
            configured_enabled: true,
            runtime_enabled: false,
            config_md5: "none".into(),
        });
        let (_payload_tx, payload_rx) = watch::channel(None);
        let disabled_wait = tokio::spawn(async move {
            let mut control_rx = control_rx;
            let mut payload_rx = payload_rx;
            wait_disabled(&mut control_rx, &mut payload_rx).await
        });
        tokio::task::yield_now().await;
        control_tx.send_modify(|control| control.runtime_enabled = true);
        assert!(tokio::time::timeout(Duration::from_secs(1), disabled_wait)
            .await
            .expect("runtime enable did not wake disabled wait")
            .unwrap());
    }

    #[tokio::test]
    async fn server_error_frames_trigger_policy_backoff() {
        let (event_tx, _event_rx) = mpsc::channel(1);
        let (report_interval_tx, _report_interval_rx) = watch::channel(DEFAULT_REPORT_INTERVAL);

        let error = handle_frame(
            r#"{"type":"error","code":401,"error":"Invalid secret"}"#,
            &event_tx,
            &report_interval_tx,
        )
        .await
        .unwrap_err();

        let error = error.downcast_ref::<PolicyError>().unwrap();
        assert_eq!(error.code, 401);
        assert_eq!(error.reason, "Invalid secret");
    }

    #[tokio::test]
    async fn schedule_error_frame_disables_only_runtime_wss() {
        let (event_tx, _event_rx) = mpsc::channel(1);
        let (report_interval_tx, _report_interval_rx) = watch::channel(DEFAULT_REPORT_INTERVAL);

        let error = handle_frame(
            r#"{"type":"error","code":409,"error":"Agent WSS report outside active hours","text":"wss_schedule_inactive","connection_mode":"http"}"#,
            &event_tx,
            &report_interval_tx,
        )
        .await
        .unwrap_err();

        let schedule = error.downcast_ref::<ScheduleInactiveError>().unwrap();
        assert_eq!(schedule.reason, "wss_schedule_inactive");
        assert!(error.downcast_ref::<PolicyError>().is_none());
    }

    #[test]
    fn schedule_handshake_and_close_signals_are_recognized_narrowly() {
        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(409)
            .header(CF_WSS_MODE_HEADER, "inactive")
            .header(CF_WSS_REASON_HEADER, "wss_schedule_empty")
            .body(Some(Vec::new()))
            .unwrap();
        let error = WsError::Http(response);
        assert_eq!(
            schedule_inactive_from_handshake(&error).as_deref(),
            Some("wss_schedule_empty")
        );

        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(409)
            .header(CF_WSS_REASON_HEADER, "wss_schedule_inactive")
            .body(Some(Vec::new()))
            .unwrap();
        let error = WsError::Http(response);
        assert_eq!(
            schedule_inactive_from_handshake(&error).as_deref(),
            Some("wss_schedule_inactive")
        );

        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(409)
            .body(Some(b"wss_schedule_empty".to_vec()))
            .unwrap();
        let error = WsError::Http(response);
        assert_eq!(
            schedule_inactive_from_handshake(&error).as_deref(),
            Some("wss_schedule_empty")
        );

        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(409)
            .body(Some(
                br#"{"text":"wss_schedule_inactive","connection_mode":"http"}"#.to_vec(),
            ))
            .unwrap();
        let error = WsError::Http(response);
        assert_eq!(
            schedule_inactive_from_handshake(&error).as_deref(),
            Some("wss_schedule_inactive")
        );

        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(409)
            .header(CF_WSS_MODE_HEADER, "active")
            .header(CF_WSS_REASON_HEADER, "wss_schedule_inactive")
            .body(Some(Vec::new()))
            .unwrap();
        let error = WsError::Http(response);
        assert!(schedule_inactive_from_handshake(&error).is_none());

        assert_eq!(
            schedule_inactive_from_close(1013, "wss_schedule_inactive").as_deref(),
            Some("wss_schedule_inactive")
        );
        assert!(schedule_inactive_from_close(1008, "wss_schedule_inactive").is_none());
        assert!(schedule_inactive_from_close(1013, "try_again_later").is_none());
    }

    #[tokio::test]
    async fn realtime_hint_updates_cadence_without_acking_an_outbound_report() {
        let (event_tx, mut event_rx) = mpsc::channel(1);
        let (report_interval_tx, report_interval_rx) = watch::channel(DEFAULT_REPORT_INTERVAL);
        let acknowledged = handle_frame(
            r#"{"type":"ack","realtimeHint":true,"nextWssReportAfterMs":1000}"#,
            &event_tx,
            &report_interval_tx,
        )
        .await
        .unwrap();
        assert!(!acknowledged);
        assert_eq!(*report_interval_rx.borrow(), Duration::from_secs(1));
        let CfWsEvent::ReportIntervalChanged(interval) = event_rx.recv().await.unwrap() else {
            panic!("expected report interval event");
        };
        assert_eq!(interval, Duration::from_secs(1));
    }

    #[test]
    fn server_report_interval_is_bounded_and_rejects_non_positive_values() {
        assert_eq!(normalize_report_interval(1), Some(Duration::from_secs(1)));
        assert_eq!(
            normalize_report_interval(600_000),
            Some(Duration::from_secs(300))
        );
        assert_eq!(normalize_report_interval(0), None);
        assert_eq!(normalize_report_interval(-1), None);
    }

    #[tokio::test]
    async fn ack_watchdog_is_disabled_without_pending_reports() {
        assert!(
            tokio::time::timeout(Duration::from_millis(5), wait_for_ack_deadline(None),)
                .await
                .is_err()
        );
        tokio::time::timeout(
            Duration::from_millis(100),
            wait_for_ack_deadline(Some(tokio::time::Instant::now() + Duration::from_millis(5))),
        )
        .await
        .unwrap();
    }

    #[test]
    fn in_flight_cursor_is_monotonic_within_a_socket_generation() {
        let (in_flight_tx, in_flight_rx) = watch::channel(None);
        advance_in_flight(&in_flight_tx, 4);
        advance_in_flight(&in_flight_tx, 3);
        assert_eq!(*in_flight_rx.borrow(), Some(4));

        in_flight_tx.send_replace(None);
        assert_eq!(*in_flight_rx.borrow(), None);
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

    #[tokio::test]
    async fn ack_config_is_delivered_without_a_pending_report() {
        let (event_tx, mut event_rx) = mpsc::channel(2);
        let (report_interval_tx, report_interval_rx) = watch::channel(DEFAULT_REPORT_INTERVAL);
        let acknowledged = handle_frame(
            r#"{"type":"ack","nextWssReportAfterMs":4000,"config_md5":"abc","config_body":"report_interval=60&wss_report_interval=4&connection_mode=auto"}"#,
            &event_tx,
            &report_interval_tx,
        )
        .await
        .unwrap();
        assert!(acknowledged);
        assert_eq!(*report_interval_rx.borrow(), Duration::from_secs(4));
        let CfWsEvent::ReportIntervalChanged(interval) = event_rx.recv().await.unwrap() else {
            panic!("expected report interval event");
        };
        assert_eq!(interval, Duration::from_secs(4));
        let CfWsEvent::Config(response) = event_rx.recv().await.unwrap() else {
            panic!("expected config event");
        };
        let push = response.push.unwrap();
        assert_eq!(push.report, Some(60));
        assert_eq!(push.wss_report_interval, Some(4));
    }
}
