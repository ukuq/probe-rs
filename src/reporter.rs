//! 上报器：POST /report，X-Secret 认证；失败保留游标（journal 事件留在
//! 共享日志中待下轮重发），成功才 ACK。
//! 响应体非空时解析为远端配置

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures_util::future::join_all;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::net::{lookup_host, UdpSocket};
use tokio::sync::watch;

use crate::model::{RemoteConfig, Report, ReportTime};
use crate::reporter_cf::{
    parse_wss_runtime_headers, CfConfirm, CfResponse, CfUpdate, CF_WSS_MODE_HEADER,
    CF_WSS_REASON_HEADER,
};

const TIMEOUT: Duration = Duration::from_secs(8);
const NTP_SERVERS: [&str; 4] = [
    "time.cloudflare.com",
    "time.google.com",
    "time.nist.gov",
    "ntp.aliyun.com",
];
const NTP_TIMEOUT: Duration = Duration::from_secs(2);
const NTP_REFRESH_INTERVAL: Duration = Duration::from_secs(600);
const MAX_CALIBRATION_AGE: Duration = Duration::from_secs(86_400);
const NTP_UNIX_EPOCH_SECONDS: i128 = 2_208_988_800;
const NANOS_PER_SECOND: i128 = 1_000_000_000;
const NANOS_PER_MILLI: i128 = 1_000_000;
/// server_time 合理性限幅:2000-01-01 .. 2100-01-01 之外的绝对时间视为非法,
/// 防止单个故障/恶意服务端污染全进程共享的校准时钟。
const MIN_PLAUSIBLE_SERVER_TIME_MS: i64 = 946_684_800_000;
const MAX_PLAUSIBLE_SERVER_TIME_MS: i64 = 4_102_444_800_000;
/// server_time 采样的 RTT 上限(请求已受 TIMEOUT 约束,这里是显式双保险)。
const MAX_CALIBRATION_RTT: Duration = Duration::from_secs(30);
/// server_time 与本机墙钟允许的最大偏差。服务端校准的价值在于修正秒/分钟级
/// 偏差;数天级的分歧几乎必然来自故障或恶意端点,而本机墙钟若真错到该量级,
/// 正确的处置是修本机时钟,而不是让任意外部样本接管全进程时间域。
const MAX_SERVER_LOCAL_DIVERGENCE_MS: i64 = 24 * 3600 * 1000;

/// 响应信封：config 缺席 = 无配置变更；next.static = true 时下次上报强制带 static
#[derive(serde::Deserialize)]
struct ReportResponse {
    config: Option<RemoteConfig>,
    #[serde(default)]
    next: Option<NextDirective>,
    /// 服务端生成响应时的 Unix 毫秒时间；旧服务端可省略。
    #[serde(default)]
    server_time: Option<i64>,
}

/// 对下一次上报的指令
#[derive(serde::Deserialize)]
struct NextDirective {
    #[serde(rename = "static", default)]
    r#static: bool,
}

/// 上报响应中需要 agent 执行的动作
pub struct ResponseAction {
    pub config: Option<RemoteConfig>,
    /// 下次上报强制带 static
    pub next_static: bool,
}

pub struct Reporter {
    client: reqwest::Client,
    url: String,
    secret: String,
    agent_version: String,
    reporter_id: String,
    protocol: String,
    clock: Arc<AgentClock>,
}

/// Agent-wide calibrated clock shared by every Reporter protocol.
#[derive(Default)]
pub struct AgentClock {
    state: Mutex<ClockState>,
}

#[derive(Default)]
struct ClockState {
    ntp: Option<ClockCalibration>,
    server: Option<ClockCalibration>,
}

struct ClockCalibration {
    source: String,
    anchor: Instant,
    accurate_at_anchor: i64,
    round_trip_ms: u64,
}

impl ClockCalibration {
    fn from_server_time(server_time: i64, round_trip: Duration, anchor: Instant) -> Self {
        let round_trip_ms = duration_millis(round_trip);
        // server_time is stamped close to response transmission. With only one
        // server timestamp, the request/response midpoint is the best estimate;
        // half the RTT is also the approximate uncertainty bound.
        let half_round_trip = round_trip_ms / 2 + round_trip_ms % 2;
        Self {
            source: "server".to_owned(),
            anchor,
            accurate_at_anchor: add_millis(server_time, half_round_trip),
            round_trip_ms,
        }
    }

    fn from_ntp(sample: NtpSample) -> Self {
        Self {
            source: format!("ntp:{}", sample.server),
            anchor: sample.anchor,
            accurate_at_anchor: sample.accurate_at_anchor,
            round_trip_ms: sample.round_trip_ms,
        }
    }

    fn snapshot(&self, local_ts: i64, age: Duration) -> ReportTime {
        let sample_age_ms = duration_millis(age);
        let accurate_ts = add_millis(self.accurate_at_anchor, sample_age_ms);
        ReportTime {
            local_ts,
            accurate_ts: Some(accurate_ts),
            offset_ms: Some(accurate_ts.saturating_sub(local_ts)),
            source: Some(self.source.clone()),
            round_trip_ms: Some(self.round_trip_ms),
            sample_age_ms: Some(sample_age_ms),
        }
    }
}

struct NtpSample {
    server: &'static str,
    anchor: Instant,
    accurate_at_anchor: i64,
    round_trip_ms: u64,
    offset_nanos: i128,
}

impl AgentClock {
    /// Snapshot both the local wall clock and the best calibrated clock.
    pub fn report_time(&self) -> ReportTime {
        self.report_time_at(Instant::now())
    }

    /// `report_time` with an injectable monotonic `now`. `now` anchors sample
    /// freshness and age, so tests can simulate stale samples without relying
    /// on the system having run longer than `MAX_CALIBRATION_AGE`.
    fn report_time_at(&self, now: Instant) -> ReportTime {
        let local_ts = crate::model::now_millis();
        let clock = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let fresh = |sample: &ClockCalibration| {
            now.saturating_duration_since(sample.anchor) <= MAX_CALIBRATION_AGE
        };
        let calibration = clock
            .ntp
            .as_ref()
            .filter(|sample| fresh(sample))
            .or_else(|| clock.server.as_ref().filter(|sample| fresh(sample)))
            // Once calibrated, stay in the monotonic calibrated domain even
            // if refreshes are temporarily unavailable. Falling back to a
            // user-adjusted wall clock would make timestamps jump domains.
            .or_else(|| match (clock.ntp.as_ref(), clock.server.as_ref()) {
                (Some(ntp), Some(server)) => Some(if ntp.anchor >= server.anchor {
                    ntp
                } else {
                    server
                }),
                (Some(ntp), None) => Some(ntp),
                (None, Some(server)) => Some(server),
                (None, None) => None,
            });
        match calibration {
            Some(calibration) => {
                calibration.snapshot(local_ts, now.saturating_duration_since(calibration.anchor))
            }
            None => ReportTime {
                local_ts,
                accurate_ts: None,
                offset_ms: None,
                source: None,
                round_trip_ms: None,
                sample_age_ms: None,
            },
        }
    }

    fn update_server_clock(
        &self,
        server_time: i64,
        round_trip: Duration,
        anchor: Instant,
        reporter_id: &str,
    ) {
        // 合理性校验:AgentClock 是全进程共享的,一个故障/恶意的原生服务端
        // 返回任意值会污染所有 Reporter(含 CF/komari)的时间域。
        if !(MIN_PLAUSIBLE_SERVER_TIME_MS..=MAX_PLAUSIBLE_SERVER_TIME_MS).contains(&server_time) {
            tracing::warn!(
                reporter_id,
                server_time,
                "server_time 超出合理范围,校准样本被拒绝"
            );
            return;
        }
        if round_trip > MAX_CALIBRATION_RTT {
            tracing::warn!(
                reporter_id,
                round_trip_ms = duration_millis(round_trip),
                "server_time 采样 RTT 过大,校准样本被拒绝"
            );
            return;
        }
        let local_ms = crate::model::now_millis();
        if server_time.saturating_sub(local_ms).abs() > MAX_SERVER_LOCAL_DIVERGENCE_MS {
            tracing::warn!(
                reporter_id,
                server_time,
                local_ms,
                "server_time 与本机墙钟偏差超过 24h,校准样本被拒绝"
            );
            return;
        }
        let calibration = ClockCalibration::from_server_time(server_time, round_trip, anchor);
        let snapshot = calibration.snapshot(crate::model::now_millis(), anchor.elapsed());
        tracing::debug!(
            reporter_id,
            offset_ms = snapshot.offset_ms,
            round_trip_ms = snapshot.round_trip_ms,
            "native server fallback time calibrated"
        );
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .server = Some(calibration);
    }

    fn update_ntp_clock(&self, sample: NtpSample) {
        let server = sample.server;
        let calibration = ClockCalibration::from_ntp(sample);
        let snapshot =
            calibration.snapshot(crate::model::now_millis(), calibration.anchor.elapsed());
        tracing::debug!(
            server,
            offset_ms = snapshot.offset_ms,
            round_trip_ms = snapshot.round_trip_ms,
            "agent clock calibrated from NTP"
        );
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .ntp = Some(calibration);
    }

    pub async fn refresh_ntp(&self) -> bool {
        match query_ntp_servers().await {
            Ok(sample) => {
                self.update_ntp_clock(sample);
                true
            }
            Err(error) => {
                tracing::debug!(
                    %error,
                    "NTP calibration unavailable; native server time remains the fallback"
                );
                false
            }
        }
    }

    /// Run the Agent-wide NTP loop independently of all Reporter intervals.
    /// Tokio's first interval tick is immediate, then refreshes every 10 minutes.
    pub fn spawn_ntp_refresh(
        self: &Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let clock = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = crate::worker::ticker(NTP_REFRESH_INTERVAL);
            loop {
                if *shutdown_rx.borrow() {
                    return;
                }
                tokio::select! {
                    _ = ticker.tick() => { clock.refresh_ntp().await; }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return;
                        }
                    }
                }
            }
        })
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn add_millis(timestamp: i64, millis: u64) -> i64 {
    timestamp.saturating_add(i64::try_from(millis).unwrap_or(i64::MAX))
}

fn duration_nanos(duration: Duration) -> i128 {
    i128::try_from(duration.as_nanos()).unwrap_or(i128::MAX)
}

fn nanos_to_millis(value: i128) -> i64 {
    let value = value.div_euclid(NANOS_PER_MILLI);
    i64::try_from(value).unwrap_or(if value.is_negative() {
        i64::MIN
    } else {
        i64::MAX
    })
}

fn positive_nanos_to_millis(value: i128) -> u64 {
    let value = value.max(0);
    let rounded_up = value / NANOS_PER_MILLI + i128::from(value % NANOS_PER_MILLI != 0);
    u64::try_from(rounded_up).unwrap_or(u64::MAX)
}

fn unix_millis_to_ntp_timestamp(unix_millis: i64) -> [u8; 8] {
    let unix_seconds = unix_millis.div_euclid(1000);
    let millis = unix_millis.rem_euclid(1000) as u64;
    let ntp_seconds =
        (i128::from(unix_seconds) + NTP_UNIX_EPOCH_SECONDS).rem_euclid(1_i128 << 32) as u32;
    let fraction = ((u128::from(millis) << 32) / 1000) as u32;
    let mut encoded = [0_u8; 8];
    encoded[..4].copy_from_slice(&ntp_seconds.to_be_bytes());
    encoded[4..].copy_from_slice(&fraction.to_be_bytes());
    encoded
}

fn ntp_timestamp_to_unix_nanos(timestamp: &[u8], reference_unix_nanos: i128) -> Result<i128> {
    if timestamp.len() != 8 {
        bail!("invalid NTP timestamp length");
    }
    let seconds = u32::from_be_bytes(timestamp[..4].try_into().expect("checked NTP length"));
    let fraction = u32::from_be_bytes(timestamp[4..].try_into().expect("checked NTP length"));
    if seconds == 0 && fraction == 0 {
        bail!("empty NTP timestamp");
    }
    let era_seconds = 1_i128 << 32;
    let reference_ntp_seconds =
        reference_unix_nanos.div_euclid(NANOS_PER_SECOND) + NTP_UNIX_EPOCH_SECONDS;
    let estimated_era = reference_ntp_seconds.div_euclid(era_seconds);
    let absolute_seconds = (estimated_era - 1..=estimated_era + 1)
        .map(|era| era * era_seconds + i128::from(seconds))
        .min_by_key(|candidate| candidate.abs_diff(reference_ntp_seconds))
        .expect("NTP era candidates are non-empty");

    Ok(
        (absolute_seconds - NTP_UNIX_EPOCH_SECONDS) * NANOS_PER_SECOND
            + i128::from(fraction) * NANOS_PER_SECOND / era_seconds,
    )
}

async fn query_ntp_servers() -> Result<NtpSample> {
    let results = join_all(NTP_SERVERS.map(query_ntp_server)).await;
    let mut samples = Vec::new();
    for (server, result) in NTP_SERVERS.into_iter().zip(results) {
        match result {
            Ok(sample) => samples.push(sample),
            Err(error) => tracing::debug!(server, %error, "NTP query failed"),
        }
    }
    if samples.is_empty() {
        bail!("all NTP queries failed");
    }
    samples.sort_by_key(|sample| sample.offset_nanos);
    let middle = samples.len() / 2;
    let median = if samples.len() % 2 == 0 {
        samples[middle - 1]
            .offset_nanos
            .saturating_add(samples[middle].offset_nanos)
            / 2
    } else {
        samples[middle].offset_nanos
    };
    let selected = (0..samples.len())
        .min_by_key(|&index| {
            (
                samples[index].offset_nanos.abs_diff(median),
                samples[index].round_trip_ms,
            )
        })
        .expect("non-empty NTP samples");
    Ok(samples.swap_remove(selected))
}

async fn query_ntp_server(server: &'static str) -> Result<NtpSample> {
    tokio::time::timeout(NTP_TIMEOUT, async move {
        let addresses = lookup_host((server, 123))
            .await
            .with_context(|| format!("resolve NTP server {server}"))?
            .collect();
        query_ntp_addresses(server, addresses).await
    })
    .await
    .with_context(|| format!("NTP query to {server} timed out"))?
}

async fn query_ntp_addresses(
    server: &'static str,
    mut addresses: Vec<std::net::SocketAddr>,
) -> Result<NtpSample> {
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty() {
        bail!("NTP server {server} has no addresses");
    }

    let mut attempts = FuturesUnordered::new();
    for address in addresses {
        attempts.push(query_ntp_address(server, address));
    }
    let mut errors = Vec::new();
    while let Some(result) = attempts.next().await {
        match result {
            Ok(sample) => return Ok(sample),
            Err(error) => errors.push(error.to_string()),
        }
    }
    bail!(
        "all resolved addresses for NTP server {server} failed: {}",
        errors.join("; ")
    )
}

async fn query_ntp_address(
    server: &'static str,
    address: std::net::SocketAddr,
) -> Result<NtpSample> {
    let bind_address = if address.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind_address)
        .await
        .context("bind NTP UDP socket")?;
    socket
        .connect(address)
        .await
        .with_context(|| format!("connect NTP server {server}"))?;

    let local_started = crate::model::now_millis();
    let started = Instant::now();
    let mut request = [0_u8; 48];
    request[0] = 0x23; // leap=0, version=4, mode=3 (client)
    request[40..48].copy_from_slice(&unix_millis_to_ntp_timestamp(local_started));
    socket.send(&request).await.context("send NTP request")?;
    let mut response = [0_u8; 512];
    let received = socket
        .recv(&mut response)
        .await
        .context("receive NTP response")?;
    let anchor = Instant::now();
    if received < 48 {
        bail!("short NTP response: {received} bytes");
    }
    let header = response[0];
    let leap = header >> 6;
    let version = (header >> 3) & 0x07;
    let mode = header & 0x07;
    let stratum = response[1];
    if leap == 3 || !(3..=4).contains(&version) || mode != 4 || !(1..=15).contains(&stratum) {
        bail!("invalid NTP response header");
    }
    if response[24..32] != request[40..48] {
        bail!("NTP originate timestamp mismatch");
    }

    let t1 = i128::from(local_started) * NANOS_PER_MILLI;
    let elapsed = anchor.duration_since(started);
    let elapsed_nanos = duration_nanos(elapsed);
    let t4 = t1.saturating_add(elapsed_nanos);
    let t2 = ntp_timestamp_to_unix_nanos(&response[32..40], t1)?;
    let t3 = ntp_timestamp_to_unix_nanos(&response[40..48], t1)?;
    if t3 < t2 {
        bail!("NTP transmit time precedes receive time");
    }
    let offset_nanos = (t2.saturating_sub(t1) + t3.saturating_sub(t4)) / 2;
    let accurate_at_anchor = nanos_to_millis(t4.saturating_add(offset_nanos));
    let network_delay = elapsed_nanos.saturating_sub(t3.saturating_sub(t2));
    Ok(NtpSample {
        server,
        anchor,
        accurate_at_anchor,
        round_trip_ms: positive_nanos_to_millis(network_delay),
        offset_nanos,
    })
}

impl Reporter {
    pub fn new(
        worker_url: &str,
        secret: &str,
        agent_version: &str,
        reporter_id: &str,
        protocol: &str,
        clock: Arc<AgentClock>,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(TIMEOUT)
            .pool_max_idle_per_host(1)
            // 307/308 会保留方法并把 X-Secret 等自定义头转发到重定向目标;
            // 上报端点应显式配置,不允许服务端把认证信息引向其他域。
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("构建 HTTP client 失败")?;
        Ok(Self {
            client,
            url: worker_url.to_string(),
            secret: secret.to_string(),
            agent_version: agent_version.to_string(),
            reporter_id: reporter_id.to_string(),
            protocol: protocol.to_string(),
            clock,
        })
    }

    /// 返回本次原生协议上报的时间状态。准确时间锚定在单调时钟上，
    /// 因此校准后本机墙钟发生跳变时，offset_ms 会在下一次上报反映出来。
    pub fn report_time(&self) -> ReportTime {
        self.clock.report_time()
    }

    /// 成功返回响应中的动作；任何失败返回 Err，调用方直接保留数据待重发
    pub async fn send(&self, report: &Report) -> Result<ResponseAction> {
        self.send_probe(report).await
    }

    async fn send_probe(&self, report: &Report) -> Result<ResponseAction> {
        let started = Instant::now();
        let resp = self
            .client
            .post(&self.url)
            .header("X-Secret", &self.secret)
            .header("X-Agent-Version", &self.agent_version)
            // Optional metadata: old servers ignore these headers. Values are
            // percent-encoded so arbitrary Unicode Reporter ids remain valid.
            .header("X-Reporter-Id", encode_header_value(&self.reporter_id))
            .header("X-Reporter-Protocol", encode_header_value(&self.protocol))
            .json(report)
            .send()
            .await
            .context("上报请求失败")?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("上报响应 HTTP {status}");
        }
        let body = resp.text().await.context("读取上报响应失败")?;
        let completed = Instant::now();
        let round_trip = completed.duration_since(started);
        let body = body.trim();
        if body.is_empty() || body == "{}" {
            return Ok(ResponseAction {
                config: None,
                next_static: false,
            });
        }
        let parsed: ReportResponse = serde_json::from_str(body).context("解析上报响应失败")?;
        if let Some(server_time) = parsed.server_time {
            self.clock
                .update_server_clock(server_time, round_trip, completed, &self.reporter_id);
        }
        Ok(ResponseAction {
            config: parsed.config,
            next_static: parsed.next.is_some_and(|n| n.r#static),
        })
    }

    /// CF 协议公共 POST：统一 agent_version 头与错误包装
    async fn post_cf(
        &self,
        body: &impl serde::Serialize,
        extra: &[(&str, &str)],
    ) -> Result<reqwest::Response> {
        // CF 服务端 agent_version 取值优先用该头——带官方兼容版本前缀以区分探针实现
        let agent_ver = crate::reporter_cf::cf_agent_version(&self.agent_version);
        let mut req = self
            .client
            .post(&self.url)
            .header("X-Agent-Version", &agent_ver);
        for (k, v) in extra {
            req = req.header(*k, *v);
        }
        req.json(body).send().await.context("CF 请求失败")
    }

    /// CF 协议上报（POST /update）。config_md5 为当前已应用的 CF 配置 MD5（空 = none）。
    /// 204 = 无变更；200 = 解析 URL-encoded 配置/校正
    pub async fn send_cf(&self, update: &CfUpdate, config_md5: &str) -> Result<CfResponse> {
        let md5 = if config_md5.is_empty() {
            "none"
        } else {
            config_md5
        };
        let resp = self
            .post_cf(
                update,
                &[
                    (
                        "X-Agent-Config-Schema",
                        crate::reporter_cf::CF_CONFIG_SCHEMA,
                    ),
                    ("X-Agent-Config-Md5", md5),
                ],
            )
            .await?;
        let status = resp.status();
        let wss_mode = resp
            .headers()
            .get(CF_WSS_MODE_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let wss_reason = resp
            .headers()
            .get(CF_WSS_REASON_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let wss_runtime = parse_wss_runtime_headers(wss_mode.as_deref(), wss_reason.as_deref());
        if status.as_u16() == 204 {
            return Ok(CfResponse {
                wss_runtime,
                ..Default::default()
            });
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("上报响应 HTTP {status}: {}", body.trim());
        }
        let resp_md5 = resp
            .headers()
            .get("x-agent-config-md5")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let body = resp.text().await.context("读取上报响应失败")?;
        let mut response = crate::reporter_cf::parse_response_body(&body, resp_md5.as_deref());
        response.wss_runtime = wss_runtime;
        Ok(response)
    }

    /// CF 校正确认（独立请求，不带 metrics）：成功 200 纯文本 OK
    pub async fn send_cf_confirm(&self, confirm: &CfConfirm) -> Result<()> {
        let resp = self.post_cf(confirm, &[]).await?;
        if !resp.status().is_success() {
            anyhow::bail!("校正确认响应 HTTP {}", resp.status());
        }
        Ok(())
    }
}

fn encode_header_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(&mut encoded, "%{byte:02X}").expect("writing to String cannot fail");
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{
        ntp_timestamp_to_unix_nanos, unix_millis_to_ntp_timestamp, ClockCalibration,
        NANOS_PER_MILLI,
    };

    #[test]
    fn reporter_header_value_percent_encodes_unicode() {
        let encoded = super::encode_header_value("本地 demo/一");
        assert_eq!(encoded, "%E6%9C%AC%E5%9C%B0%20demo%2F%E4%B8%80");
    }

    #[test]
    fn server_clock_calibration_uses_rtt_midpoint_and_monotonic_age() {
        let calibration = ClockCalibration::from_server_time(
            1_000_000,
            Duration::from_millis(80),
            Instant::now(),
        );
        let snapshot = calibration.snapshot(999_500, Duration::from_millis(20));
        assert_eq!(snapshot.accurate_ts, Some(1_000_060));
        assert_eq!(snapshot.offset_ms, Some(560));
        assert_eq!(snapshot.source.as_deref(), Some("server"));
        assert_eq!(snapshot.round_trip_ms, Some(80));
        assert_eq!(snapshot.sample_age_ms, Some(20));
    }

    #[test]
    fn calibrated_time_does_not_follow_local_wall_clock_jumps() {
        let calibration = ClockCalibration::from_server_time(
            1_000_000,
            Duration::from_millis(80),
            Instant::now(),
        );
        let age = Duration::from_secs(5);
        let before_jump = calibration.snapshot(900_000, age);
        let after_jump = calibration.snapshot(9_000_000, age);
        assert_eq!(before_jump.accurate_ts, after_jump.accurate_ts);
        assert_ne!(before_jump.offset_ms, after_jump.offset_ms);
    }

    #[test]
    fn implausible_server_time_samples_are_rejected() {
        let clock = super::AgentClock::default();
        let anchor = Instant::now();
        // 范围外绝对时间
        clock.update_server_clock(0, Duration::from_millis(80), anchor, "primary");
        clock.update_server_clock(i64::MAX, Duration::from_millis(80), anchor, "primary");
        assert!(clock.state.lock().unwrap().server.is_none());
        // 范围合法但 RTT 过大
        clock.update_server_clock(
            crate::model::now_millis(),
            Duration::from_secs(120),
            anchor,
            "primary",
        );
        assert!(clock.state.lock().unwrap().server.is_none());
        // 范围合法但与本机墙钟偏差超过 24h
        clock.update_server_clock(
            crate::model::now_millis().saturating_add(10 * 24 * 3600 * 1000),
            Duration::from_millis(80),
            anchor,
            "primary",
        );
        assert!(clock.state.lock().unwrap().server.is_none());
        // 合法样本接受
        clock.update_server_clock(
            crate::model::now_millis(),
            Duration::from_millis(80),
            anchor,
            "primary",
        );
        assert!(clock.state.lock().unwrap().server.is_some());
    }

    #[test]
    fn stale_calibration_does_not_fall_back_to_local_time() {
        let clock = super::AgentClock::default();
        let anchor = Instant::now();
        clock.state.lock().unwrap().ntp = Some(super::ClockCalibration {
            source: "ntp:test".into(),
            anchor,
            accurate_at_anchor: 1_000_000,
            round_trip_ms: 1,
        });
        // Simulate a stale sample with a virtual monotonic now, so the test
        // does not depend on the system having run past MAX_CALIBRATION_AGE.
        let virtual_now = anchor
            .checked_add(super::MAX_CALIBRATION_AGE)
            .and_then(|now| now.checked_add(Duration::from_secs(1)))
            .expect("virtual monotonic now must fit in Instant");
        let snapshot = clock.report_time_at(virtual_now);
        assert!(snapshot.accurate_ts.is_some());
        assert_eq!(snapshot.source.as_deref(), Some("ntp:test"));
        assert!(snapshot.sample_age_ms.unwrap() > super::MAX_CALIBRATION_AGE.as_millis() as u64);
    }

    #[test]
    fn ntp_timestamp_round_trip_preserves_unix_milliseconds() {
        let unix_millis = 1_754_300_060_123_i64;
        let encoded = unix_millis_to_ntp_timestamp(unix_millis);
        let reference = i128::from(unix_millis) * NANOS_PER_MILLI;
        let decoded = ntp_timestamp_to_unix_nanos(&encoded, reference).unwrap() / NANOS_PER_MILLI;
        assert!((decoded - i128::from(unix_millis)).abs() <= 1);
    }

    #[test]
    fn ntp_timestamp_resolves_era_after_2036_rollover() {
        let unix_millis = 2_208_988_800_456_i64; // 2040-01-01T00:00:00.456Z
        let encoded = unix_millis_to_ntp_timestamp(unix_millis);
        let reference = i128::from(unix_millis) * NANOS_PER_MILLI;
        let decoded = ntp_timestamp_to_unix_nanos(&encoded, reference).unwrap() / NANOS_PER_MILLI;
        assert!((decoded - i128::from(unix_millis)).abs() <= 1);
    }

    #[tokio::test]
    async fn ntp_client_uses_server_timestamps_instead_of_local_wall_clock() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut request = [0_u8; 48];
            let (_, peer) = socket.recv_from(&mut request).await.unwrap();
            let accurate = crate::model::now_millis().saturating_add(250);
            let stamp = unix_millis_to_ntp_timestamp(accurate);
            let mut response = [0_u8; 48];
            response[0] = 0x24; // leap=0, version=4, mode=4 (server)
            response[1] = 1;
            response[24..32].copy_from_slice(&request[40..48]);
            response[32..40].copy_from_slice(&stamp);
            response[40..48].copy_from_slice(&stamp);
            socket.send_to(&response, peer).await.unwrap();
        });

        let sample = super::query_ntp_address("local-test", address)
            .await
            .unwrap();
        server.await.unwrap();
        let observed_offset_ms = sample.offset_nanos / NANOS_PER_MILLI;
        assert!((200..=300).contains(&observed_offset_ms));
        assert_eq!(sample.server, "local-test");
    }

    #[tokio::test]
    async fn ntp_client_uses_a_reachable_resolved_address() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let reachable = socket.local_addr().unwrap();
        let unavailable = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap();
        let server = tokio::spawn(async move {
            let mut request = [0_u8; 48];
            let (_, peer) = socket.recv_from(&mut request).await.unwrap();
            let stamp = unix_millis_to_ntp_timestamp(crate::model::now_millis());
            let mut response = [0_u8; 48];
            response[0] = 0x24;
            response[1] = 1;
            response[24..32].copy_from_slice(&request[40..48]);
            response[32..40].copy_from_slice(&stamp);
            response[40..48].copy_from_slice(&stamp);
            socket.send_to(&response, peer).await.unwrap();
        });

        let sample = tokio::time::timeout(
            Duration::from_secs(1),
            super::query_ntp_addresses("local-test", vec![unavailable, reachable]),
        )
        .await
        .expect("reachable address should not wait for the unavailable address")
        .unwrap();
        server.await.unwrap();
        assert_eq!(sample.server, "local-test");
    }
}
