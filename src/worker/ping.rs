//! 网络探测 worker：每组（key/url/interval）独立 task、独立节奏
//!
//! target 推断类型：http(s):// 开头 → HTTP；否则 TCP（host[:port]，默认 80）。
//! 快照为 HashMap<key, PingRecord>，每组各自 ts；采集端按 key 新鲜度摘取。
//! icmp 一期不支持（需 root/CAP_NET_RAW）。

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use tokio::sync::watch;
use tokio::time::interval;

use std::sync::Arc as StdArc;

use crate::buffer::Buffers;
use crate::model::{Intervals, PingRecord, PingTarget};

const PING_TIMEOUT: Duration = Duration::from_secs(3);
const HIGH_LATENCY_MS: i64 = 1000;
const HIGH_LATENCY_RETRIES: u32 = 3;
/// TCP 重测降幅超过该值判定为 SYN 重传污染
const RETRY_DROP_TCP_MS: i64 = 800;
const PING_COUNT: u32 = 4;
const DEFAULT_TCP_PORT: u16 = 80;

pub type PingSnapshot = HashMap<String, PingRecord>;

/// ping worker：每组一个 task，可整体中止（配置热加载时重建）
pub struct PingWorker {
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl PingWorker {
    /// tx 由调用方持有：worker 可销毁重建，scheduler 侧的 rx 不受影响
    pub fn start(
        targets: Vec<PingTarget>,
        tx: watch::Sender<PingSnapshot>,
        buffers: StdArc<Buffers>,
        intervals_rx: watch::Receiver<Intervals>,
    ) -> Self {
        let handles = targets
            .into_iter()
            .map(|t| {
                tokio::spawn(target_loop(
                    t,
                    tx.clone(),
                    buffers.clone(),
                    intervals_rx.clone(),
                ))
            })
            .collect();
        Self { handles }
    }

    pub fn stop(self) {
        for h in self.handles {
            h.abort();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PingKind {
    Tcp,
    Http,
}

fn kind_of(target: &str) -> PingKind {
    if target.starts_with("http://") || target.starts_with("https://") {
        PingKind::Http
    } else {
        PingKind::Tcp
    }
}

/// 单目标循环：interval 缺省时跟随全局 intervals.ping（远端可调）
async fn target_loop(
    target: PingTarget,
    tx: watch::Sender<PingSnapshot>,
    buffers: StdArc<Buffers>,
    mut intervals_rx: watch::Receiver<Intervals>,
) {
    let kind = kind_of(&target.target);
    let effective = |i: &Intervals| target.interval.unwrap_or(i.ping).max(1);
    let mut ticker = interval(Duration::from_secs(effective(&intervals_rx.borrow())));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let ts = crate::model::now_millis();
                let (rtt, loss, err) = ping_one(&target, kind).await;
                let name = target.name.clone();
                if rtt < 0 {
                    buffers.push_error(
                        format!("ping:{}", name),
                        err.unwrap_or_else(|| "全部测量失败".to_string()),
                    );
                }
                tx.send_modify(|m| {
                    m.insert(name.clone(), PingRecord { ts, name, rtt, loss });
                });
            }
            r = intervals_rx.changed() => {
                if r.is_err() { return; }
                if target.interval.is_none() {
                    ticker = interval(Duration::from_secs(effective(&intervals_rx.borrow())));
                }
            }
        }
    }
}

/// 单目标一轮：4 次测量（每次带防重传重试），中位数 + 丢包率 + 首个错误文本
async fn ping_one(target: &PingTarget, kind: PingKind) -> (i64, u32, Option<String>) {
    let mut values: Vec<i64> = Vec::with_capacity(PING_COUNT as usize);
    let mut first_err: Option<String> = None;
    for _ in 0..PING_COUNT {
        match measure_with_retry(&target.target, kind).await {
            Ok(ms) => values.push(ms),
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e.to_string());
                }
                tracing::debug!(target = %target.target, error = %e, "探测单次失败");
            }
        }
    }
    let (rtt, loss) = build_result(PING_COUNT, &mut values);
    (rtt, loss, first_err)
}

fn build_result(count: u32, values: &mut [i64]) -> (i64, u32) {
    let count = count.max(1);
    if values.is_empty() {
        return (-1, 100);
    }
    values.sort_unstable();
    let mid = values.len() / 2;
    let median = if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2
    };
    let loss = (count as usize - values.len()) as u32 * 100 / count;
    (median, loss)
}

/// 防重传：首次 >1000ms 时重测最多 3 次；TCP 降幅 >800ms 判失败
async fn measure_with_retry(target: &str, kind: PingKind) -> Result<i64> {
    let first = measure(target, kind).await?;
    if first <= HIGH_LATENCY_MS {
        return Ok(first);
    }
    for i in 0..HIGH_LATENCY_RETRIES {
        let second = measure(target, kind).await?;
        if second <= HIGH_LATENCY_MS {
            if kind == PingKind::Tcp && first - second > RETRY_DROP_TCP_MS {
                bail!("suspicious retransmission detected in tcp handshake");
            }
            return Ok(second);
        }
        if i == HIGH_LATENCY_RETRIES - 1 {
            bail!("latency remains high after retries");
        }
    }
    Ok(first)
}

async fn measure(target: &str, kind: PingKind) -> Result<i64> {
    match kind {
        PingKind::Tcp => tcp_ping(target).await,
        PingKind::Http => http_ping(target).await,
    }
}

fn split_host_port(target: &str) -> Result<(String, u16)> {
    let target = target.trim();
    if target.is_empty() {
        bail!("empty target");
    }
    // [v6]:port 标准写法
    if let Some(rest) = target.strip_prefix('[') {
        if let Some((host, port)) = rest.split_once("]:") {
            let port = port.parse::<u16>().ok().filter(|p| *p >= 1);
            return match (host.is_empty(), port) {
                (false, Some(port)) => Ok((host.to_string(), port)),
                _ => bail!("非法 target: {target}"),
            };
        }
        // [v6] 无端口
        if let Some(host) = rest.strip_suffix(']') {
            if host.is_empty() {
                bail!("非法 target: {target}");
            }
            return Ok((host.to_string(), DEFAULT_TCP_PORT));
        }
        bail!("非法 target: {target}");
    }
    if let Some((host, port)) = target.rsplit_once(':') {
        if !host.is_empty()
            && host
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
        {
            if let Ok(port) = port.parse::<u16>() {
                if port >= 1 {
                    return Ok((host.to_string(), port));
                }
            }
        }
    }
    Ok((target.to_string(), DEFAULT_TCP_PORT))
}

async fn tcp_ping(target: &str) -> Result<i64> {
    let (host, port) = split_host_port(target)?;
    let addr = tokio::time::timeout(PING_TIMEOUT, tokio::net::lookup_host((host.as_str(), port)))
        .await??
        .next()
        .ok_or_else(|| anyhow::anyhow!("no address"))?;
    let start = Instant::now();
    let stream = tokio::time::timeout(PING_TIMEOUT, tokio::net::TcpStream::connect(addr)).await??;
    drop(stream);
    Ok((start.elapsed().as_millis() as i64).max(1))
}

async fn http_ping(target: &str) -> Result<i64> {
    let client = reqwest::Client::builder()
        .timeout(PING_TIMEOUT)
        .pool_max_idle_per_host(0)
        .build()?;
    let start = Instant::now();
    let resp = client.get(target).send().await?;
    let ms = (start.elapsed().as_millis() as i64).max(1);
    if resp.status().is_success() || resp.status().is_redirection() {
        Ok(ms)
    } else {
        bail!("http status {}", resp.status())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_odd_even() {
        assert_eq!(build_result(4, &mut [30, 10, 20, 40]).0, 25);
        assert_eq!(build_result(3, &mut [30, 10, 20]).0, 20);
    }

    #[test]
    fn loss_calc() {
        assert_eq!(build_result(4, &mut [10, 20]).1, 50);
        assert_eq!(build_result(4, &mut []).0, -1);
        assert_eq!(build_result(4, &mut []).1, 100);
    }

    #[test]
    fn host_port_split() {
        assert_eq!(
            split_host_port("1.2.3.4").unwrap(),
            ("1.2.3.4".to_string(), 80)
        );
        assert_eq!(
            split_host_port("1.2.3.4:8080").unwrap(),
            ("1.2.3.4".to_string(), 8080)
        );
        assert_eq!(
            split_host_port("example.com:443").unwrap(),
            ("example.com".to_string(), 443)
        );
        // IPv6 标准写法
        assert_eq!(
            split_host_port("[::1]:8080").unwrap(),
            ("::1".to_string(), 8080)
        );
        assert_eq!(
            split_host_port("[2001:db8::1]").unwrap(),
            ("2001:db8::1".to_string(), 80)
        );
        assert!(split_host_port("[::1]").is_ok());
        assert!(split_host_port("[::1]:abc").is_err());
    }

    #[test]
    fn kind_inference() {
        assert_eq!(kind_of("https://example.com"), PingKind::Http);
        assert_eq!(kind_of("http://1.2.3.4:8080/health"), PingKind::Http);
        assert_eq!(kind_of("1.2.3.4:80"), PingKind::Tcp);
        assert_eq!(kind_of("example.com"), PingKind::Tcp);
    }
}
