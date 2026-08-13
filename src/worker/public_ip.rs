//! 公网 IP worker：多 provider 顺序回退，绑定本地地址强制 v4/v6 分流
//!
//! 公网 IP 是身份信息（"你是谁"），不是被测量的指标——因此喂 static、
//! 不进 async[]。它的变化会触发 static 重报（scheduler 监听快照变化）。
//! 故障域在外网（不通 ≠ agent 坏），失败时保留旧测量及其时间戳；Reporter
//! 会按采集/上报周期剔除过期值，不会无限复用。

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::interval;

use std::sync::Arc;

use crate::buffer::Buffers;
use crate::model::Intervals;

/// 按顺序回退；cloudflare trace 在某些网络不可达时仍有兜底。
/// api64/icanhazip 返回纯文本 IP，parser 兼容两种形态。
const PROVIDERS: &[&str] = &[
    "https://cloudflare.com/cdn-cgi/trace",
    "https://api64.ipify.org",
    "https://icanhazip.com",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpMeasurement {
    pub address: String,
    pub measured_at_ms: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IpSnapshot {
    pub ipv4: Option<IpMeasurement>,
    pub ipv6: Option<IpMeasurement>,
}

impl IpSnapshot {
    fn merge(&mut self, ipv4: Option<IpMeasurement>, ipv6: Option<IpMeasurement>) -> bool {
        let mut changed = false;
        if let Some(value) = ipv4 {
            changed |= self.ipv4.as_ref() != Some(&value);
            self.ipv4 = Some(value);
        }
        if let Some(value) = ipv6 {
            changed |= self.ipv6.as_ref() != Some(&value);
            self.ipv6 = Some(value);
        }
        changed
    }
}

pub fn spawn(
    buffers: Arc<Buffers>,
    mut intervals_rx: watch::Receiver<Intervals>,
) -> (tokio::task::JoinHandle<()>, watch::Receiver<IpSnapshot>) {
    let (tx, rx) = watch::channel(IpSnapshot::default());
    let handle = tokio::spawn(async move {
        let v4 = reqwest::Client::builder()
            .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
            .timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(0)
            .build();
        let v6 = reqwest::Client::builder()
            .local_address(IpAddr::V6(Ipv6Addr::UNSPECIFIED))
            .timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(0)
            .build();
        let mut ticker = interval(Duration::from_secs(intervals_rx.borrow().ip.max(1)));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let ipv4 = match &v4 { Ok(c) => query(c).await, Err(_) => None };
                    let ipv6 = match &v6 { Ok(c) => query(c).await, Err(_) => None };
                    if ipv4.is_none() && ipv6.is_none() {
                        buffers.push_error("ip", "公网 IP 查询失败（所有 provider 均不可达）");
                    }
                    tx.send_if_modified(|current| current.merge(ipv4, ipv6));
                }
                r = intervals_rx.changed() => {
                    if r.is_err() { return; }
                    ticker = interval(Duration::from_secs(intervals_rx.borrow().ip.max(1)));
                }
            }
        }
    });
    (handle, rx)
}

async fn query(client: &reqwest::Client) -> Option<IpMeasurement> {
    for url in PROVIDERS {
        if let Some(measurement) = query_one(client, url).await {
            return Some(measurement);
        }
    }
    None
}

async fn query_one(client: &reqwest::Client, url: &str) -> Option<IpMeasurement> {
    let body = client.get(url).send().await.ok()?.text().await.ok()?;
    let address = parse_ip_body(&body)?;
    Some(IpMeasurement {
        address,
        measured_at_ms: crate::model::now_millis(),
    })
}

/// 兼容 cloudflare trace 的 `ip=` 行与纯文本 IP 两种响应。
fn parse_ip_body(body: &str) -> Option<String> {
    for line in body.lines() {
        if let Some(ip) = line.strip_prefix("ip=") {
            let ip = ip.trim();
            return ip.parse::<IpAddr>().ok().map(|_| ip.to_string());
        }
    }
    let ip = body.trim();
    ip.parse::<IpAddr>().ok().map(|_| ip.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn measurement(address: &str, measured_at_ms: i64) -> IpMeasurement {
        IpMeasurement {
            address: address.into(),
            measured_at_ms,
        }
    }

    #[test]
    fn parses_trace_and_plain_text_bodies() {
        let trace = "fl=123f45\nh=cloudflare.com\nip=203.0.113.7\nts=1786.1\n";
        assert_eq!(parse_ip_body(trace).as_deref(), Some("203.0.113.7"));
        assert_eq!(parse_ip_body(" 2001:db8::1 \n").as_deref(), Some("2001:db8::1"));
        assert_eq!(parse_ip_body("192.0.2.9").as_deref(), Some("192.0.2.9"));
        assert_eq!(parse_ip_body("not an ip"), None);
        assert_eq!(parse_ip_body(""), None);
    }

    #[test]
    fn successful_family_refreshes_while_failed_family_keeps_its_timestamp() {        let mut snapshot = IpSnapshot::default();
        assert!(snapshot.merge(Some(measurement("192.0.2.1", 10)), None));
        assert!(!snapshot.merge(None, None));
        assert_eq!(snapshot.ipv4.as_ref().unwrap().measured_at_ms, 10);

        assert!(snapshot.merge(
            Some(measurement("192.0.2.1", 20)),
            Some(measurement("2001:db8::1", 21)),
        ));
        assert_eq!(snapshot.ipv4.as_ref().unwrap().measured_at_ms, 20);
        assert_eq!(snapshot.ipv6.as_ref().unwrap().measured_at_ms, 21);

        assert!(snapshot.merge(None, Some(measurement("2001:db8::1", 30))));
        assert_eq!(snapshot.ipv4.as_ref().unwrap().measured_at_ms, 20);
        assert_eq!(snapshot.ipv6.as_ref().unwrap().measured_at_ms, 30);
    }
}
