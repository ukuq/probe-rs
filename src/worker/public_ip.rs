//! 公网 IP worker：查 cloudflare trace，绑定本地地址强制 v4/v6 分流
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

const TRACE_URL: &str = "https://cloudflare.com/cdn-cgi/trace";

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
                        buffers.push_error("ip", "cloudflare trace 查询失败（v4/v6 均不可达）");
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
    let body = client.get(TRACE_URL).send().await.ok()?.text().await.ok()?;
    for line in body.lines() {
        if let Some(ip) = line.strip_prefix("ip=") {
            return Some(IpMeasurement {
                address: ip.trim().to_string(),
                measured_at_ms: crate::model::now_millis(),
            });
        }
    }
    None
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
    fn successful_family_refreshes_while_failed_family_keeps_its_timestamp() {
        let mut snapshot = IpSnapshot::default();
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
