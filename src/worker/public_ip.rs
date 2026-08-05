//! 公网 IP worker：查 cloudflare trace，绑定本地地址强制 v4/v6 分流
//!
//! 公网 IP 是身份信息（"你是谁"），不是被测量的指标——因此喂 static、
//! 不进 async[]。它的变化会触发 static 重报（scheduler 监听快照变化）。
//! 故障域在外网（不通 ≠ agent 坏），失败时保留旧值（慢变量，陈旧代价可接受）。

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::interval;

use std::sync::Arc;

use crate::buffer::Buffers;
use crate::model::Intervals;

const TRACE_URL: &str = "https://cloudflare.com/cdn-cgi/trace";

pub type IpSnapshot = (Option<String>, Option<String>);

pub fn spawn(
    buffers: Arc<Buffers>,
    mut intervals_rx: watch::Receiver<Intervals>,
) -> (tokio::task::JoinHandle<()>, watch::Receiver<IpSnapshot>) {
    let (tx, rx) = watch::channel::<IpSnapshot>((None, None));
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
                    tx.send_if_modified(|cur| {
                        // 失败保留旧值
                        let new_v4 = ipv4.or_else(|| cur.0.clone());
                        let new_v6 = ipv6.or_else(|| cur.1.clone());
                        if new_v4 != cur.0 || new_v6 != cur.1 {
                            *cur = (new_v4, new_v6);
                            true
                        } else {
                            false
                        }
                    });
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

async fn query(client: &reqwest::Client) -> Option<String> {
    let body = client.get(TRACE_URL).send().await.ok()?.text().await.ok()?;
    for line in body.lines() {
        if let Some(ip) = line.strip_prefix("ip=") {
            return Some(ip.trim().to_string());
        }
    }
    None
}
