//! slow worker：每台机器都有的系统慢指标（disk_used/连接数/进程数）
//! + 探针自身资源占用（report_self=true 时同节奏产出 kind:"self" 快照）
//!
//! 与 dynamic 的 cpu/mem 同族，但变化慢、采集贵（statfs 遍历、连接表扫描），
//! 不配跟 collect tick 每秒跑，独立 worker 按 intervals.slow 节奏出 kind:"slow" 记录。
//! 只发布最近一次测量的快照；采集端按 ts 新鲜度决定是否摘取——
//! 同一份数据不会重复进入缓冲，ts 永远是真实测量时刻。

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::interval;

use crate::buffer::Buffers;
use crate::collector::{self, SelfMonitor};
use crate::model::{Intervals, SelfRecord, SlowBlock};

#[allow(clippy::type_complexity)]
pub fn spawn(
    mut intervals_rx: watch::Receiver<Intervals>,
    buffers: Arc<Buffers>,
) -> (
    tokio::task::JoinHandle<()>,
    watch::Receiver<Option<SlowBlock>>,
    watch::Receiver<Option<SelfRecord>>,
) {
    let (slow_tx, slow_rx) = watch::channel::<Option<SlowBlock>>(None);
    let (self_tx, self_rx) = watch::channel::<Option<SelfRecord>>(None);
    let handle = tokio::spawn(async move {
        let mut self_monitor = SelfMonitor::new();
        let mut ticker = interval(Duration::from_secs(intervals_rx.borrow().slow.max(1)));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let ts = crate::model::now_millis();
                    let (_, disk_used) = collector::disk();
                    let (tcp_conn, udp_conn) = match collector::connections() {
                        Ok((tcp, udp)) => (Some(tcp), Some(udp)),
                        Err(e) => {
                            tracing::warn!(error = %e, "连接数采集失败");
                            buffers.push_error("connections", e);
                            (None, None)
                        }
                    };
                    let processes = collector::processes();
                    slow_tx.send_replace(Some(SlowBlock {
                        ts,
                        disk_used: Some(disk_used),
                        tcp_conn,
                        udp_conn,
                        processes,
                    }));
                    // 自身占用与 slow 同频实际采集；各 Reporter 仅决定是否输出。
                    let stats = self_monitor.sample();
                    self_tx.send_replace(Some(SelfRecord {
                        ts,
                        cpu_usage: stats.cpu_usage,
                        mem_rss: stats.mem_rss,
                    }));
                    tracing::debug!(ts, "slow 快照已更新");
                }
                r = intervals_rx.changed() => {
                    if r.is_err() { return; }
                    ticker = interval(Duration::from_secs(intervals_rx.borrow().slow.max(1)));
                }
            }
        }
    });
    (handle, slow_rx, self_rx)
}
