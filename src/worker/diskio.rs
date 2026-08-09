//! diskio worker：磁盘 IO 速率（read/write bps、iops、await、usage）。
//!
//! Linux 读 /proc/diskstats（微秒级，可高频）；macOS spawn ioreg（几十 ms，建议降频）。
//! 只发布最近一次快照；采集失败 = 快照不更新（ts 停滞），保留上一份有效数据并记 errors——
//! 与其他异步 worker 同规则。

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{interval, interval_at};

use crate::buffer::Buffers;
use crate::collector::{self, DiskIoCounters};
use crate::model::{DiskIoRecord, Intervals};

pub fn spawn(
    mut intervals_rx: watch::Receiver<Intervals>,
    buffers: Arc<Buffers>,
) -> (
    tokio::task::JoinHandle<()>,
    watch::Receiver<Option<DiskIoRecord>>,
) {
    let (tx, rx) = watch::channel::<Option<DiskIoRecord>>(None);
    let handle = tokio::spawn(async move {
        let mut prev: Option<(DiskIoCounters, i64)> = None;
        let mut ticker = interval(Duration::from_secs(intervals_rx.borrow().diskio.max(1)));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let ts = crate::model::now_millis();
                    match collector::disk_io_counters().await {
                        Some(cur) => {
                            let r = collector::disk_io_diff(prev, cur.clone(), ts);
                            prev = Some((cur, ts));
                            tx.send_replace(Some(DiskIoRecord {
                                ts,
                                read_bps: r.read_bps,
                                write_bps: r.write_bps,
                                read_iops: r.read_iops,
                                write_iops: r.write_iops,
                                await_ms: r.await_ms,
                                usage: r.usage,
                                disks: r.disks,
                            }));
                        }
                        None => {
                            // 失败：快照不动（ts 停滞语义），记错误事件（同源同文去重）
                            buffers.push_error("diskio", "磁盘 IO 采集失败（平台不支持或读取错误）");
                        }
                    }
                }
                r = intervals_rx.changed() => {
                    if r.is_err() { return; }
                    // interval_at：从下一个完整周期开始，避免重建后的立即 tick
                    // 产生毫秒级差值（diff 另有 <200ms 守卫兜底）
                    let d = Duration::from_secs(intervals_rx.borrow().diskio.max(1));
                    ticker = interval_at(tokio::time::Instant::now() + d, d);
                }
            }
        }
    });
    (handle, rx)
}
