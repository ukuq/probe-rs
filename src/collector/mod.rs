//! 采集器平台门面
//!
//! - Linux：手写 /proc 解析（linux/ 目录），零依赖、行为可控
//! - 其他平台（macOS/Windows）：sysinfo crate 实现（universal.rs），连接数解析 netstat

pub mod net;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(target_os = "linux"))]
mod universal;
#[cfg(target_os = "windows")]
mod windows_diskio;

#[cfg(target_os = "linux")]
use linux as imp;
#[cfg(not(target_os = "linux"))]
use universal as imp;

use crate::model::StaticInfo;

/// 探针自身资源采样器（有状态，CPU 差值法；首轮 cpu 为 None）
pub struct SelfMonitor(imp::SelfMonitor);

impl SelfMonitor {
    pub fn new() -> Self {
        Self(imp::SelfMonitor::new())
    }
    pub fn sample(&mut self) -> imp::SelfStats {
        self.0.sample()
    }
}

/// CPU 采样器（有状态，差值法；首轮返回 None）
pub struct CpuMonitor(imp::CpuMonitor);

impl CpuMonitor {
    pub fn new() -> Self {
        Self(imp::CpuMonitor::new())
    }
    pub fn sample(&mut self) -> Option<f64> {
        self.0.sample()
    }
}

/// (mem_total, mem_used, swap_total, swap_used)，字节
pub fn memory() -> (u64, u64, u64, u64) {
    imp::memory()
}

/// (disk_total, disk_used)，字节
pub fn disk() -> (u64, u64) {
    imp::disk()
}

/// [load1, load5, load15]；Windows 等为 None
pub fn load() -> Option<[f64; 3]> {
    imp::load()
}

/// 进程数；不可得为 None
pub fn processes() -> Option<u64> {
    imp::processes()
}

/// (tcp_conn, udp_conn)；失败返回错误，避免把采集故障误报为 0
pub fn connections() -> Result<(u64, u64), String> {
    imp::connections()
}

/// 白名单网卡合计 (rx, tx)，字节
/// 遍历全部网卡（不过滤），netstatic 采样用
pub fn scan_net_dev(f: impl FnMut(&str, u64, u64)) {
    imp::scan_net_dev(f);
}

/// 磁盘 IO 累计计数器（整盘合计，开机起）
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DiskIoCounters {
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub read_ops: u64,
    pub write_ops: u64,
    /// 读+写总耗时（ms），算 await 用
    pub total_time_ms: u64,
    /// 各整盘"io 进行中"总时长（ms）：usage = 各盘差值/间隔的最大值（多盘并行不累加）。
    /// 无此信息的平台（macOS）为空 map → usage 恒 None
    pub io_ms_per_dev: std::collections::HashMap<String, u64>,
}

/// 磁盘 IO 速率（disk_io_diff 的输出，1:1 对应 DiskIoRecord 各指标字段）
#[derive(Debug, Clone, Copy, Default)]
pub struct DiskIoRates {
    pub read_bps: Option<f64>,
    pub write_bps: Option<f64>,
    pub read_iops: Option<f64>,
    pub write_iops: Option<f64>,
    pub await_ms: Option<f64>,
    pub usage: Option<f64>,
}

/// 磁盘 IO 累计计数器（整盘合计）；不支持的平台返回 None
pub async fn disk_io_counters() -> Option<DiskIoCounters> {
    imp::read_disk_io_counters().await
}

/// 磁盘 IO 差值计算（纯函数，各平台共用）。首轮/间隔过短（<200ms，如 ticker
/// 重建后的立即 tick）返回全 None——避免把毫秒级差值放大成幻影尖峰
pub fn disk_io_diff(
    prev: Option<(DiskIoCounters, i64)>,
    cur: DiskIoCounters,
    now_ms: i64,
) -> DiskIoRates {
    let none = DiskIoRates::default();
    let Some((p, pts)) = prev else {
        return none;
    };
    let dt = (now_ms - pts) as f64 / 1000.0;
    if dt < 0.2 {
        return none;
    }
    let dr = cur.read_bytes.saturating_sub(p.read_bytes) as f64;
    let dw = cur.write_bytes.saturating_sub(p.write_bytes) as f64;
    let dro = cur.read_ops.saturating_sub(p.read_ops) as f64;
    let dwo = cur.write_ops.saturating_sub(p.write_ops) as f64;
    let dt_time = cur.total_time_ms.saturating_sub(p.total_time_ms) as f64;
    let ops = dro + dwo;
    let await_ms = Some(if ops > 0.0 { dt_time / ops } else { 0.0 });
    // usage = 各盘 Δio_ms/dt 的最大值（多盘并行不叠加；单盘打满才是 100%）
    let usage = if cur.io_ms_per_dev.is_empty() {
        None
    } else {
        let dt_ms = dt * 1000.0;
        cur.io_ms_per_dev
            .iter()
            .filter_map(|(dev, v)| p.io_ms_per_dev.get(dev).map(|pv| (dev, v, pv)))
            .map(|(_, v, pv)| (v.saturating_sub(*pv) as f64 / dt_ms * 100.0).min(100.0))
            .reduce(f64::max)
    };
    DiskIoRates {
        read_bps: Some(dr / dt),
        write_bps: Some(dw / dt),
        read_iops: Some(dro / dt),
        write_iops: Some(dwo / dt),
        await_ms,
        usage,
    }
}

/// static 信息（os/kernel/cpu/虚拟化/boot_time/内存磁盘总量/当前配置等）
pub fn static_info(
    ipv4: Option<String>,
    ipv6: Option<String>,
    gpu_name: Option<String>,
    agent_version: &str,
    cfg: &crate::model::StaticConfig,
) -> StaticInfo {
    imp::static_info(ipv4, ipv6, gpu_name, agent_version, cfg)
}

#[cfg(target_os = "linux")]
pub fn scan_file(path: &str, mut f: impl FnMut(&str)) -> std::io::Result<()> {
    let data = std::fs::read_to_string(path)?;
    for line in data.lines() {
        f(line);
    }
    Ok(())
}
