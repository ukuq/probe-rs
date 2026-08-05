//! 采集器平台门面
//!
//! - Linux：手写 /proc 解析（linux/ 目录），零依赖、行为可控
//! - 其他平台（macOS/Windows）：sysinfo crate 实现（universal.rs），连接数解析 netstat

pub mod net;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(target_os = "linux"))]
mod universal;

#[cfg(target_os = "linux")]
use linux as imp;
#[cfg(not(target_os = "linux"))]
use universal as imp;

use crate::model::StaticInfo;
use net::{IfaceFilter, NetBytes};

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

impl Default for SelfMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// CPU 采样器（有状态，差值法；首轮返回 None）
pub struct CpuMonitor(imp::CpuMonitor);

impl CpuMonitor {
    pub fn new() -> Self {
        Self(imp::CpuMonitor::new())
    }

    /// 百分比 0-100；首轮或计数器回退时为 None
    pub fn sample(&mut self) -> Option<f64> {
        self.0.sample()
    }
}

impl Default for CpuMonitor {
    fn default() -> Self {
        Self::new()
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

/// [load1, load5, load15]；不支持的平台为 None
pub fn load() -> Option<[f64; 3]> {
    imp::load()
}

pub fn processes() -> Option<u64> {
    imp::processes()
}

/// (tcp_conn, udp_conn)；TCP 全状态计数
pub fn connections() -> (u64, u64) {
    imp::connections()
}

/// 白名单网卡的合计计数器
pub fn net_bytes(filter: &IfaceFilter) -> NetBytes {
    imp::net_bytes(filter)
}

/// 遍历全部网卡（不过滤），netstatic 采样用
pub fn scan_net_dev(f: impl FnMut(&str, u64, u64)) {
    imp::scan_net_dev(f);
}

/// static 信息（os/kernel/cpu/虚拟化/boot_time/内存磁盘总量/当前配置等）
pub fn static_info(
    ipv4: Option<String>,
    ipv6: Option<String>,
    gpu_name: Option<String>,
    agent_version: &str,
    cfg: &crate::config::LocalConfig,
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
