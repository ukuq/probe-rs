//! Linux 平台实现：手写 /proc 解析

pub mod conn;
pub mod cpu;
pub mod disk;
pub mod diskio;
pub mod load;
pub mod mem;
pub mod process;
pub mod selfproc;
pub mod sysinfo;

pub use super::scan_file;

use crate::model::StaticInfo;

pub use selfproc::{SelfMonitor, SelfStats};

/// Linux：读 /proc/diskstats（同步快读，包成 async 对齐门面签名）
pub async fn read_disk_io_counters() -> Option<super::DiskIoCounters> {
    diskio::read_counters()
}

pub struct CpuMonitor {
    prev: Option<cpu::CpuTimes>,
}

impl CpuMonitor {
    pub fn new() -> Self {
        Self { prev: None }
    }

    pub fn sample(&mut self) -> Option<f64> {
        let current = cpu::read_cpu_times()?;
        let usage = self.prev.and_then(|prev| cpu::usage_percent(prev, current));
        self.prev = Some(current);
        usage
    }
}

/// (mem_total, mem_used, swap_total, swap_used)，字节
pub fn memory() -> (u64, u64, u64, u64) {
    mem::collect()
}

pub fn disks() -> Vec<crate::model::DiskVolume> {
    disk::collect_volumes()
}

pub fn load() -> Option<[f64; 3]> {
    load::collect()
}

pub fn processes() -> Option<u64> {
    process::collect()
}

pub fn connections() -> Result<(u64, u64), String> {
    Ok(conn::collect())
}

pub fn scan_net_dev(f: impl FnMut(&str, u64, u64)) {
    crate::collector::net::scan_net_dev(f);
}

pub fn static_info(
    ipv4: Option<String>,
    ipv6: Option<String>,
    gpu_name: Option<String>,
    agent_version: &str,
    cfg: &crate::model::StaticConfig,
) -> StaticInfo {
    sysinfo::collect(ipv4, ipv6, gpu_name, agent_version, cfg)
}
