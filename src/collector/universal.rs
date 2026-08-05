//! 非 Linux 平台实现（macOS/Windows）：sysinfo crate + netstat 解析
//!
//! 与 linux/ 模块保持相同 API，由 collector/mod.rs 按 cfg 选用。

use std::process::Command;

use sysinfo::{Disks, Networks, ProcessesToUpdate, System};

use crate::collector::net::{IfaceFilter, NetBytes};
use crate::model::StaticInfo;

#[derive(Debug, Clone, Copy)]
pub struct SelfStats {
    pub cpu_usage: Option<f64>,
    pub mem_rss: Option<u64>,
}

pub struct SelfMonitor {
    system: System,
    pid: sysinfo::Pid,
    sampled_once: bool,
}

impl SelfMonitor {
    pub fn new() -> Self {
        Self {
            system: System::new(),
            pid: sysinfo::Pid::from_u32(std::process::id()),
            sampled_once: false,
        }
    }

    pub fn sample(&mut self) -> SelfStats {
        self.system
            .refresh_processes(ProcessesToUpdate::Some(&[self.pid]), true);
        let first = !self.sampled_once;
        self.sampled_once = true;
        let p = self.system.process(self.pid);
        SelfStats {
            cpu_usage: if first { None } else { p.map(|p| p.cpu_usage() as f64) },
            mem_rss: p.map(|p| p.memory()),
        }
    }
}

pub struct CpuMonitor {
    system: System,
    sampled_once: bool,
}

impl CpuMonitor {
    pub fn new() -> Self {
        Self {
            system: System::new(),
            sampled_once: false,
        }
    }

    pub fn sample(&mut self) -> Option<f64> {
        self.system.refresh_cpu_usage();
        if !self.sampled_once {
            // sysinfo 需要两次刷新才有差值，首轮与 Linux 行为对齐返回 None
            self.sampled_once = true;
            return None;
        }
        Some(self.system.global_cpu_usage() as f64)
    }
}

/// (mem_total, mem_used, swap_total, swap_used)，字节
pub fn memory() -> (u64, u64, u64, u64) {
    let mut s = System::new();
    s.refresh_memory();
    (
        s.total_memory(),
        s.used_memory(),
        s.total_swap(),
        s.used_swap(),
    )
}

/// 排除的虚拟/网络文件系统（小写包含匹配）
const EXCLUDED_FS: &[&str] = &[
    "devfs", "map", "autofs", "tmpfs", "fuse", "nfs", "smbfs", "cifs", "mqueue", "proc",
];

/// (disk_total, disk_used)，字节
pub fn disk() -> (u64, u64) {
    let disks = Disks::new_with_refreshed_list();
    let mut devices: std::collections::HashMap<String, (u64, u64)> = Default::default();
    for d in &disks {
        let mount = d.mount_point().to_string_lossy().to_lowercase();
        let fs = d.file_system().to_string_lossy().to_lowercase();
        if !include_mount(&mount, &fs) {
            continue;
        }
        let total = d.total_space();
        let used = total.saturating_sub(d.available_space());
        if total == 0 {
            continue;
        }
        let name = d.name().to_string_lossy().to_string();
        match devices.get(&name) {
            Some(&(t, _)) if t >= total => {}
            _ => {
                devices.insert(name, (total, used));
            }
        }
    }
    devices
        .values()
        .fold((0u64, 0u64), |(t, u), &(dt, du)| (t + dt, u + du))
}

#[cfg(target_os = "macos")]
fn include_mount(mount: &str, fs: &str) -> bool {
    if mount != "/" && !mount.starts_with("/volumes/") {
        return false;
    }
    !EXCLUDED_FS.iter().any(|x| fs.contains(x))
}

#[cfg(target_os = "windows")]
fn include_mount(_mount: &str, fs: &str) -> bool {
    !EXCLUDED_FS.iter().any(|x| fs.contains(x))
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
fn include_mount(mount: &str, fs: &str) -> bool {
    if mount != "/" {
        return false;
    }
    !EXCLUDED_FS.iter().any(|x| fs.contains(x))
}

pub fn load() -> Option<[f64; 3]> {
    let l = System::load_average();
    Some([l.one, l.five, l.fifteen])
}

pub fn processes() -> Option<u64> {
    let mut s = System::new();
    s.refresh_processes(ProcessesToUpdate::All, true);
    Some(s.processes().len() as u64)
}

/// 解析 netstat -an：TCP 全状态计数（行首 tcp/TCP），UDP 同理
pub fn connections() -> (u64, u64) {
    let Ok(out) = Command::new("netstat").args(["-an"]).output() else {
        return (0, 0);
    };
    let mut tcp = 0u64;
    let mut udp = 0u64;
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let line = line.trim_start().to_lowercase();
        if line.starts_with("tcp") {
            tcp += 1;
        } else if line.starts_with("udp") {
            udp += 1;
        }
    }
    (tcp, udp)
}

pub fn net_bytes(filter: &IfaceFilter) -> NetBytes {
    let mut total = NetBytes::default();
    scan_net_dev(|name, rx, tx| {
        if filter.includes(name) {
            total.rx += rx;
            total.tx += tx;
        }
    });
    total
}

pub fn scan_net_dev(mut f: impl FnMut(&str, u64, u64)) {
    let networks = Networks::new_with_refreshed_list();
    for (name, data) in &networks {
        f(name, data.total_received(), data.total_transmitted());
    }
}

pub fn static_info(
    ipv4: Option<String>,
    ipv6: Option<String>,
    gpu_name: Option<String>,
    agent_version: &str,
    cfg: &crate::config::LocalConfig,
) -> StaticInfo {
    let mut s = System::new();
    s.refresh_cpu_all();
    let (mem_total, _, swap_total, _) = memory();
    let (disk_total, _) = disk();
    StaticInfo {
        ts: crate::model::now_millis(),
        os: System::long_os_version()
            .or_else(System::name)
            .unwrap_or_else(|| std::env::consts::OS.to_string()),
        kernel: System::kernel_version().unwrap_or_default(),
        arch: {
            let a = System::cpu_arch();
            if a.is_empty() { std::env::consts::ARCH.to_string() } else { a }
        },
        cpu_name: s
            .cpus()
            .first()
            .map(|c| c.brand().trim().to_string())
            .filter(|b| !b.is_empty())
            .unwrap_or_else(|| std::env::consts::ARCH.to_string()),
        cpu_cores: std::thread::available_parallelism().map_or(1, |n| n.get() as u32),
        cpu_physical_cores: s.physical_core_count().map(|n| n as u32),
        mem_total,
        swap_total,
        disk_total,
        gpu_name,
        // DMI 检测是 Linux 专属；macOS 物理机为主，Windows 一期不做虚拟化检测
        virtualization: None,
        boot_time: (System::boot_time() * 1000) as i64,
        ipv4,
        ipv6,
        agent_version: agent_version.to_string(),
        config: crate::model::StaticConfig {
            intervals: cfg.intervals,
            reset_day: cfg.reset_day,
            interfaces: cfg.interfaces.clone(),
            enable_gpu: cfg.enable_gpu,
            report_errors: cfg.report_errors,
            report_self: cfg.report_self,
            pings: cfg.pings.clone(),
        },
    }
}
