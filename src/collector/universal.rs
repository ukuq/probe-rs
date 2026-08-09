//! 非 Linux 平台实现（macOS/Windows）：sysinfo crate + netstat 解析
//!
//! 与 linux/ 模块保持相同 API，由 collector/mod.rs 按 cfg 选用。

use std::process::Command;

use sysinfo::{Disks, Networks, ProcessesToUpdate, System};

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
            cpu_usage: if first {
                None
            } else {
                p.map(|p| p.cpu_usage() as f64)
            },
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
pub fn disks() -> Vec<crate::model::DiskVolume> {
    let disks = Disks::new_with_refreshed_list();
    let mut devices: std::collections::HashMap<String, crate::model::DiskVolume> =
        Default::default();
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
        let mount_point = d.mount_point().to_string_lossy().to_string();
        let id = if cfg!(target_os = "windows") {
            mount_point.clone()
        } else {
            name.clone()
        };
        match devices.get(&id) {
            Some(existing) if existing.total >= total => {}
            _ => {
                devices.insert(
                    id.clone(),
                    crate::model::DiskVolume {
                        id,
                        name,
                        mount_point,
                        file_system: fs,
                        total,
                        used,
                    },
                );
            }
        }
    }
    let mut volumes: Vec<_> = devices.into_values().collect();
    volumes.sort_by(|a, b| a.id.cmp(&b.id));
    volumes
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
pub fn connections() -> Result<(u64, u64), String> {
    let out = Command::new("netstat")
        .args(["-an"])
        .output()
        .map_err(|e| format!("netstat -an 启动失败: {e}"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(format!(
            "netstat -an 退出失败（{}）: {}",
            out.status,
            stderr.trim()
        ));
    }
    Ok(parse_netstat(&String::from_utf8_lossy(&out.stdout)))
}

fn parse_netstat(text: &str) -> (u64, u64) {
    let mut tcp = 0u64;
    let mut udp = 0u64;
    for line in text.lines() {
        let line = line.trim_start().to_lowercase();
        if line.starts_with("tcp") {
            tcp += 1;
        } else if line.starts_with("udp") {
            udp += 1;
        }
    }
    (tcp, udp)
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
    cfg: &crate::model::StaticConfig,
) -> StaticInfo {
    let mut s = System::new();
    s.refresh_cpu_all();
    let (mem_total, _, swap_total, _) = memory();
    let disks = disks();
    let disk_total = disks.iter().map(|disk| disk.total).sum();
    StaticInfo {
        ts: crate::model::now_millis(),
        os: System::long_os_version()
            .or_else(System::name)
            .unwrap_or_else(|| std::env::consts::OS.to_string()),
        kernel: System::kernel_version().unwrap_or_default(),
        arch: {
            let a = System::cpu_arch();
            if a.is_empty() {
                std::env::consts::ARCH.to_string()
            } else {
                a
            }
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
        disks,
        gpu_name,
        // DMI 检测是 Linux 专属；macOS 物理机为主，Windows 一期不做虚拟化检测
        virtualization: None,
        boot_time: (System::boot_time() * 1000) as i64,
        ipv4,
        ipv6,
        agent_version: agent_version.to_string(),
        config: cfg.clone(),
    }
}

/// macOS：ioreg 读 IOBlockStorageDriver 的 Statistics 合计（异步 + 超时）；
/// Windows 一期不支持
#[cfg(target_os = "macos")]
pub async fn read_disk_io_counters() -> Option<super::DiskIoCounters> {
    let out = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::process::Command::new("ioreg")
            .args(["-rc", "IOBlockStorageDriver", "-d", "1"])
            .output(),
    )
    .await
    .ok()?
    .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    Some(parse_ioreg_statistics(&text))
}

#[cfg(target_os = "windows")]
pub async fn read_disk_io_counters() -> Option<super::DiskIoCounters> {
    super::windows_diskio::read_counters()
}

#[cfg(target_os = "macos")]
fn parse_ioreg_statistics(text: &str) -> super::DiskIoCounters {
    fn sum_key(text: &str, key: &str) -> u64 {
        let needle = format!("\"{key}\"=");
        let mut sum = 0u64;
        let mut rest = text;
        while let Some(i) = rest.find(&needle) {
            rest = &rest[i + needle.len()..];
            let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            sum += digits.parse::<u64>().unwrap_or(0);
        }
        sum
    }
    // Total Time 单位是纳秒，换算成 ms
    let total_ns = sum_key(text, "Total Time (Read)") + sum_key(text, "Total Time (Write)");
    super::DiskIoCounters {
        read_bytes: sum_key(text, "Bytes (Read)"),
        write_bytes: sum_key(text, "Bytes (Write)"),
        read_ops: sum_key(text, "Operations (Read)"),
        write_ops: sum_key(text, "Operations (Write)"),
        total_time_ms: total_ns / 1_000_000,
        io_ms_per_dev: Default::default(), // macOS 无 io 进行中时长计数器，usage 不可得
        devices: Default::default(),
    }
}

#[cfg(all(test, target_os = "macos"))]
mod diskio_tests {
    #[test]
    fn parses_ioreg_statistics() {
        let text = r#""Statistics" = {"Operations (Write)"=10,"Bytes (Read)"=2048,"Total Time (Read)"=5000000,"Total Time (Write)"=3000000,"Bytes (Write)"=1024,"Operations (Read)"=20}
    "Statistics" = {"Operations (Write)"=5,"Bytes (Read)"=512,"Total Time (Read)"=1000000,"Total Time (Write)"=0,"Bytes (Write)"=256,"Operations (Read)"=7}"#;
        let c = super::parse_ioreg_statistics(text);
        assert_eq!(c.read_bytes, 2560);
        assert_eq!(c.write_bytes, 1280);
        assert_eq!(c.read_ops, 27);
        assert_eq!(c.write_ops, 15);
        assert_eq!(c.total_time_ms, 9);
        assert!(c.io_ms_per_dev.is_empty());
    }
}

#[cfg(test)]
mod connection_tests {
    #[test]
    fn parses_windows_and_unix_netstat_rows() {
        let text = r#"
  Proto  Local Address          Foreign Address        State
  TCP    0.0.0.0:135            0.0.0.0:0              LISTENING
  TCP    [::]:445               [::]:0                 LISTENING
  UDP    0.0.0.0:500            *:*
tcp4       0      0  127.0.0.1.80          *.*                    LISTEN
udp6       0      0  *.5353                *.*
"#;
        assert_eq!(super::parse_netstat(text), (3, 2));
    }
}
