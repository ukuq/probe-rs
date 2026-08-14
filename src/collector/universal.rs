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
    cores: f64,
}

impl SelfMonitor {
    pub fn new() -> Self {
        Self {
            system: System::new(),
            pid: sysinfo::Pid::from_u32(std::process::id()),
            sampled_once: false,
            cores: std::thread::available_parallelism().map_or(1.0, |n| n.get() as f64),
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
                // sysinfo 的进程 CPU 未按核数归一（多线程可 >100）；契约是
                // 整机容量百分比 0-100，这里除以核数对齐 Linux 实现。
                p.map(|p| p.cpu_usage() as f64 / self.cores)
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

/// (mem_total, mem_used, swap_total, swap_used)，字节。
/// sysinfo 路径不会失败,但为与 Linux 门面对齐(读失败 → None → 线上 null),
/// 保持 Option 签名。
pub fn memory() -> Option<(u64, u64, u64, u64)> {
    let mut s = System::new();
    s.refresh_memory();
    Some((
        s.total_memory(),
        s.used_memory(),
        s.total_swap(),
        s.used_swap(),
    ))
}

/// 排除的虚拟/网络文件系统（小写包含匹配）
const EXCLUDED_FS: &[&str] = &[
    "devfs", "map", "autofs", "tmpfs", "fuse", "nfs", "smbfs", "cifs", "mqueue", "proc",
];

/// (disk_total, disk_used)，字节
pub fn disks() -> Vec<crate::model::DiskVolume> {
    let disks = Disks::new_with_refreshed_list();
    let mut volumes = Vec::new();
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
        volumes.push(crate::model::DiskVolume {
            id,
            name,
            mount_point,
            file_system: fs,
            total,
            used,
        });
    }

    // macOS APFS 的根快照和 Data 卷共享空间。保留根卷 id/name/"/" 作为
    // Reporter 筛选别名，只用 Data 卷的容量替换其 total/used，然后移除
    // Data 记录，既不会双计，也不会让已有 disks=["/"] 配置升级后失效。
    merge_apfs_data_volumes(&mut volumes);

    let mut devices: std::collections::HashMap<String, crate::model::DiskVolume> =
        Default::default();
    for volume in volumes {
        match devices.get(&volume.id) {
            Some(existing) if existing.total >= volume.total => {}
            _ => {
                devices.insert(volume.id.clone(), volume);
            }
        }
    }
    let mut volumes: Vec<_> = devices.into_values().collect();
    volumes.sort_by(|a, b| a.id.cmp(&b.id));
    volumes
}

fn apfs_data_matches_root(data_name: &str, root_name: &str) -> bool {
    data_name == root_name || data_name.strip_suffix(" - Data") == Some(root_name)
}

fn merge_apfs_data_volumes(volumes: &mut Vec<crate::model::DiskVolume>) {
    let data_indices: Vec<_> = volumes
        .iter()
        .enumerate()
        .filter(|(_, volume)| {
            volume.file_system.eq_ignore_ascii_case("apfs")
                && volume
                    .mount_point
                    .eq_ignore_ascii_case("/System/Volumes/Data")
        })
        .map(|(index, _)| index)
        .collect();
    let mut remove = std::collections::BTreeSet::new();
    for data_index in data_indices {
        let data_name = volumes[data_index].name.clone();
        let root_index = volumes.iter().enumerate().find_map(|(index, volume)| {
            (index != data_index
                && volume.file_system.eq_ignore_ascii_case("apfs")
                && volume.mount_point == "/"
                && apfs_data_matches_root(&data_name, &volume.name))
            .then_some(index)
        });
        if let Some(root_index) = root_index {
            volumes[root_index].total = volumes[data_index].total;
            volumes[root_index].used = volumes[data_index].used;
            remove.insert(data_index);
        }
    }
    if !remove.is_empty() {
        *volumes = volumes
            .drain(..)
            .enumerate()
            .filter_map(|(index, volume)| (!remove.contains(&index)).then_some(volume))
            .collect();
    }
}

#[cfg(target_os = "macos")]
fn include_mount(mount: &str, fs: &str) -> bool {
    // 系统根快照、APFS 数据卷(真实用户数据占用)、外接卷。
    // 注意:纳入 Data 卷后由 disks() 的卷组去重防止与根快照双计。
    let accepted =
        mount == "/" || mount.starts_with("/volumes/") || mount == "/system/volumes/data";
    accepted && !EXCLUDED_FS.iter().any(|x| fs.contains(x))
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
    let (mem_total, _, swap_total, _) = memory().unwrap_or((0, 0, 0, 0));
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
        // 采集失败(0)必须表达为 null，否则 uptime 会被算成"自 1970 起"。
        boot_time: {
            let boot_seconds = System::boot_time();
            (boot_seconds > 0).then_some((boot_seconds * 1000) as i64)
        },
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

    #[test]
    fn apfs_data_capacity_replaces_root_without_losing_root_aliases() {
        let volume =
            |id: &str, name: &str, mount: &str, total: u64, used: u64| crate::model::DiskVolume {
                id: id.into(),
                name: name.into(),
                mount_point: mount.into(),
                file_system: "apfs".into(),
                total,
                used,
            };
        let mut volumes = vec![
            volume("Macintosh HD", "Macintosh HD", "/", 100, 10),
            volume(
                "Macintosh HD - Data",
                "Macintosh HD - Data",
                "/System/Volumes/Data",
                100,
                70,
            ),
            volume("Backup", "Backup", "/Volumes/Backup", 200, 20),
        ];

        super::merge_apfs_data_volumes(&mut volumes);

        assert_eq!(volumes.len(), 2);
        let root = volumes
            .iter()
            .find(|volume| volume.mount_point == "/")
            .unwrap();
        assert_eq!(root.id, "Macintosh HD");
        assert_eq!(root.name, "Macintosh HD");
        assert_eq!((root.total, root.used), (100, 70));
        assert!(volumes
            .iter()
            .all(|volume| volume.mount_point != "/System/Volumes/Data"));
        assert!(volumes.iter().any(|volume| volume.name == "Backup"));
    }

    #[test]
    fn apfs_data_label_matching_is_specific_to_the_root_group() {
        assert!(super::apfs_data_matches_root(
            "Macintosh HD - Data",
            "Macintosh HD"
        ));
        assert!(super::apfs_data_matches_root(
            "Macintosh HD",
            "Macintosh HD"
        ));
        assert!(!super::apfs_data_matches_root(
            "Backup - Data",
            "Macintosh HD"
        ));
    }
}
