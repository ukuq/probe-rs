//! static 信息：OS/内核/CPU/虚拟化/开机时间等慢变项

use std::collections::HashSet;

use crate::model::StaticInfo;

use super::{disk, mem, scan_file};

pub fn collect(
    ipv4: Option<String>,
    ipv6: Option<String>,
    gpu_name: Option<String>,
    agent_version: &str,
    cfg: &crate::config::LocalConfig,
) -> StaticInfo {
    let (mem_total, _, swap_total, _) = mem::collect();
    let (disk_total, _) = disk::collect();
    StaticInfo {
        ts: crate::model::now_millis(),
        os: os_name(),
        kernel: kernel_release(),
        arch: std::env::consts::ARCH.to_string(),
        cpu_name: cpu_name(),
        cpu_cores: std::thread::available_parallelism().map_or(1, |n| n.get() as u32),
        cpu_physical_cores: physical_cores(),
        mem_total,
        swap_total,
        disk_total,
        gpu_name,
        virtualization: detect_virtualization(),
        boot_time: boot_time_ms().unwrap_or_else(|| crate::model::now_millis()),
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

fn os_name() -> String {
    let mut pretty = String::new();
    let mut id = String::new();
    let _ = scan_file("/etc/os-release", |line| {
        if let Some((k, v)) = line.split_once('=') {
            let v = v.trim().trim_matches('"').to_string();
            match k {
                "PRETTY_NAME" => pretty = v,
                "ID" => id = v,
                _ => {}
            }
        }
    });
    if !pretty.is_empty() {
        pretty
    } else if !id.is_empty() {
        id
    } else {
        "Linux".into()
    }
}

fn kernel_release() -> String {
    std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn cpu_name() -> String {
    let mut name = String::new();
    let _ = scan_file("/proc/cpuinfo", |line| {
        if !name.is_empty() {
            return;
        }
        for key in ["model name", "Hardware", "Processor"] {
            if let Some(rest) = line.strip_prefix(key) {
                if let Some((_, v)) = rest.split_once(':') {
                    name = v.trim().to_string();
                    return;
                }
            }
        }
    });
    if name.is_empty() {
        name = std::env::consts::ARCH.to_string();
    }
    name
}

/// x86：physical id + core id 去重；ARM 等无此信息的平台返回 None
fn physical_cores() -> Option<u32> {
    let mut pairs = HashSet::new();
    let mut physical_id: Option<String> = None;
    let mut core_id: Option<String> = None;
    let mut seen_any = false;
    let _ = scan_file("/proc/cpuinfo", |line| {
        if line.trim().is_empty() {
            if let (Some(p), Some(c)) = (physical_id.take(), core_id.take()) {
                pairs.insert((p, c));
            }
            return;
        }
        if let Some((k, v)) = line.split_once(':') {
            let v = v.trim().to_string();
            match k.trim() {
                "physical id" => {
                    physical_id = Some(v);
                    seen_any = true;
                }
                "core id" => core_id = Some(v),
                _ => {}
            }
        }
    });
    if let (Some(p), Some(c)) = (physical_id, core_id) {
        pairs.insert((p, c));
    }
    if !seen_any || pairs.is_empty() {
        None
    } else {
        Some(pairs.len() as u32)
    }
}

fn boot_time_ms() -> Option<i64> {
    let mut btime = 0i64;
    scan_file("/proc/stat", |line| {
        if let Some(rest) = line.strip_prefix("btime ") {
            btime = rest.trim().parse().unwrap_or(0);
        }
    })
    .ok()?;
    (btime > 0).then_some(btime * 1000)
}

/// 基于 DMI/容器标记的轻量虚拟化检测，不 fork systemd-detect-virt
fn detect_virtualization() -> Option<String> {
    if std::path::Path::new("/.dockerenv").exists() {
        return Some("docker".into());
    }
    if std::path::Path::new("/run/.containerenv").exists() {
        return Some("container".into());
    }
    if let Ok(cgroup) = std::fs::read_to_string("/proc/1/cgroup") {
        if cgroup.contains("lxc") {
            return Some("lxc".into());
        }
    }
    let product = std::fs::read_to_string("/sys/class/dmi/id/product_name")
        .map(|s| s.trim().to_lowercase())
        .unwrap_or_default();
    let vendor = std::fs::read_to_string("/sys/class/dmi/id/sys_vendor")
        .map(|s| s.trim().to_lowercase())
        .unwrap_or_default();
    let haystack = format!("{product} {vendor}");
    let rules: &[(&str, &str)] = &[
        ("kvm", "kvm"),
        ("qemu", "kvm"),
        ("vmware", "vmware"),
        ("virtualbox", "virtualbox"),
        ("microsoft corporation", "hyper-v"),
        ("xen", "xen"),
        ("amazon ec2", "aws"),
        ("openstack", "openstack"),
    ];
    for (needle, name) in rules {
        if haystack.contains(needle) {
            return Some((*name).to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    #[test]
    fn reads_host_static() {
        // 在 Linux CI/开发机上这些应当能读到；非 Linux 直接跳过
        if !std::path::Path::new("/proc/stat").exists() {
            return;
        }
        let cfg = crate::config::LocalConfig {
            server_id: "test".into(),
            secret: String::new(),
            worker_url: String::new(),
            intervals: crate::model::Intervals::default(),
            reset_day: 1,
            config_version: String::new(),
            interfaces: vec![],
            enable_gpu: false,
            net_static_path: String::new(),
            pings: vec![],
            report_errors: true,
            report_self: false,
        };
        let info = super::collect(None, None, None, "test", &cfg);
        assert!(!info.os.is_empty());
        assert!(!info.kernel.is_empty());
        assert!(info.cpu_cores >= 1);
        assert!(info.boot_time > 0);
        assert!(info.mem_total > 0);
    }
}
