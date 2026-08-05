//! 网卡计数器：/proc/net/dev + 白名单过滤

use globset::{Glob, GlobSet, GlobSetBuilder};

#[cfg(target_os = "linux")]
use super::scan_file;

/// 默认排除的虚拟网卡前缀（Linux 容器/虚拟化 + macOS 虚拟接口）
const EXCLUDED_PREFIXES: &[&str] = &[
    "br", "cni", "docker", "podman", "flannel", "lo", "veth", "virbr", "vmbr", "tap", "fwbr",
    "fwpr", // Linux
    "utun", "awdl", "gif", "stf", "llw", "anpi", // macOS
];

#[derive(Debug, Clone, Copy, Default)]
pub struct NetBytes {
    pub rx: u64,
    pub tx: u64,
}

#[derive(Debug, Clone)]
pub struct IfaceFilter {
    whitelist: Option<GlobSet>,
}

impl IfaceFilter {
    pub fn new(patterns: &[String]) -> Self {
        let mut builder = GlobSetBuilder::new();
        let mut any = false;
        for p in patterns {
            let p = p.trim();
            if p.is_empty() {
                continue;
            }
            if let Ok(g) = Glob::new(p) {
                builder.add(g);
                any = true;
            } else {
                tracing::warn!(pattern = p, "忽略非法网卡 glob");
            }
        }
        Self {
            whitelist: if any { Some(builder.build().expect("non-empty globset")) } else { None },
        }
    }

    pub fn includes(&self, name: &str) -> bool {
        match &self.whitelist {
            Some(set) => set.is_match(name),
            None => !EXCLUDED_PREFIXES.iter().any(|p| name.starts_with(p)),
        }
    }
}

/// 遍历 /proc/net/dev 全部网卡（不过滤），回调 (name, rx, tx)
#[cfg(target_os = "linux")]
pub fn scan_net_dev(mut f: impl FnMut(&str, u64, u64)) {
    let _ = scan_file("/proc/net/dev", |line| {
        let Some((name, rest)) = line.split_once(':') else {
            return;
        };
        let fields: Vec<&str> = rest.split_whitespace().collect();
        if fields.len() < 16 {
            return;
        }
        f(
            name.trim(),
            fields[0].parse::<u64>().unwrap_or(0),
            fields[8].parse::<u64>().unwrap_or(0),
        );
    });
}

#[cfg(target_os = "linux")]
pub fn read_net_bytes(filter: &IfaceFilter) -> NetBytes {
    let mut total = NetBytes::default();
    scan_net_dev(|name, rx, tx| {
        if filter.includes(name) {
            total.rx += rx;
            total.tx += tx;
        }
    });
    total
}

/// 计数器回退（重启/换卡）时 delta 记 0，绝不出错误增量
pub fn counter_delta(current: u64, previous: u64) -> u64 {
    current.saturating_sub(previous)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_excludes_virtual() {
        let f = IfaceFilter::new(&[]);
        assert!(f.includes("eth0"));
        assert!(f.includes("enp3s0"));
        assert!(!f.includes("lo"));
        assert!(!f.includes("docker0"));
        assert!(!f.includes("veth123abc"));
        assert!(!f.includes("br-abc"));
    }

    #[test]
    fn whitelist_glob() {
        let f = IfaceFilter::new(&["eth*".to_string()]);
        assert!(f.includes("eth0"));
        assert!(!f.includes("enp3s0"));
        // 白名单模式不排除 docker 之类：以白名单为准
        assert!(!f.includes("docker0"));
    }

    #[test]
    fn regression_delta_zero() {
        assert_eq!(counter_delta(100, 200), 0);
        assert_eq!(counter_delta(300, 200), 100);
    }
}
