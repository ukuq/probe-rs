//! 网卡计数器：/proc/net/dev + 白名单过滤

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

#[cfg(target_os = "linux")]
use super::scan_file;

/// 默认排除的虚拟网卡前缀（Linux 容器/虚拟化 + macOS 虚拟接口）
#[cfg(not(target_os = "windows"))]
const EXCLUDED_PREFIXES: &[&str] = &[
    "br", "cni", "docker", "podman", "flannel", "lo", "veth", "virbr", "vmbr", "tap", "fwbr",
    "fwpr", // Linux
    "utun", "awdl", "gif", "stf", "llw", "anpi", // macOS
];

/// Windows 的 sysinfo 网卡键是 InterfaceAlias；常见 Hyper-V、VPN 和隧道
/// 接口通过别名片段排除。显式 interfaces 白名单始终优先，可重新纳入这些接口。
#[cfg(target_os = "windows")]
const WINDOWS_EXCLUDED_PARTS: &[&str] = &[
    "vethernet",
    "vswitch",
    "virtual",
    "loopback",
    "vpn",
    "wintun",
    "wireguard",
    "tailscale",
    "zerotier",
    "hamachi",
    "nordlynx",
    "openvpn",
    "cloudflare warp",
    "mihomo",
    "clash",
    "cfw-tap",
    "sing-box",
    "tap-windows",
    "teredo",
    "isatap",
    "6to4",
    "local area connection*",
    "本地连接*",
];

#[cfg(target_os = "windows")]
const WINDOWS_EXCLUDED_PREFIXES: &[&str] = &[
    "docker", "veth", "br-", "virbr", "vmbr", "tap", "fwbr", "fwpr",
];

#[cfg(not(target_os = "windows"))]
fn is_default_excluded(name: &str) -> bool {
    EXCLUDED_PREFIXES.iter().any(|p| name.starts_with(p))
}

#[cfg(target_os = "windows")]
fn is_default_excluded(name: &str) -> bool {
    let name = name.to_lowercase();
    name == "lo"
        || WINDOWS_EXCLUDED_PREFIXES
            .iter()
            .any(|p| name.starts_with(p))
        || WINDOWS_EXCLUDED_PARTS.iter().any(|p| name.contains(p))
}

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
            let glob = {
                let mut builder = GlobBuilder::new(p);
                builder.case_insensitive(cfg!(target_os = "windows"));
                builder.build()
            };
            if let Ok(g) = glob {
                builder.add(g);
                any = true;
            } else {
                tracing::warn!(pattern = p, "忽略非法网卡 glob");
            }
        }
        Self {
            whitelist: if any {
                Some(builder.build().expect("non-empty globset"))
            } else {
                None
            },
        }
    }

    pub fn includes(&self, name: &str) -> bool {
        match &self.whitelist {
            Some(set) => set.is_match(name),
            None => !is_default_excluded(name),
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

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_default_excludes_virtual_and_vpn_adapters() {
        let f = IfaceFilter::new(&[]);
        assert!(f.includes("Ethernet"));
        assert!(f.includes("WLAN"));
        assert!(!f.includes("vEthernet (Default Switch)"));
        assert!(!f.includes("Mihomo"));
        assert!(!f.includes("本地连接* 8"));

        let explicit = IfaceFilter::new(&["VETHERNET*".to_string()]);
        assert!(explicit.includes("vEthernet (Default Switch)"));
    }

    #[test]
    fn regression_delta_zero() {
        assert_eq!(counter_delta(100, 200), 0);
        assert_eq!(counter_delta(300, 200), 100);
    }
}
