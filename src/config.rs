use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::model::{Intervals, PingTarget, RemoteConfig};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalConfig {
    pub server_id: String,
    pub secret: String,
    pub worker_url: String,
    pub intervals: Intervals,
    #[serde(default = "default_reset_day")]
    pub reset_day: u8,
    /// 版本字符串（UTC+8 时间戳格式）；空串 = 从未下发过
    #[serde(default, deserialize_with = "crate::model::de_config_version")]
    pub config_version: String,
    #[serde(default)]
    pub interfaces: Vec<String>,
    #[serde(default)]
    pub enable_gpu: bool,
    #[serde(default = "default_net_static_path")]
    pub net_static_path: String,
    #[serde(default)]
    pub pings: Vec<PingTarget>,
    /// 是否上报 errors 错误事件（默认 true）
    #[serde(default = "default_report_errors")]
    pub report_errors: bool,
    /// 是否上报探针自身资源占用 kind:"self"（默认 false）
    #[serde(default)]
    pub report_self: bool,
}

fn default_report_errors() -> bool {
    true
}

fn default_reset_day() -> u8 {
    1
}

fn default_net_static_path() -> String {
    "/var/lib/probe-rs/net_static.json".into()
}

impl LocalConfig {
    pub fn validate(&self) -> Result<()> {
        if self.server_id.trim().is_empty() {
            bail!("server_id 不能为空");
        }
        if self.secret.trim().is_empty() {
            bail!("secret 不能为空");
        }
        let url = self.worker_url.trim();
        if !url.starts_with("http://") && !url.starts_with("https://") {
            bail!("worker_url 必须是 http(s) URL");
        }
        self.intervals.validate().map_err(anyhow::Error::msg)?;
        if self.reset_day > 31 {
            bail!("reset_day 必须在 0-31 之间");
        }
        for p in &self.pings {
            if p.name.trim().is_empty() {
                bail!("探测目标 name 不能为空");
            }
            if p.target.trim().is_empty() {
                bail!("探测目标 {} 的 target 不能为空", p.name);
            }
            if let Some(i) = p.interval {
                if i == 0 {
                    bail!("探测目标 {} 的 interval 必须 >= 1 秒", p.name);
                }
            }
        }
        Ok(())
    }
}

/// 共享运行时配置：本地配置 + intervals 变更通知（scheduler 重建 ticker）
/// + 全量变更通知（supervisor 重建 worker；本地热加载与远端下发共用）
pub struct SharedConfig {
    inner: RwLock<LocalConfig>,
    path: PathBuf,
    intervals_tx: watch::Sender<Intervals>,
    config_tx: watch::Sender<LocalConfig>,
}

impl SharedConfig {
    pub fn new(
        cfg: LocalConfig,
        path: PathBuf,
    ) -> (Arc<Self>, watch::Receiver<Intervals>, watch::Receiver<LocalConfig>) {
        let (tx, rx) = watch::channel(cfg.intervals);
        let (config_tx, config_rx) = watch::channel(cfg.clone());
        (
            Arc::new(Self {
                inner: RwLock::new(cfg),
                path,
                intervals_tx: tx,
                config_tx,
            }),
            rx,
            config_rx,
        )
    }

    pub fn get(&self) -> LocalConfig {
        self.inner.read().expect("config lock poisoned").clone()
    }

    /// 本地文件热加载：整体替换（文件是唯一事实源，远端应用也会回写文件）。
    /// 仅在 intervals 变化时通知 scheduler 重建 ticker
    pub fn update_local(&self, cfg: LocalConfig) {
        let mut guard = self.inner.write().expect("config lock poisoned");
        self.intervals_tx.send_if_modified(|cur| {
            if *cur != cfg.intervals {
                *cur = cfg.intervals;
                true
            } else {
                false
            }
        });
        self.config_tx.send_if_modified(|cur| {
            if *cur != cfg {
                *cur = cfg.clone();
                true
            } else {
                false
            }
        });
        *guard = cfg;
    }

    /// 应用远端配置：整体校验通过才应用 + 落盘；version 不更大则忽略。
    /// intervals/reset_day/interfaces 立即更新到内存（intervals 经 watch 触发热重建）；
    /// pings/enable_gpu 写入文件后由热加载监听（≤3s）重建对应 worker
    pub fn apply_remote(&self, remote: RemoteConfig) -> Result<()> {
        {
            let current = self.inner.read().expect("config lock poisoned");
            // != 判断：版本不同才应用（幂等：同版本跳过）
            if remote.config_version.is_empty() || remote.config_version == current.config_version {
                return Ok(());
            }
        }
        validate_remote(&remote)?;
        let mut cfg = self.inner.write().expect("config lock poisoned");
        // 二次检查，避免读锁释放期间被其他响应抢先应用
        if remote.config_version == cfg.config_version {
            return Ok(());
        }
        // 先在副本上改、落盘成功才换入内存：落盘失败时内存与磁盘保持一致，
        // 版本号不会被提前吃掉导致服务端永远不再重发
        let mut next = cfg.clone();
        if let Some(intervals) = remote.intervals {
            next.intervals = intervals;
        }
        if let Some(reset_day) = remote.reset_day {
            next.reset_day = reset_day;
        }
        if let Some(interfaces) = remote.interfaces {
            next.interfaces = interfaces;
        }
        if let Some(pings) = remote.pings {
            next.pings = pings;
        }
        if let Some(enable_gpu) = remote.enable_gpu {
            next.enable_gpu = enable_gpu;
        }
        if let Some(report_errors) = remote.report_errors {
            next.report_errors = report_errors;
        }
        if let Some(report_self) = remote.report_self {
            next.report_self = report_self;
        }
        let version = remote.config_version;
        next.config_version = version.clone();
        persist(&self.path, &next).context("远端配置落盘失败")?;
        let intervals = next.intervals;
        let full = next.clone();
        *cfg = next;
        drop(cfg);
        self.intervals_tx.send_replace(intervals);
        self.config_tx.send_replace(full);
        tracing::info!(config_version = version, "远端配置已应用");
        Ok(())
    }
}

/// 远端配置整体校验：任何一项非法则整体拒绝
fn validate_remote(remote: &RemoteConfig) -> Result<()> {
    if let Some(intervals) = &remote.intervals {
        intervals.validate().map_err(anyhow::Error::msg)?;
    }
    if let Some(reset_day) = remote.reset_day {
        if reset_day > 31 {
            bail!("远端 reset_day 非法: {reset_day}");
        }
    }
    if let Some(interfaces) = &remote.interfaces {
        for pattern in interfaces {
            let p = pattern.trim();
            if p.is_empty() || p.len() > 64 {
                bail!("远端 interfaces 参数非法: {pattern:?}");
            }
            globset::Glob::new(p).map_err(|e| anyhow::anyhow!("远端 interfaces glob 非法 {pattern:?}: {e}"))?;
        }
    }
    if let Some(pings) = &remote.pings {
        let mut names = std::collections::HashSet::new();
        for p in pings {
            if p.name.trim().is_empty() {
                bail!("远端 pings 存在空 name");
            }
            if p.target.trim().is_empty() {
                bail!("远端 pings 目标 {} 的 target 为空", p.name);
            }
            if !names.insert(p.name.clone()) {
                bail!("远端 pings name 重复: {}", p.name);
            }
            if let Some(i) = p.interval {
                if i == 0 {
                    bail!("远端 pings 目标 {} 的 interval 必须 >= 1 秒", p.name);
                }
            }
        }
    }
    Ok(())
}

pub fn load(path: &Path) -> Result<LocalConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("读取配置失败: {}", path.display()))?;
    let cfg: LocalConfig = toml::from_str(&raw).context("解析配置 TOML 失败")?;
    cfg.validate()?;
    Ok(cfg)
}

/// tmp + rename 原子写
fn persist(path: &Path, cfg: &LocalConfig) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let data = toml::to_string_pretty(cfg)?;
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, data)?;
    // 配置含 secret：rename 前固定 0600，避免 umask 把 install.sh 建的 600 降级成 644
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> LocalConfig {
        LocalConfig {
            server_id: "s1".into(),
            secret: "sec".into(),
            worker_url: "https://example.com/report".into(),
            intervals: Intervals { collect: 10, report: 60, ping: 30, ..Default::default() },
            reset_day: 1,
            config_version: String::new(),
            interfaces: vec![],
            enable_gpu: false,
            net_static_path: "/tmp/x.json".into(),
            pings: vec![],
            report_errors: true,
            report_self: false,
        }
    }

    #[test]
    fn rejects_zero_intervals() {
        let mut cfg = base_config();
        cfg.intervals.collect = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn example_config_parses() {
        // 防回归：config.example.toml 本身必须能解析（TOML 布局陷阱曾让示例文件失效）
        let text = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/config.example.toml"))
            .unwrap();
        let cfg: LocalConfig = toml::from_str(&text).unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn rejects_unknown_fields_loudly() {
        // 标量误放在 [intervals] 段之后会被解析进 intervals 表，必须报错而不是静默忽略
        let bad_toml = r#"
server_id = "s1"
secret = "sec"
worker_url = "https://example.com/report"

[intervals]
collect = 1
report = 2
ping = 2
reset_day = 15
"#;
        assert!(toml::from_str::<LocalConfig>(bad_toml).is_err());
    }

    #[test]
    fn remote_config_applied_atomically() {
        let dir = std::env::temp_dir().join(format!("probe-rs-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        let cfg = base_config();
        persist(&path, &cfg).unwrap();
        let (shared, rx, _config_rx) = SharedConfig::new(cfg, path.clone());

        // 版本相同或为空：忽略（!= 语义，空版本号视为无版本）
        shared
            .apply_remote(RemoteConfig {
                config_version: String::new(),
                intervals: Some(Intervals { collect: 1, report: 1, ping: 1, ..Default::default() }),
                reset_day: Some(5),
                interfaces: None,
                enable_gpu: None,
                pings: None,
                report_errors: None,
                report_self: None,
            })
            .unwrap();
        assert_eq!(shared.get().reset_day, 1);

        // 零值间隔：整体拒绝
        assert!(shared
            .apply_remote(RemoteConfig {
                config_version: "2026-08-06T15:00:00+08:00".into(),
                intervals: Some(Intervals { collect: 0, report: 20, ping: 30, ..Default::default() }),
                reset_day: Some(5),
                interfaces: None,
                enable_gpu: None,
                pings: None,
                report_errors: None,
                report_self: None,
            })
            .is_err());
        assert_eq!(shared.get().config_version, "");

        // 合法：应用并落盘
        shared
            .apply_remote(RemoteConfig {
                config_version: "2026-08-06T15:00:00+08:00".into(),
                intervals: Some(Intervals { collect: 5, report: 20, ping: 15, ..Default::default() }),
                reset_day: Some(15),
                interfaces: None,
                enable_gpu: None,
                pings: None,
                report_errors: None,
                report_self: None,
            })
            .unwrap();
        let after = shared.get();
        assert_eq!(after.config_version, "2026-08-06T15:00:00+08:00");
        assert_eq!(after.reset_day, 15);
        assert!(rx.has_changed().unwrap());
        let on_disk = load(&path).unwrap();
        assert_eq!(on_disk.config_version, "2026-08-06T15:00:00+08:00");
        assert_eq!(on_disk.intervals.report, 20);

        std::fs::remove_dir_all(&dir).ok();
    }
}
