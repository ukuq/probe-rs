use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::model::{
    CollectionIntervals, ExtConfig, Intervals, PingTarget, RemoteConfig, ReporterConfig,
    StaticConfig,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalConfig {
    /// 实际 collector/async worker 周期；不含任何上报周期。
    pub intervals: CollectionIntervals,
    /// 是否实际启动 GPU worker。各 Reporter 只能决定是否输出 GPU。
    #[serde(default)]
    pub enable_gpu: bool,
    #[serde(default = "default_net_static_path")]
    pub net_static_path: String,
    /// 全局 Ping worker 定义；Reporter 仅用 ping_names 选择输出子集。
    #[serde(default)]
    pub pings: Vec<PingTarget>,
    /// 所有独立上报实例，包括 id="primary"。
    pub reporters: Vec<ReporterConfig>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReporterSpec {
    pub id: String,
    pub protocol: String,
    pub server_id: String,
    pub secret: String,
    pub worker_url: String,
    pub config_version: String,
    pub intervals: Intervals,
    pub reset_day: u8,
    pub interfaces: Vec<String>,
    pub report_gpu: bool,
    pub report_errors: bool,
    pub report_self: bool,
    pub pings: Vec<PingTarget>,
    pub ext: ExtConfig,
}

impl ReporterSpec {
    pub fn connection_key(&self) -> (&str, &str, &str, &str, &str) {
        (
            &self.id,
            &self.protocol,
            &self.server_id,
            &self.secret,
            &self.worker_url,
        )
    }

    pub fn static_config(&self) -> StaticConfig {
        StaticConfig {
            reset_day: self.reset_day,
            intervals: self.intervals,
            interfaces: self.interfaces.clone(),
            enable_gpu: self.report_gpu,
            report_errors: self.report_errors,
            report_self: self.report_self,
            pings: self.pings.clone(),
            ext: self.ext.clone(),
        }
    }
}

pub fn default_config_path() -> PathBuf {
    platform_config_dir().join("config.toml")
}

fn default_net_static_path() -> String {
    platform_data_dir()
        .join("net_static.json")
        .to_string_lossy()
        .into_owned()
}

#[cfg(windows)]
fn platform_data_dir() -> PathBuf {
    std::env::var_os("ProgramData")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
        .join("probe-rs")
}

#[cfg(windows)]
fn platform_config_dir() -> PathBuf {
    platform_data_dir()
}

#[cfg(not(windows))]
fn platform_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/probe-rs")
}

#[cfg(not(windows))]
fn platform_config_dir() -> PathBuf {
    PathBuf::from("/etc/probe-rs")
}

impl LocalConfig {
    pub fn validate(&self) -> Result<()> {
        self.intervals.validate().map_err(anyhow::Error::msg)?;
        validate_pings(&self.pings)?;
        if self.reporters.is_empty() {
            bail!("至少需要一个 [[reporters]]");
        }
        let mut ids = std::collections::HashSet::new();
        let global_ping_names: std::collections::HashSet<&str> =
            self.pings.iter().map(|p| p.name.as_str()).collect();
        for reporter in &self.reporters {
            if reporter.id.trim().is_empty() {
                bail!("reporters.id 不能为空");
            }
            if !ids.insert(reporter.id.clone()) {
                bail!("reporters.id 重复: {}", reporter.id);
            }
            validate_connection(
                &reporter.protocol,
                &reporter.server_id,
                &reporter.secret,
                &reporter.worker_url,
            )?;
            if reporter.report_interval == 0 {
                bail!("reporter {} report_interval 必须 >= 1", reporter.id);
            }
            if reporter.reset_day > 31 {
                bail!("reporter {} reset_day 必须在 0-31 之间", reporter.id);
            }
            validate_interfaces(&reporter.interfaces)?;
            if let Some(names) = &reporter.ping_names {
                let mut selected = std::collections::HashSet::new();
                for name in names {
                    if !selected.insert(name) {
                        bail!("reporter {} ping_names 重复: {name}", reporter.id);
                    }
                    if !global_ping_names.contains(name.as_str()) {
                        bail!("reporter {} 引用了不存在的全局 ping: {name}", reporter.id);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn reporter_specs(&self) -> Vec<ReporterSpec> {
        self.reporters
            .iter()
            .map(|r| {
                let pings = match &r.ping_names {
                    None => self.pings.clone(),
                    Some(names) => self
                        .pings
                        .iter()
                        .filter(|ping| names.contains(&ping.name))
                        .cloned()
                        .collect(),
                };
                ReporterSpec {
                    id: r.id.clone(),
                    protocol: r.protocol.clone(),
                    server_id: r.server_id.clone(),
                    secret: r.secret.clone(),
                    worker_url: r.worker_url.clone(),
                    config_version: r.config_version.clone(),
                    intervals: self.intervals.with_report(r.report_interval),
                    reset_day: r.reset_day,
                    interfaces: r.interfaces.clone(),
                    report_gpu: r.report_gpu.unwrap_or(r.protocol == "cf"),
                    report_errors: r.report_errors,
                    report_self: r.report_self,
                    pings,
                    ext: r.ext.clone(),
                }
            })
            .collect()
    }

    pub fn reporter(&self, id: &str) -> Option<ReporterSpec> {
        self.reporter_specs().into_iter().find(|r| r.id == id)
    }

    pub fn effective_intervals(&self) -> Intervals {
        // report 仅为旧的内部载体占位；任何 Reporter 变更都不会触发 collector ticker。
        self.intervals.with_report(1)
    }

    pub fn effective_gpu(&self) -> bool {
        self.enable_gpu
    }

    pub fn effective_pings(&self) -> Vec<PingTarget> {
        self.pings
            .iter()
            .cloned()
            .map(|mut ping| {
                ping.interval = Some(ping.interval.unwrap_or(self.intervals.ping));
                ping
            })
            .collect()
    }
}

fn validate_connection(protocol: &str, server_id: &str, secret: &str, url: &str) -> Result<()> {
    if server_id.trim().is_empty() {
        bail!("server_id 不能为空");
    }
    if secret.trim().is_empty() {
        bail!("secret 不能为空");
    }
    if !url.starts_with("http://") && !url.starts_with("https://") {
        bail!("worker_url 必须是 http(s) URL");
    }
    if !["probe", "cf", "komari"].contains(&protocol) {
        bail!("protocol 必须是 probe / cf / komari");
    }
    Ok(())
}

fn validate_interfaces(interfaces: &[String]) -> Result<()> {
    for pattern in interfaces {
        let p = pattern.trim();
        if p.is_empty() || p.len() > 64 {
            bail!("interfaces 参数非法: {pattern:?}");
        }
        globset::Glob::new(p)
            .map_err(|e| anyhow::anyhow!("interfaces glob 非法 {pattern:?}: {e}"))?;
    }
    Ok(())
}

fn validate_pings(pings: &[PingTarget]) -> Result<()> {
    let mut names = std::collections::HashSet::new();
    for ping in pings {
        if ping.name.trim().is_empty() || ping.target.trim().is_empty() {
            bail!("ping name/target 不能为空");
        }
        if !names.insert(&ping.name) {
            bail!("ping name 重复: {}", ping.name);
        }
        if ping.interval == Some(0) {
            bail!("ping {} interval 必须 >= 1", ping.name);
        }
    }
    Ok(())
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
    ) -> (
        Arc<Self>,
        watch::Receiver<Intervals>,
        watch::Receiver<LocalConfig>,
    ) {
        let (tx, rx) = watch::channel(cfg.effective_intervals());
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

    pub fn subscribe_config(&self) -> watch::Receiver<LocalConfig> {
        self.config_tx.subscribe()
    }

    /// 本地文件热加载：整体替换（文件是唯一事实源，远端应用也会回写文件）。
    /// 仅在 intervals 变化时通知 scheduler 重建 ticker
    pub fn update_local(&self, cfg: LocalConfig) {
        let mut guard = self.inner.write().expect("config lock poisoned");
        let effective = cfg.effective_intervals();
        *guard = cfg.clone();
        drop(guard);
        self.intervals_tx.send_if_modified(|cur| {
            if *cur != effective {
                *cur = effective;
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
    }

    #[cfg(test)]
    pub fn apply_remote(&self, remote: RemoteConfig) -> Result<()> {
        self.apply_remote_for("primary", remote)
    }

    /// 远端配置只写入产生它的 Reporter；全局采集配置永不受上报端影响。
    pub fn apply_remote_for(&self, reporter_id: &str, remote: RemoteConfig) -> Result<()> {
        {
            let current = self.inner.read().expect("config lock poisoned");
            let version = current
                .reporter(reporter_id)
                .ok_or_else(|| anyhow::anyhow!("Reporter 不存在: {reporter_id}"))?
                .config_version;
            if remote.config_version.is_empty() || remote.config_version == version {
                return Ok(());
            }
        }
        validate_remote(&remote)?;
        let mut cfg = self.inner.write().expect("config lock poisoned");
        if remote.config_version
            == cfg
                .reporter(reporter_id)
                .ok_or_else(|| anyhow::anyhow!("Reporter 不存在: {reporter_id}"))?
                .config_version
        {
            return Ok(());
        }
        let RemoteConfig {
            config_version,
            report_interval,
            reset_day,
            interfaces,
            report_gpu,
            report_errors,
            report_self,
            ext,
        } = remote;
        let mut next = cfg.clone();
        let reporter = next
            .reporters
            .iter_mut()
            .find(|r| r.id == reporter_id)
            .ok_or_else(|| anyhow::anyhow!("Reporter 不存在: {reporter_id}"))?;
        if let Some(value) = report_interval {
            reporter.report_interval = value;
        }
        if let Some(value) = reset_day {
            reporter.reset_day = value;
        }
        if let Some(value) = interfaces {
            reporter.interfaces = value;
        }
        if let Some(value) = report_gpu {
            reporter.report_gpu = Some(value);
        }
        if let Some(value) = report_errors {
            reporter.report_errors = value;
        }
        if let Some(value) = report_self {
            reporter.report_self = value;
        }
        if let Some(value) = ext {
            if let Some(cf) = value.cf {
                if let Some(value) = cf.correction {
                    reporter.ext.cf.correction = value;
                }
                if let Some(value) = cf.batch {
                    reporter.ext.cf.batch = value;
                }
            }
        }
        reporter.config_version = config_version.clone();
        next.validate()
            .context("remote Reporter config is invalid")?;
        persist(&self.path, &next).context("远端配置落盘失败")?;
        let effective = next.effective_intervals();
        let full = next.clone();
        *cfg = next;
        drop(cfg);
        self.intervals_tx.send_if_modified(|current| {
            if *current != effective {
                *current = effective;
                true
            } else {
                false
            }
        });
        self.config_tx.send_replace(full);
        tracing::info!(reporter_id, config_version, "Reporter 远端配置已应用");
        Ok(())
    }
}

/// 远端配置整体校验：任何一项非法则整体拒绝
fn validate_remote(remote: &RemoteConfig) -> Result<()> {
    if remote.report_interval == Some(0) {
        bail!("远端 report_interval 必须 >= 1");
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
            globset::Glob::new(p)
                .map_err(|e| anyhow::anyhow!("远端 interfaces glob 非法 {pattern:?}: {e}"))?;
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
            intervals: CollectionIntervals {
                collect: 10,
                ping: 30,
                ..Default::default()
            },
            enable_gpu: true,
            net_static_path: "/tmp/x.json".into(),
            pings: vec![PingTarget {
                name: "ct".into(),
                target: "example.com:80".into(),
                interval: None,
            }],
            reporters: vec![ReporterConfig {
                id: "primary".into(),
                protocol: "probe".into(),
                server_id: "s1".into(),
                secret: "sec".into(),
                worker_url: "https://example.com/report".into(),
                config_version: String::new(),
                report_interval: 60,
                reset_day: 1,
                interfaces: vec![],
                report_gpu: Some(false),
                report_errors: true,
                report_self: false,
                ping_names: None,
                ext: Default::default(),
            }],
        }
    }

    #[test]
    fn default_paths_match_platform_layout() {
        #[cfg(windows)]
        {
            let base = std::env::var_os("ProgramData")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
                .join("probe-rs");
            assert_eq!(default_config_path(), base.join("config.toml"));
            assert_eq!(
                PathBuf::from(default_net_static_path()),
                base.join("net_static.json")
            );
        }

        #[cfg(not(windows))]
        {
            assert_eq!(
                default_config_path(),
                PathBuf::from("/etc/probe-rs/config.toml")
            );
            assert_eq!(
                default_net_static_path(),
                "/var/lib/probe-rs/net_static.json"
            );
        }
    }

    #[test]
    fn rejects_zero_intervals() {
        let mut cfg = base_config();
        cfg.intervals.collect = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn toml_roundtrip_preserves_ext_and_pings() {
        // TOML 布局陷阱防回归：[ext.cf] 与 [[pings]] 的表格次序必须往返无损
        let mut cfg = base_config();
        cfg.reporters[0].ext.cf.correction = false;
        let text = toml::to_string_pretty(&cfg).unwrap();
        let back: LocalConfig = toml::from_str(&text).unwrap();
        assert_eq!(back, cfg);
        assert!(!back.reporters[0].ext.cf.correction);
        assert_eq!(back.pings[0].name, "ct");
    }

    #[test]
    fn global_collection_and_reporter_output_are_independent() {
        let mut cfg = base_config();
        cfg.reporters.push(ReporterConfig {
            id: "komari-a".into(),
            protocol: "komari".into(),
            server_id: "node-a".into(),
            secret: "token".into(),
            worker_url: "http://panel.example".into(),
            config_version: String::new(),
            report_interval: 3,
            reset_day: 12,
            interfaces: vec!["Ethernet*".into()],
            report_gpu: Some(true),
            report_errors: false,
            report_self: true,
            ping_names: Some(vec![]),
            ext: Default::default(),
        });
        cfg.reporters.push(ReporterConfig {
            id: "cf-a".into(),
            protocol: "cf".into(),
            server_id: "cf-id".into(),
            secret: "cf-secret".into(),
            worker_url: "https://worker.example/update".into(),
            config_version: String::new(),
            report_interval: 30,
            reset_day: 1,
            interfaces: vec![],
            report_gpu: None,
            report_errors: true,
            report_self: false,
            ping_names: Some(vec!["ct".into()]),
            ext: Default::default(),
        });

        cfg.validate().unwrap();
        let komari = cfg.reporter("komari-a").unwrap();
        assert_eq!(komari.reset_day, 12);
        assert_eq!(komari.intervals.collect, 10);
        assert_eq!(komari.intervals.report, 3);
        assert!(komari.report_gpu);
        assert!(komari.report_self);
        assert!(!komari.report_errors);
        assert!(komari.pings.is_empty());
        assert!(cfg.reporter("cf-a").unwrap().report_gpu);
        assert_eq!(cfg.effective_intervals().collect, 10);
        assert_eq!(cfg.effective_intervals().report, 1); // 内部占位，不是上报周期
        assert!(cfg.effective_gpu());
        assert_eq!(cfg.effective_pings()[0].interval, Some(30));

        let serialized = toml::to_string_pretty(&cfg).unwrap();
        let roundtrip: LocalConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(roundtrip, cfg);
    }

    #[test]
    fn example_config_parses() {
        // 防回归：config.example.toml 本身必须能解析（TOML 布局陷阱曾让示例文件失效）
        let text =
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/config.example.toml"))
                .unwrap();
        let cfg: LocalConfig = toml::from_str(&text).unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn minimal_canonical_config_parses() {
        let text = r#"
net_static_path = "/tmp/net.json"
enable_gpu = true

[intervals]
collect = 1
ping = 30
slow = 60
gpu = 60
ip = 600
diskio = 10

[[reporters]]
id = "komari"
protocol = "komari"
server_id = "node-a"
secret = "token"
worker_url = "http://panel.example"
config_version = ""
report_interval = 1
reset_day = 12
interfaces = []
report_gpu = true
report_errors = true
report_self = false
"#;
        let cfg: LocalConfig = toml::from_str(text).unwrap();
        cfg.validate().unwrap();
        let komari = cfg.reporter("komari").unwrap();
        assert_eq!(komari.protocol, "komari");
        assert_eq!(komari.reset_day, 12);
        assert_eq!(komari.intervals.collect, 1);
        assert!(komari.report_gpu);
    }

    #[test]
    fn rejects_unknown_fields_loudly() {
        // 标量误放在 [intervals] 段之后会被解析进 intervals 表，必须报错而不是静默忽略
        let bad_toml = r#"
[intervals]
collect = 1
ping = 2
reset_day = 15
"#;
        assert!(toml::from_str::<LocalConfig>(bad_toml).is_err());
    }

    #[test]
    fn rejects_legacy_root_connection_shape() {
        let legacy = r#"
server_id = "s1"
secret = "sec"
worker_url = "https://example.com/report"
protocol = "probe"

[intervals]
collect = 1
report = 10
ping = 30
"#;
        assert!(toml::from_str::<LocalConfig>(legacy).is_err());
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
                report_interval: Some(1),
                reset_day: Some(5),
                interfaces: None,
                report_gpu: None,
                report_errors: None,
                report_self: None,
                ext: None,
            })
            .unwrap();
        assert_eq!(shared.get().reporter("primary").unwrap().reset_day, 1);

        // 零值间隔：整体拒绝
        assert!(shared
            .apply_remote(RemoteConfig {
                config_version: "2026-08-06T15:00:00+08:00".into(),
                report_interval: Some(0),
                reset_day: Some(5),
                interfaces: None,
                report_gpu: None,
                report_errors: None,
                report_self: None,
                ext: None,
            })
            .is_err());
        assert_eq!(shared.get().reporter("primary").unwrap().config_version, "");

        // 合法：应用并落盘
        shared
            .apply_remote(RemoteConfig {
                config_version: "2026-08-06T15:00:00+08:00".into(),
                report_interval: Some(20),
                reset_day: Some(15),
                interfaces: None,
                report_gpu: None,
                report_errors: None,
                report_self: None,
                ext: None,
            })
            .unwrap();
        let after = shared.get();
        let primary = after.reporter("primary").unwrap();
        assert_eq!(primary.config_version, "2026-08-06T15:00:00+08:00");
        assert_eq!(primary.reset_day, 15);
        assert_eq!(primary.intervals.report, 20);
        assert_eq!(after.intervals.collect, 10);
        assert!(!rx.has_changed().unwrap());
        let on_disk = load(&path).unwrap();
        assert_eq!(
            on_disk.reporter("primary").unwrap().config_version,
            "2026-08-06T15:00:00+08:00"
        );
        assert_eq!(on_disk.reporters[0].report_interval, 20);

        std::fs::remove_dir_all(&dir).ok();
    }
}
