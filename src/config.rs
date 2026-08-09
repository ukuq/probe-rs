use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::model::{
    CollectionIntervals, ExtConfig, GlobalConfigSummary, GlobalPingTarget, Intervals, PingKind,
    PingTarget, RemoteConfig, ReporterConfig, ReporterSummary, StaticConfig,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalConfig {
    #[serde(default = "default_net_static_path")]
    pub net_static_path: String,
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
    pub disks: Vec<String>,
    pub report_gpu: bool,
    pub report_errors: bool,
    pub report_self: bool,
    pub pings: Vec<ScopedPingTarget>,
    pub ext: ExtConfig,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScopedPingTarget {
    pub task_id: String,
    pub target: PingTarget,
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

    pub fn static_config(
        &self,
        global: GlobalConfigSummary,
        reporters: Vec<ReporterSummary>,
    ) -> StaticConfig {
        StaticConfig {
            global,
            reporters,
            reset_day: self.reset_day,
            intervals: self.intervals,
            interfaces: self.interfaces.clone(),
            disks: self.disks.clone(),
            enable_gpu: self.report_gpu,
            report_errors: self.report_errors,
            report_self: self.report_self,
            pings: self.pings.iter().map(|ping| ping.target.clone()).collect(),
            ext: (self.protocol == "cf").then(|| self.ext.clone()),
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
        if self.reporters.is_empty() {
            bail!("至少需要一个 [[reporters]]");
        }
        let mut ids = std::collections::HashSet::new();
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
            reporter.intervals.validate().map_err(anyhow::Error::msg)?;
            if reporter.reset_day > 31 {
                bail!("reporter {} reset_day 必须在 0-31 之间", reporter.id);
            }
            validate_interfaces(&reporter.interfaces)?;
            validate_patterns("disks", &reporter.disks)?;
            validate_pings(&reporter.pings)
                .with_context(|| format!("reporter {} pings 非法", reporter.id))?;
        }
        Ok(())
    }

    pub fn reporter_specs(&self) -> Vec<ReporterSpec> {
        self.reporters
            .iter()
            .map(|r| ReporterSpec {
                id: r.id.clone(),
                protocol: r.protocol.clone(),
                server_id: r.server_id.clone(),
                secret: r.secret.clone(),
                worker_url: r.worker_url.clone(),
                config_version: r.config_version.clone(),
                intervals: r.intervals.with_report(r.report_interval),
                reset_day: r.reset_day,
                interfaces: r.interfaces.clone(),
                disks: r.disks.clone(),
                report_gpu: r.report_gpu.unwrap_or(r.protocol == "cf"),
                report_errors: r.report_errors,
                report_self: r.report_self,
                pings: r
                    .pings
                    .iter()
                    .cloned()
                    .map(|target| ScopedPingTarget {
                        task_id: ping_task_key(&target).expect("validated ping target"),
                        target,
                    })
                    .collect(),
                ext: r.ext.clone(),
            })
            .collect()
    }

    pub fn reporter(&self, id: &str) -> Option<ReporterSpec> {
        self.reporter_specs().into_iter().find(|r| r.id == id)
    }

    /// 可安全随 static 上报的全局采集摘要。
    pub fn global_summary(&self) -> GlobalConfigSummary {
        let interfaces: std::collections::BTreeSet<String> = self
            .reporters
            .iter()
            .flat_map(|reporter| reporter.interfaces.iter().cloned())
            .collect();
        let disks: std::collections::BTreeSet<String> = self
            .reporters
            .iter()
            .flat_map(|reporter| reporter.disks.iter().cloned())
            .collect();
        GlobalConfigSummary {
            intervals: self.effective_collection_intervals(),
            enable_gpu: self.effective_gpu(),
            interfaces: interfaces.into_iter().collect(),
            all_interfaces: self
                .reporters
                .iter()
                .any(|reporter| reporter.interfaces.is_empty()),
            disks: disks.into_iter().collect(),
            all_disks: self
                .reporters
                .iter()
                .any(|reporter| reporter.disks.is_empty()),
            pings: {
                let mut tasks = std::collections::BTreeMap::new();
                for reporter in &self.reporters {
                    for ping in &reporter.pings {
                        let interval = ping.interval.unwrap_or(reporter.intervals.ping);
                        tasks
                            .entry(ping_task_key(ping).expect("validated ping target"))
                            .and_modify(|task: &mut GlobalPingTarget| {
                                task.interval = task.interval.min(interval);
                            })
                            .or_insert_with(|| GlobalPingTarget {
                                target: global_ping_uri(ping).expect("validated ping target"),
                                interval,
                            });
                    }
                }
                tasks.into_values().collect()
            },
        }
    }

    /// 可安全随 static 上报的 Reporter 拓扑与输出策略摘要。
    pub fn reporter_summaries(&self) -> Vec<ReporterSummary> {
        self.reporter_specs()
            .into_iter()
            .map(|spec| ReporterSummary {
                id: spec.id,
                protocol: spec.protocol,
                intervals: CollectionIntervals {
                    collect: spec.intervals.collect,
                    ping: spec.intervals.ping,
                    slow: spec.intervals.slow,
                    gpu: spec.intervals.gpu,
                    ip: spec.intervals.ip,
                    diskio: spec.intervals.diskio,
                },
                report_interval: spec.intervals.report,
                reset_day: spec.reset_day,
                interfaces: spec.interfaces,
                disks: spec.disks,
                report_gpu: spec.report_gpu,
                report_errors: spec.report_errors,
                report_self: spec.report_self,
                pings: spec.pings.into_iter().map(|ping| ping.target).collect(),
            })
            .collect()
    }

    pub fn effective_intervals(&self) -> Intervals {
        self.effective_collection_intervals().with_report(1)
    }

    pub fn effective_gpu(&self) -> bool {
        self.reporter_specs()
            .iter()
            .any(|reporter| reporter.report_gpu)
    }

    pub fn effective_pings(&self) -> Vec<PingTarget> {
        let mut tasks: std::collections::BTreeMap<String, PingTarget> = Default::default();
        for reporter in self.reporter_specs() {
            for ping in reporter.pings {
                let interval = ping.target.interval.unwrap_or(reporter.intervals.ping);
                tasks
                    .entry(ping.task_id.clone())
                    .and_modify(|task| {
                        task.interval = Some(task.interval.unwrap_or(interval).min(interval));
                    })
                    .or_insert_with(|| PingTarget {
                        name: ping.task_id,
                        kind: ping.target.kind,
                        target: ping.target.target,
                        interval: Some(interval),
                    });
            }
        }
        tasks.into_values().collect()
    }

    pub fn effective_collection_intervals(&self) -> CollectionIntervals {
        self.reporters
            .iter()
            .map(|reporter| reporter.intervals)
            .reduce(|a, b| CollectionIntervals {
                collect: a.collect.min(b.collect),
                ping: a.ping.min(b.ping),
                slow: a.slow.min(b.slow),
                gpu: a.gpu.min(b.gpu),
                ip: a.ip.min(b.ip),
                diskio: a.diskio.min(b.diskio),
            })
            .unwrap_or_default()
    }
}

fn ping_task_key(ping: &PingTarget) -> Result<String> {
    match ping.kind {
        PingKind::Http => {
            let url = reqwest::Url::parse(&ping.target)
                .with_context(|| format!("非法 HTTP Ping URL: {}", ping.target))?;
            if !matches!(url.scheme(), "http" | "https") {
                bail!("HTTP Ping 只支持 http/https: {}", ping.target);
            }
            if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
                bail!(
                    "HTTP Ping target 不允许 path/query/fragment: {}",
                    ping.target
                );
            }
            let host = url
                .host_str()
                .ok_or_else(|| anyhow::anyhow!("HTTP Ping 缺少 host: {}", ping.target))?
                .trim_end_matches('.')
                .to_ascii_lowercase();
            let port = url
                .port_or_known_default()
                .ok_or_else(|| anyhow::anyhow!("HTTP Ping 缺少 port: {}", ping.target))?;
            Ok(format!("http:{}://{host}:{port}", url.scheme()))
        }
        PingKind::Tcp => {
            if ping
                .target
                .chars()
                .any(|ch| matches!(ch, '/' | '\\' | '?' | '#'))
            {
                bail!(
                    "TCP Ping target 不允许 path/query/fragment: {}",
                    ping.target
                );
            }
            let (host, port) = crate::worker::ping::split_host_port(&ping.target)?;
            Ok(format!(
                "tcp:{}:{port}",
                host.trim_end_matches('.').to_ascii_lowercase()
            ))
        }
        PingKind::Icmp => {
            let host = ping.target.trim().trim_matches(['[', ']']);
            if host.is_empty() || host.chars().any(|ch| matches!(ch, '/' | '\\' | '?' | '#')) {
                bail!("非法 ICMP host: {}", ping.target);
            }
            Ok(format!(
                "icmp:{}",
                host.trim_end_matches('.').to_ascii_lowercase()
            ))
        }
    }
}

/// 全局只读摘要使用 URI 自带类型，Reporter 私有配置仍保留独立 type 字段。
fn global_ping_uri(ping: &PingTarget) -> Result<String> {
    let authority_host = |host: &str| {
        let host = host
            .trim_matches(['[', ']'])
            .trim_end_matches('.')
            .to_ascii_lowercase();
        if host.contains(':') {
            format!("[{host}]")
        } else {
            host
        }
    };
    match ping.kind {
        PingKind::Http => {
            let url = reqwest::Url::parse(&ping.target)
                .with_context(|| format!("非法 HTTP Ping URL: {}", ping.target))?;
            let host = url
                .host_str()
                .ok_or_else(|| anyhow::anyhow!("HTTP Ping 缺少 host: {}", ping.target))?;
            let port = url
                .port_or_known_default()
                .ok_or_else(|| anyhow::anyhow!("HTTP Ping 缺少 port: {}", ping.target))?;
            Ok(format!(
                "{}://{}:{port}",
                url.scheme(),
                authority_host(host)
            ))
        }
        PingKind::Tcp => {
            let (host, port) = crate::worker::ping::split_host_port(&ping.target)?;
            Ok(format!("tcp://{}:{port}", authority_host(&host)))
        }
        PingKind::Icmp => {
            let host = ping.target.trim().trim_matches(['[', ']']);
            if host.is_empty() {
                bail!("非法 ICMP host: {}", ping.target);
            }
            Ok(format!("icmp://{}", authority_host(host)))
        }
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
    validate_patterns("interfaces", interfaces)
}

fn validate_patterns(field: &str, patterns: &[String]) -> Result<()> {
    for pattern in patterns {
        let p = pattern.trim();
        if p.is_empty() || p.len() > 64 {
            bail!("{field} 参数非法: {pattern:?}");
        }
        globset::Glob::new(p).map_err(|e| anyhow::anyhow!("{field} glob 非法 {pattern:?}: {e}"))?;
    }
    Ok(())
}

fn validate_pings(pings: &[PingTarget]) -> Result<()> {
    let mut names = std::collections::HashSet::new();
    for ping in pings {
        if ping.name.trim().is_empty() || ping.target.trim().is_empty() {
            bail!("ping name/target 不能为空");
        }
        ping_task_key(ping)?;
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
            intervals,
            report_interval,
            reset_day,
            interfaces,
            disks,
            pings,
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
        if let Some(value) = intervals {
            reporter.intervals = value;
        }
        if let Some(value) = report_interval {
            reporter.report_interval = value;
        }
        if let Some(value) = reset_day {
            reporter.reset_day = value;
        }
        if let Some(value) = interfaces {
            reporter.interfaces = value;
        }
        if let Some(value) = disks {
            reporter.disks = value;
        }
        if let Some(value) = pings {
            reporter.pings = value;
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
    if let Some(intervals) = remote.intervals {
        intervals.validate().map_err(anyhow::Error::msg)?;
    }
    if remote.report_interval == Some(0) {
        bail!("远端 report_interval 必须 >= 1");
    }
    if let Some(reset_day) = remote.reset_day {
        if reset_day > 31 {
            bail!("远端 reset_day 非法: {reset_day}");
        }
    }
    if let Some(interfaces) = &remote.interfaces {
        validate_patterns("远端 interfaces", interfaces)?;
    }
    if let Some(disks) = &remote.disks {
        validate_patterns("远端 disks", disks)?;
    }
    if let Some(pings) = &remote.pings {
        validate_pings(pings).context("远端 pings 非法")?;
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
            net_static_path: "/tmp/x.json".into(),
            reporters: vec![ReporterConfig {
                id: "primary".into(),
                protocol: "probe".into(),
                server_id: "s1".into(),
                secret: "sec".into(),
                worker_url: "https://example.com/report".into(),
                config_version: String::new(),
                intervals: CollectionIntervals {
                    collect: 10,
                    ping: 30,
                    ..Default::default()
                },
                report_interval: 60,
                reset_day: 1,
                interfaces: vec![],
                disks: vec![],
                report_gpu: Some(false),
                report_errors: true,
                report_self: false,
                pings: vec![PingTarget {
                    name: "ct".into(),
                    kind: PingKind::Tcp,
                    target: "example.com:80".into(),
                    interval: None,
                }],
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
        cfg.reporters[0].intervals.collect = 0;
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
        assert_eq!(back.reporters[0].pings[0].name, "ct");
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
            intervals: CollectionIntervals {
                collect: 5,
                ping: 10,
                ..Default::default()
            },
            report_interval: 3,
            reset_day: 12,
            interfaces: vec!["Ethernet*".into()],
            disks: vec!["C:*".into()],
            report_gpu: Some(true),
            report_errors: false,
            report_self: true,
            pings: vec![PingTarget {
                name: "same-host".into(),
                kind: PingKind::Tcp,
                target: "EXAMPLE.com:80".into(),
                interval: Some(10),
            }],
            ext: Default::default(),
        });
        cfg.reporters.push(ReporterConfig {
            id: "cf-a".into(),
            protocol: "cf".into(),
            server_id: "cf-id".into(),
            secret: "cf-secret".into(),
            worker_url: "https://worker.example/update".into(),
            config_version: String::new(),
            intervals: CollectionIntervals {
                collect: 1,
                ping: 20,
                ..Default::default()
            },
            report_interval: 30,
            reset_day: 1,
            interfaces: vec![],
            disks: vec![],
            report_gpu: None,
            report_errors: true,
            report_self: false,
            pings: vec![],
            ext: Default::default(),
        });

        cfg.validate().unwrap();
        let komari = cfg.reporter("komari-a").unwrap();
        assert_eq!(komari.reset_day, 12);
        assert_eq!(komari.intervals.collect, 5);
        assert_eq!(komari.intervals.report, 3);
        assert!(komari.report_gpu);
        assert!(komari.report_self);
        assert!(!komari.report_errors);
        assert_eq!(komari.pings.len(), 1);
        let cf = cfg.reporter("cf-a").unwrap();
        assert!(cf.report_gpu);
        assert_eq!(cfg.effective_intervals().collect, 1);
        assert_eq!(cfg.effective_intervals().report, 1); // 内部占位，不是上报周期
        assert!(cfg.effective_gpu());
        assert_eq!(cfg.effective_pings().len(), 1); // type + 规范化 endpoint 去重
        assert_eq!(cfg.effective_pings()[0].interval, Some(10)); // 各消费者取最小周期

        let serialized = toml::to_string_pretty(&cfg).unwrap();
        let roundtrip: LocalConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(roundtrip, cfg);

        let native_receipt = komari.static_config(cfg.global_summary(), cfg.reporter_summaries());
        assert!(native_receipt.ext.is_none());
        assert!(!serde_json::to_string(&native_receipt)
            .unwrap()
            .contains("\"ext\""));

        let receipt = cf.static_config(cfg.global_summary(), cfg.reporter_summaries());
        assert!(receipt.ext.is_some());
        assert_eq!(receipt.global.pings.len(), 1);
        assert_eq!(receipt.global.pings[0].target, "tcp://example.com:80");
        assert_eq!(receipt.global.pings[0].interval, 10);
        assert_eq!(receipt.reporters.len(), 3);
        assert_eq!(receipt.reporters[1].id, "komari-a");
        assert_eq!(receipt.reporters[1].pings[0].name, "same-host");
        assert_eq!(receipt.reporters[1].pings[0].target, "EXAMPLE.com:80");
        assert!(receipt.reporters[2].report_gpu); // CF 缺省值已展开
        let json = serde_json::to_string(&receipt).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(value["global"]["pings"][0].get("name").is_none());
        assert!(value["global"]["pings"][0].get("type").is_none());
        for private in [
            "sec",
            "token",
            "cf-secret",
            "https://example.com/report",
            "http://panel.example",
            "https://worker.example/update",
            "node-a",
            "cf-id",
        ] {
            assert!(!json.contains(private), "摘要泄露了私有字段: {private}");
        }
    }

    #[test]
    fn ping_task_key_normalizes_endpoint_and_rejects_paths() {
        let target = |kind, target: &str| PingTarget {
            name: "test".into(),
            kind,
            target: target.into(),
            interval: None,
        };
        let key = |kind, value: &str| ping_task_key(&target(kind, value)).unwrap();

        assert_eq!(
            key(PingKind::Tcp, "EXAMPLE.com"),
            key(PingKind::Tcp, "example.com.:80")
        );
        assert_eq!(
            key(PingKind::Icmp, "EXAMPLE.com."),
            key(PingKind::Icmp, "example.com")
        );
        assert_eq!(
            key(PingKind::Http, "https://EXAMPLE.com"),
            key(PingKind::Http, "https://example.com:443/")
        );
        // HTTP and HTTPS on the same numeric port are not the same probe:
        // TLS handshake and request semantics differ.
        assert_ne!(
            key(PingKind::Http, "http://example.com:443"),
            key(PingKind::Http, "https://example.com:443")
        );
        assert_ne!(
            key(PingKind::Tcp, "example.com:80"),
            key(PingKind::Icmp, "example.com")
        );

        for (kind, value) in [
            (PingKind::Http, "https://example.com/health"),
            (PingKind::Http, "https://example.com?ready=1"),
            (PingKind::Http, "https://example.com#status"),
            (PingKind::Tcp, "example.com/status"),
            (PingKind::Tcp, "example.com\\status"),
            (PingKind::Icmp, "example.com?ready=1"),
        ] {
            assert!(
                ping_task_key(&target(kind, value)).is_err(),
                "带 path/query/fragment 的 target 应被拒绝: {value}"
            );
        }
    }

    #[test]
    fn global_ping_uri_contains_kind_and_normalized_endpoint() {
        let target = |kind, target: &str| PingTarget {
            name: "test".into(),
            kind,
            target: target.into(),
            interval: None,
        };
        assert_eq!(
            global_ping_uri(&target(PingKind::Tcp, "EXAMPLE.com")).unwrap(),
            "tcp://example.com:80"
        );
        assert_eq!(
            global_ping_uri(&target(PingKind::Http, "https://EXAMPLE.com")).unwrap(),
            "https://example.com:443"
        );
        assert_eq!(
            global_ping_uri(&target(PingKind::Icmp, "EXAMPLE.com.")).unwrap(),
            "icmp://example.com"
        );
        assert_eq!(
            global_ping_uri(&target(PingKind::Tcp, "[2001:DB8::1]:443")).unwrap(),
            "tcp://[2001:db8::1]:443"
        );
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
disks = []
report_gpu = true
report_errors = true
report_self = false

[reporters.intervals]
collect = 1
ping = 30
slow = 60
gpu = 60
ip = 600
diskio = 10
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
                intervals: None,
                report_interval: Some(1),
                reset_day: Some(5),
                interfaces: None,
                disks: None,
                pings: None,
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
                intervals: None,
                report_interval: Some(0),
                reset_day: Some(5),
                interfaces: None,
                disks: None,
                pings: None,
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
                intervals: None,
                report_interval: Some(20),
                reset_day: Some(15),
                interfaces: None,
                disks: None,
                pings: None,
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
        assert_eq!(after.effective_intervals().collect, 10);
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
