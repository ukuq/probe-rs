use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::model::{
    CollectionIntervals, ExtConfig, GlobalConfigSummary, GlobalPingTarget, Intervals,
    KomariLearnedPing, PingKind, PingTarget, RemoteConfig, ReporterConfig, ReporterProtocol,
    ReporterSummary, StaticConfig,
};

pub const KOMARI_LEARNED_PING_LIMIT: usize = 5;
const KOMARI_PING_TOUCH_PERSIST_MS: i64 = 60_000;
pub const MIN_UPDATE_CHECK_INTERVAL: u64 = 300;
/// 远端可下发的 Ping/glob 数量上限:被攻陷或出错的服务端不应能把
/// worker 任务数、回执体积与内存推到无界。上限值同步进协议文档。
pub const MAX_REMOTE_PINGS: usize = 64;
pub const MAX_REMOTE_PATTERNS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum UpdateChannel {
    #[default]
    Stable,
    Prerelease,
}

impl std::fmt::Display for UpdateChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stable => f.write_str("stable"),
            Self::Prerelease => f.write_str("prerelease"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AutoUpdateConfig {
    pub enabled: bool,
    pub channel: UpdateChannel,
    pub check_interval: u64,
}

impl Default for AutoUpdateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            channel: UpdateChannel::Stable,
            check_interval: 21_600,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalConfig {
    #[serde(default = "default_net_static_path")]
    pub net_static_path: String,
    #[serde(default)]
    pub auto_update: AutoUpdateConfig,
    /// 所有独立上报实例，包括 id="primary"。
    pub reporters: Vec<ReporterConfig>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReporterSpec {
    pub id: String,
    pub protocol: ReporterProtocol,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KomariPingRegistration {
    pub task_id: String,
    pub interval: u64,
}

impl ReporterSpec {
    pub fn connection_key(&self) -> (&str, ReporterProtocol, &str, &str, &str) {
        (
            &self.id,
            self.protocol,
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
            ext: (self.protocol == ReporterProtocol::Cf).then(|| self.ext.clone()),
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
        if self.auto_update.check_interval < MIN_UPDATE_CHECK_INTERVAL {
            bail!("auto_update.check_interval must be >= {MIN_UPDATE_CHECK_INTERVAL} seconds");
        }
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
            validate_connection(&reporter.server_id, &reporter.secret, &reporter.worker_url)?;
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
            validate_komari_pings(reporter)
                .with_context(|| format!("reporter {} Komari Ping 非法", reporter.id))?;
        }
        Ok(())
    }

    pub fn reporter_specs(&self) -> Vec<ReporterSpec> {
        self.reporters
            .iter()
            .map(|r| ReporterSpec {
                id: r.id.clone(),
                protocol: r.protocol,
                server_id: r.server_id.clone(),
                secret: r.secret.clone(),
                worker_url: r.worker_url.clone(),
                config_version: r.config_version.clone(),
                intervals: r.intervals.with_report(r.report_interval),
                reset_day: r.reset_day,
                interfaces: r.interfaces.clone(),
                disks: r.disks.clone(),
                report_gpu: r.report_gpu.unwrap_or(r.protocol == ReporterProtocol::Cf),
                report_errors: r.report_errors,
                report_self: r.report_self,
                pings: reporter_ping_targets(r),
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
                for reporter in self.reporter_specs() {
                    for ping in reporter.pings {
                        let interval = ping.target.interval.unwrap_or(reporter.intervals.ping);
                        tasks
                            .entry(ping.task_id)
                            .and_modify(|task: &mut GlobalPingTarget| {
                                task.interval = task.interval.min(interval);
                            })
                            .or_insert_with(|| GlobalPingTarget {
                                target: global_ping_uri(&ping.target)
                                    .expect("validated ping target"),
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

fn reporter_ping_targets(reporter: &ReporterConfig) -> Vec<ScopedPingTarget> {
    let mut targets: Vec<_> = reporter
        .pings
        .iter()
        .cloned()
        .map(|target| ScopedPingTarget {
            task_id: ping_task_key(&target).expect("validated ping target"),
            target,
        })
        .collect();
    if reporter.protocol != ReporterProtocol::Komari {
        return targets;
    }

    let mut configured: std::collections::HashSet<_> = targets
        .iter()
        .map(|target| target.task_id.clone())
        .collect();
    for learned in &reporter.ext.komari.learned_pings {
        let mut target = learned_ping_target(learned);
        let task_id = ping_task_key(&target).expect("validated Komari Ping target");
        if configured.insert(task_id.clone()) {
            target.name = format!("komari:{task_id}");
            targets.push(ScopedPingTarget { task_id, target });
        }
    }
    targets
}

fn learned_ping_target(learned: &KomariLearnedPing) -> PingTarget {
    PingTarget {
        name: "komari:auto".to_string(),
        kind: learned.kind,
        target: learned.target.clone(),
        interval: None,
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

fn validate_connection(server_id: &str, secret: &str, url: &str) -> Result<()> {
    if server_id.trim().is_empty() {
        bail!("server_id 不能为空");
    }
    if secret.trim().is_empty() {
        bail!("secret 不能为空");
    }
    if !url.starts_with("http://") && !url.starts_with("https://") {
        bail!("worker_url 必须是 http(s) URL");
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

fn validate_komari_pings(reporter: &ReporterConfig) -> Result<()> {
    let learned = &reporter.ext.komari.learned_pings;
    if reporter.protocol != ReporterProtocol::Komari && !learned.is_empty() {
        bail!("ext.komari.learned_pings 只允许用于 protocol=\"komari\"");
    }
    if learned.len() > KOMARI_LEARNED_PING_LIMIT {
        bail!(
            "自动学习目标最多 {} 个，当前 {} 个",
            KOMARI_LEARNED_PING_LIMIT,
            learned.len()
        );
    }
    let mut keys = std::collections::HashSet::new();
    for ping in learned {
        if ping.target.trim().is_empty() {
            bail!("自动学习 Ping target 不能为空");
        }
        if ping.last_seen_at < 0 {
            bail!("自动学习 Ping last_seen_at 不能为负数");
        }
        let key = ping_task_key(&learned_ping_target(ping))?;
        if !keys.insert(key) {
            bail!("自动学习 Ping target 重复");
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalReload {
    Applied,
    Unchanged,
    RestartRequired,
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

    /// 原子热加载：持有写锁时读取、校验并提交同一份文件快照。
    ///
    /// 远端应用和 Komari 学习也在该锁内落盘，因此不会在本次读取与提交之间
    /// 插入一个更新；`is_compatible` 检查的正是随后要应用的 `cfg`，连接身份
    /// 变化不能通过调用方预读另一份快照绕过 restart-only 限制。
    pub fn update_local_from_disk(
        &self,
        path: &Path,
        is_compatible: impl FnOnce(&LocalConfig) -> bool,
    ) -> Result<LocalReload> {
        let mut guard = self.inner.write().expect("config lock poisoned");
        let cfg = load(path)?;
        if !is_compatible(&cfg) {
            return Ok(LocalReload::RestartRequired);
        }
        if *guard == cfg {
            return Ok(LocalReload::Unchanged);
        }
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
        Ok(LocalReload::Applied)
    }

    /// 记录 Komari 面板下发的 Ping 目标。这里只修改该 Reporter 的本地采集需求；
    /// 真正探测由全局 Ping worker 在配置通知后异步完成。
    pub fn learn_komari_ping(
        &self,
        reporter_id: &str,
        kind: PingKind,
        target: &str,
        observed_at: i64,
    ) -> Result<KomariPingRegistration> {
        let target = target.trim().to_string();
        let candidate = PingTarget {
            name: "komari:auto".to_string(),
            kind,
            target: target.clone(),
            interval: None,
        };
        let task_id = ping_task_key(&candidate)?;
        let observed_at = observed_at.max(0);

        let mut cfg = self.inner.write().expect("config lock poisoned");
        let reporter_index = cfg
            .reporters
            .iter()
            .position(|reporter| reporter.id == reporter_id)
            .ok_or_else(|| anyhow::anyhow!("Reporter 不存在: {reporter_id}"))?;
        let reporter = &cfg.reporters[reporter_index];
        if reporter.protocol != ReporterProtocol::Komari {
            bail!("Reporter {reporter_id} 不是 Komari 协议");
        }
        let default_interval = reporter.intervals.ping;

        // 手工 Ping 已经表达同一采集需求时直接复用，不占自动学习的 5 个名额。
        for configured in &reporter.pings {
            if ping_task_key(configured)? == task_id {
                return Ok(KomariPingRegistration {
                    task_id,
                    interval: configured.interval.unwrap_or(default_interval),
                });
            }
        }

        let existing = reporter.ext.komari.learned_pings.iter().position(|ping| {
            ping_task_key(&learned_ping_target(ping)).is_ok_and(|key| key == task_id)
        });
        if let Some(index) = existing {
            // 内存中始终保留精确 LRU；落盘按分钟合并，避免面板高频任务持续写盘。
            let previous =
                cfg.reporters[reporter_index].ext.komari.learned_pings[index].last_seen_at;
            let current = observed_at.max(previous);
            cfg.reporters[reporter_index].ext.komari.learned_pings[index].last_seen_at = current;
            if previous / KOMARI_PING_TOUCH_PERSIST_MS != current / KOMARI_PING_TOUCH_PERSIST_MS {
                if let Err(error) = persist(&self.path, &cfg) {
                    tracing::warn!(reporter_id, %error, "Komari Ping 最近使用时间落盘失败");
                }
            }
            return Ok(KomariPingRegistration {
                task_id,
                interval: default_interval,
            });
        }

        let mut next = cfg.clone();
        let learned = &mut next.reporters[reporter_index].ext.komari.learned_pings;
        if learned.len() >= KOMARI_LEARNED_PING_LIMIT {
            let oldest = learned
                .iter()
                .enumerate()
                .min_by_key(|(_, ping)| ping.last_seen_at)
                .map(|(index, _)| index)
                .expect("capacity check guarantees an entry");
            learned.remove(oldest);
        }
        learned.push(KomariLearnedPing {
            kind,
            target,
            last_seen_at: observed_at,
        });
        next.validate().context("Komari Ping 学习结果非法")?;
        persist(&self.path, &next).context("Komari Ping 配置落盘失败")?;
        let full = next.clone();
        *cfg = next;
        drop(cfg);

        // 只有目标集合变化才通知，普通 touch 不重建 worker，也不重置 Reporter ticker。
        self.config_tx.send_replace(full);
        tracing::info!(
            reporter_id,
            kind = ?kind,
            target = %candidate.target,
            "Komari Ping 目标已学习"
        );
        Ok(KomariPingRegistration {
            task_id,
            interval: default_interval,
        })
    }

    #[cfg(test)]
    pub fn apply_remote(&self, remote: RemoteConfig) -> Result<()> {
        self.apply_remote_for("primary", remote).map(|_| ())
    }

    /// 远端配置只写入产生它的 Reporter；全局采集配置永不受上报端影响。
    /// Returns true only when a new config_version was successfully persisted
    /// and applied. Callers can use this to trigger one-shot side effects
    /// without reacting to idempotent responses.
    pub fn apply_remote_for(&self, reporter_id: &str, remote: RemoteConfig) -> Result<bool> {
        {
            let current = self.inner.read().expect("config lock poisoned");
            let version = current
                .reporter(reporter_id)
                .ok_or_else(|| anyhow::anyhow!("Reporter 不存在: {reporter_id}"))?
                .config_version;
            if remote.config_version.is_empty() || remote.config_version == version {
                return Ok(false);
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
            return Ok(false);
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
        // 协议守卫:ext.cf 只对 CF 协议 Reporter 有意义,其他线路收到应整体
        // 拒绝,避免把无意义的协议扩展写进 TOML。
        if ext.as_ref().is_some_and(|ext| ext.cf.is_some())
            && reporter.protocol != ReporterProtocol::Cf
        {
            bail!(
                "ext.cf 仅对 CF 协议 Reporter 生效,当前协议: {}",
                reporter.protocol
            );
        }
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
                if let Some(value) = cf.connection_mode {
                    reporter.ext.cf.connection_mode = value;
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
        Ok(true)
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
        if interfaces.len() > MAX_REMOTE_PATTERNS {
            bail!("远端 interfaces 数量超限(最多 {MAX_REMOTE_PATTERNS} 项)");
        }
        validate_patterns("远端 interfaces", interfaces)?;
    }
    if let Some(disks) = &remote.disks {
        if disks.len() > MAX_REMOTE_PATTERNS {
            bail!("远端 disks 数量超限(最多 {MAX_REMOTE_PATTERNS} 项)");
        }
        validate_patterns("远端 disks", disks)?;
    }
    if let Some(pings) = &remote.pings {
        if pings.len() > MAX_REMOTE_PINGS {
            bail!("远端 pings 数量超限(最多 {MAX_REMOTE_PINGS} 项)");
        }
        validate_pings(pings).context("远端 pings 非法")?;
    }
    Ok(())
}

pub fn load(path: &Path) -> Result<LocalConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("读取配置失败: {}", path.display()))?;
    parse_text(&raw)
}

fn parse_text(raw: &str) -> Result<LocalConfig> {
    let cfg: LocalConfig = toml::from_str(raw).context("解析配置 TOML 失败")?;
    cfg.validate()?;
    Ok(cfg)
}

/// 同目录临时文件 + 原子替换。
///
/// SharedConfig 在其写锁内调用这里；唯一临时文件避免与编辑器或上次异常退出
/// 遗留的固定 `.tmp` 冲突。配置变更路径先同步文件、替换成功后才更新内存，
/// 从而保证磁盘始终是可恢复的事实源。
pub(crate) fn persist(path: &Path, cfg: &LocalConfig) -> Result<()> {
    let data = toml::to_string_pretty(cfg)?;
    persist_bytes(path, data.as_bytes())
}

/// 校验托盘编辑器中的原始 TOML，并在正式配置自打开编辑器后未变化时保存。
///
/// 编辑内容先通过与启动加载完全相同的解析和业务校验，再备份当前正式配置，
/// 最后执行同目录原子替换。任一步失败都不会以未校验内容覆盖正式配置。
pub(crate) fn persist_edited_text(
    path: &Path,
    expected_original: &str,
    edited: &str,
) -> Result<PathBuf> {
    parse_text(edited).context("编辑后的配置未通过校验")?;

    let current = std::fs::read_to_string(path)
        .with_context(|| format!("重新读取正式配置失败: {}", path.display()))?;
    if current != expected_original {
        bail!("正式配置在编辑期间已被其他进程修改；请取消并重新打开编辑器");
    }

    let backup_path = path.with_extension("toml.bak");
    persist_bytes(&backup_path, current.as_bytes())
        .with_context(|| format!("备份正式配置失败: {}", backup_path.display()))?;
    persist_bytes(path, edited.as_bytes())?;
    Ok(backup_path)
}

fn persist_bytes(path: &Path, data: &[u8]) -> Result<()> {
    use std::io::Write;

    let dir = path
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(dir)?;
    let mut staged = tempfile::Builder::new()
        .prefix(".probe-rs-config-")
        .tempfile_in(dir)
        .with_context(|| format!("创建配置临时文件失败: {}", dir.display()))?;

    // 配置含 secret：替换前固定 0600，避免权限随旧文件或 umask 漂移。
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        staged
            .as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    staged.write_all(data)?;
    staged.as_file_mut().sync_all()?;
    staged
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("原子替换配置失败: {}", path.display()))?;

    // rename 本身持久化到目录项后，掉电恢复不会回到旧文件名状态。
    #[cfg(unix)]
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> LocalConfig {
        LocalConfig {
            net_static_path: "/tmp/x.json".into(),
            auto_update: AutoUpdateConfig::default(),
            reporters: vec![ReporterConfig {
                id: "primary".into(),
                protocol: ReporterProtocol::Probe,
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
    fn local_reload_validates_and_applies_the_same_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let initial = base_config();
        persist(&path, &initial).unwrap();
        let (shared, intervals_rx, config_rx) = SharedConfig::new(initial.clone(), path.clone());

        let mut incompatible = initial.clone();
        incompatible.reporters[0].worker_url = "https://other.example/report".into();
        incompatible.reporters[0].intervals.collect = 5;
        persist(&path, &incompatible).unwrap();

        let result = shared
            .update_local_from_disk(&path, |candidate| {
                candidate.reporters[0].worker_url == initial.reporters[0].worker_url
            })
            .unwrap();
        assert_eq!(result, LocalReload::RestartRequired);
        assert_eq!(shared.get(), initial);
        assert!(!intervals_rx.has_changed().unwrap());
        assert!(!config_rx.has_changed().unwrap());

        let mut compatible = initial.clone();
        compatible.reporters[0].intervals.collect = 5;
        persist(&path, &compatible).unwrap();

        let result = shared
            .update_local_from_disk(&path, |candidate| {
                candidate.reporters[0].worker_url == initial.reporters[0].worker_url
            })
            .unwrap();
        assert_eq!(result, LocalReload::Applied);
        assert_eq!(shared.get(), compatible);
        assert!(intervals_rx.has_changed().unwrap());
        assert!(config_rx.has_changed().unwrap());

        let result = shared.update_local_from_disk(&path, |_| true).unwrap();
        assert_eq!(result, LocalReload::Unchanged);
    }

    #[test]
    fn persist_atomically_replaces_an_existing_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let initial = base_config();
        persist(&path, &initial).unwrap();

        let mut replacement = initial;
        replacement.reporters[0].report_interval = 7;
        persist(&path, &replacement).unwrap();

        assert_eq!(load(&path).unwrap(), replacement);
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".probe-rs-config-")
            })
            .collect();
        assert!(leftovers.is_empty());
    }

    #[test]
    fn invalid_edited_config_does_not_replace_the_official_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let original = toml::to_string_pretty(&base_config()).unwrap();
        std::fs::write(&path, &original).unwrap();

        let error = persist_edited_text(&path, &original, "this is not valid TOML").unwrap_err();

        assert!(error.to_string().contains("编辑后的配置未通过校验"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), original);
        assert!(!path.with_extension("toml.bak").exists());
    }

    #[test]
    fn valid_edited_config_preserves_text_and_backs_up_the_original() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let original = format!(
            "# original comment\n{}",
            toml::to_string_pretty(&base_config()).unwrap()
        );
        std::fs::write(&path, &original).unwrap();
        let edited = original
            .replace("# original comment", "# edited comment")
            .replace("report_interval = 60", "report_interval = 7");

        let backup_path = persist_edited_text(&path, &original, &edited).unwrap();

        assert_eq!(backup_path, path.with_extension("toml.bak"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), edited);
        assert_eq!(std::fs::read_to_string(backup_path).unwrap(), original);
        assert_eq!(load(&path).unwrap().reporters[0].report_interval, 7);
    }

    #[test]
    fn edited_config_does_not_overwrite_a_concurrent_change() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let original = toml::to_string_pretty(&base_config()).unwrap();
        std::fs::write(&path, &original).unwrap();

        let mut concurrent = base_config();
        concurrent.reporters[0].report_interval = 11;
        let concurrent = toml::to_string_pretty(&concurrent).unwrap();
        std::fs::write(&path, &concurrent).unwrap();
        let edited = original.replace("report_interval = 60", "report_interval = 7");

        let error = persist_edited_text(&path, &original, &edited).unwrap_err();

        assert!(error.to_string().contains("编辑期间已被其他进程修改"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), concurrent);
        assert!(!path.with_extension("toml.bak").exists());
    }

    #[test]
    fn auto_update_defaults_to_disabled_stable_channel() {
        let text = r#"
            net_static_path = "/tmp/x.json"
            [[reporters]]
            id = "primary"
            protocol = "probe"
            server_id = "s1"
            secret = "sec"
            worker_url = "https://example.com/report"
            config_version = ""
            report_interval = 60
            reset_day = 1
            interfaces = []
            disks = []
            report_gpu = false
            report_errors = true
            report_self = false
            [reporters.intervals]
            collect = 10
            ping = 30
            slow = 60
            gpu = 60
            ip = 600
            diskio = 10
        "#;
        let cfg: LocalConfig = toml::from_str(text).unwrap();
        assert_eq!(cfg.auto_update, AutoUpdateConfig::default());
    }

    #[test]
    fn example_config_stays_valid() {
        let cfg: LocalConfig = toml::from_str(include_str!("../config.example.toml")).unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn rejects_too_frequent_update_checks() {
        let mut cfg = base_config();
        cfg.auto_update.check_interval = MIN_UPDATE_CHECK_INTERVAL - 1;
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
            protocol: ReporterProtocol::Komari,
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
            protocol: ReporterProtocol::Cf,
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
        assert_eq!(komari.protocol, ReporterProtocol::Komari);
        assert_eq!(komari.reset_day, 12);
        assert_eq!(komari.intervals.collect, 1);
        assert!(komari.report_gpu);
    }

    #[test]
    fn komari_learned_pings_are_persisted_and_lru_bounded() {
        let dir =
            std::env::temp_dir().join(format!("probe-rs-komari-ping-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        let mut cfg = base_config();
        let reporter = &mut cfg.reporters[0];
        reporter.protocol = ReporterProtocol::Komari;
        reporter.worker_url = "https://komari.example.com".into();
        reporter.pings.clear();
        persist(&path, &cfg).unwrap();
        let (shared, _intervals_rx, config_rx) = SharedConfig::new(cfg, path.clone());

        for i in 1..=KOMARI_LEARNED_PING_LIMIT {
            shared
                .learn_komari_ping("primary", PingKind::Icmp, &format!("192.0.2.{i}"), i as i64)
                .unwrap();
        }
        assert!(config_rx.has_changed().unwrap());
        let learned = &shared.get().reporters[0].ext.komari.learned_pings;
        assert_eq!(learned.len(), KOMARI_LEARNED_PING_LIMIT);
        assert_eq!(
            shared.get().effective_pings().len(),
            KOMARI_LEARNED_PING_LIMIT
        );

        // 最近再次出现的第一个目标必须保留；加入第六个时淘汰未使用最久的第二个。
        shared
            .learn_komari_ping("primary", PingKind::Icmp, "192.0.2.1", 100)
            .unwrap();
        shared
            .learn_komari_ping("primary", PingKind::Icmp, "192.0.2.6", 101)
            .unwrap();
        let current = shared.get();
        let learned = &current.reporters[0].ext.komari.learned_pings;
        assert_eq!(learned.len(), KOMARI_LEARNED_PING_LIMIT);
        assert!(learned.iter().any(|ping| ping.target == "192.0.2.1"));
        assert!(!learned.iter().any(|ping| ping.target == "192.0.2.2"));
        assert!(learned.iter().any(|ping| ping.target == "192.0.2.6"));

        let on_disk = load(&path).unwrap();
        assert_eq!(
            on_disk.reporters[0].ext.komari.learned_pings,
            current.reporters[0].ext.komari.learned_pings
        );
        let toml = std::fs::read_to_string(&path).unwrap();
        assert!(toml.contains("[[reporters.ext.komari.learned_pings]]"));
        assert!(shared
            .learn_komari_ping("primary", PingKind::Http, "https://example.com/health", 102,)
            .is_err());
        assert_eq!(
            shared.get().reporters[0].ext.komari.learned_pings.len(),
            KOMARI_LEARNED_PING_LIMIT
        );
        std::fs::remove_dir_all(&dir).ok();
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
        assert!(!shared
            .apply_remote_for(
                "primary",
                RemoteConfig {
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
                }
            )
            .unwrap());
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
        let applied = shared
            .apply_remote_for(
                "primary",
                RemoteConfig {
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
                },
            )
            .unwrap();
        assert!(applied);
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

        // The same CF config version is idempotent and must not trigger
        // one-shot work such as an update check again.
        assert!(!shared
            .apply_remote_for(
                "primary",
                RemoteConfig {
                    config_version: "2026-08-06T15:00:00+08:00".into(),
                    intervals: None,
                    report_interval: Some(30),
                    reset_day: None,
                    interfaces: None,
                    disks: None,
                    pings: None,
                    report_gpu: None,
                    report_errors: None,
                    report_self: None,
                    ext: None,
                }
            )
            .unwrap());
        assert_eq!(
            shared.get().reporter("primary").unwrap().intervals.report,
            20
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cf_connection_mode_remote_update_is_persisted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let mut cfg = base_config();
        cfg.reporters[0].protocol = ReporterProtocol::Cf;
        cfg.reporters[0].worker_url = "https://worker.example/update".into();
        persist(&path, &cfg).unwrap();
        let (shared, _intervals_rx, _config_rx) = SharedConfig::new(cfg, path.clone());

        assert!(shared
            .apply_remote_for(
                "primary",
                RemoteConfig {
                    config_version: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                    intervals: None,
                    report_interval: None,
                    reset_day: None,
                    interfaces: None,
                    disks: None,
                    pings: None,
                    report_gpu: None,
                    report_errors: None,
                    report_self: None,
                    ext: Some(crate::model::RemoteExt {
                        cf: Some(crate::model::RemoteCfExt {
                            correction: None,
                            batch: None,
                            connection_mode: Some(crate::model::CfConnectionMode::Http),
                        }),
                    }),
                },
            )
            .unwrap());

        assert_eq!(
            shared
                .get()
                .reporter("primary")
                .unwrap()
                .ext
                .cf
                .connection_mode,
            crate::model::CfConnectionMode::Http
        );
        assert_eq!(
            load(&path)
                .unwrap()
                .reporter("primary")
                .unwrap()
                .ext
                .cf
                .connection_mode,
            crate::model::CfConnectionMode::Http
        );
    }
}
