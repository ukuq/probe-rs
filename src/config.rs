use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::model::{
    CfConnectionMode, CollectConfig, CollectionIntervals, GlobalConfigSummary, GlobalPingTarget,
    Intervals, KomariLearnedPing, PingKind, PingTarget, RemoteConfig, ReporterConfig,
    ReporterProtocol, ReporterSummary, StaticConfig,
};

pub const KOMARI_LEARNED_PING_LIMIT: usize = 5;
const KOMARI_PING_TOUCH_PERSIST_MS: i64 = 60_000;
pub const MIN_UPDATE_CHECK_INTERVAL: u64 = 300;
/// 远端可下发的 Ping/glob 数量上限:被攻陷或出错的服务端不应能把
/// worker 任务数、回执体积与内存推到无界。上限值同步进协议文档。
pub const MAX_REMOTE_PINGS: usize = 64;
pub const MAX_REMOTE_PATTERNS: usize = 32;

/// 当前配置文件结构版本;文件缺少 schema 键时视为旧版(schema 0),
/// 加载时自动迁移并回写。
pub const CONFIG_SCHEMA: u32 = 1;

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
    /// 可选的 GitHub Release 仓库覆盖；缺省使用构建产物内嵌的来源。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    pub channel: UpdateChannel,
    pub check_interval: u64,
    /// Release 资产直连失败后依次尝试的 GitHub 代理前缀。
    pub proxys: Vec<String>,
}

impl Default for AutoUpdateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            repository: None,
            channel: UpdateChannel::Stable,
            check_interval: 21_600,
            proxys: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalConfig {
    /// 配置结构版本,固定为 CONFIG_SCHEMA。
    #[serde(default = "default_schema")]
    pub schema: u32,
    /// 数据目录:net_static.json 等运行态文件都存放在这里。
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default)]
    pub auto_update: AutoUpdateConfig,
    /// 所有独立上报实例,协议由各条目的协议段(cf/komari/probe)决定。
    pub reporters: Vec<ReporterConfig>,
}

fn default_schema() -> u32 {
    CONFIG_SCHEMA
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReporterSpec {
    pub id: String,
    pub protocol: ReporterProtocol,
    pub server_id: String,
    pub secret: String,
    pub worker_url: String,
    pub config_version: String,
    pub connection_mode: Option<CfConnectionMode>,
    pub ping_mode: Option<crate::model::CfPingMode>,
    pub wss_report_interval: Option<u64>,
    /// 协议原始采集周期。CF 保留 0 用于线上协议；内部采集使用
    /// intervals.collect 中的非零映射值。
    pub source_collect_interval: u64,
    pub intervals: Intervals,
    pub reset_day: u8,
    pub interfaces: Vec<String>,
    pub disks: Vec<String>,
    pub report_gpu: bool,
    pub report_errors: bool,
    pub report_self: bool,
    pub pings: Vec<ScopedPingTarget>,
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
        }
    }
}

pub fn default_config_path() -> PathBuf {
    platform_config_dir().join("config.toml")
}

pub(crate) fn default_data_dir() -> String {
    platform_data_dir().to_string_lossy().into_owned()
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
    /// net_static 流量账本固定存放在数据目录下。
    pub fn net_static_path(&self) -> PathBuf {
        PathBuf::from(&self.data_dir).join("net_static.json")
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema != CONFIG_SCHEMA {
            bail!(
                "不支持的配置 schema 版本: {}(当前支持 {CONFIG_SCHEMA})",
                self.schema
            );
        }
        if self.data_dir.trim().is_empty() {
            bail!("data_dir 不能为空");
        }
        if self.auto_update.check_interval < MIN_UPDATE_CHECK_INTERVAL {
            bail!("auto_update.check_interval must be >= {MIN_UPDATE_CHECK_INTERVAL} seconds");
        }
        if let Some(repository) = &self.auto_update.repository {
            validate_update_repository(repository)?;
        }
        for proxy in &self.auto_update.proxys {
            validate_update_proxy(proxy)?;
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
            match reporter.protocol() {
                Some(ReporterProtocol::Cf) => {
                    let cf = reporter.cf.as_ref().expect("protocol checked");
                    validate_connection(&cf.server_id, &cf.secret, &cf.url)
                        .with_context(|| format!("reporter {} cf 段非法", reporter.id))?;
                    if cf.interval == 0 {
                        bail!("reporter {} cf.interval 必须 >= 1", reporter.id);
                    }
                    if !(1..=5).contains(&cf.wss_report_interval) {
                        bail!(
                            "reporter {} cf.wss_report_interval 必须在 1-5 之间",
                            reporter.id
                        );
                    }
                    if cf.reset_day > 31 {
                        bail!("reporter {} cf.reset_day 必须在 0-31 之间", reporter.id);
                    }
                    validate_patterns("interface", &split_list(&cf.interface, ','))
                        .with_context(|| format!("reporter {} cf.interface 非法", reporter.id))?;
                    for (name, target) in [
                        ("ct", &cf.ct),
                        ("cu", &cf.cu),
                        ("cm", &cf.cm),
                        ("bd", &cf.bd),
                    ] {
                        if let Some(target) = target {
                            validate_cf_node(name, target, cf.ping_mode).with_context(|| {
                                format!("reporter {} cf.{name} 非法", reporter.id)
                            })?;
                        }
                    }
                }
                Some(ReporterProtocol::Komari) => {
                    let komari = reporter.komari.as_ref().expect("protocol checked");
                    if komari.token.trim().is_empty() {
                        bail!("reporter {} komari.token 不能为空", reporter.id);
                    }
                    if !komari.endpoint.starts_with("http://")
                        && !komari.endpoint.starts_with("https://")
                    {
                        bail!(
                            "reporter {} komari.endpoint 必须是 http(s) URL",
                            reporter.id
                        );
                    }
                    if komari.interval == 0 {
                        bail!("reporter {} komari.interval 必须 >= 1", reporter.id);
                    }
                    if komari.month_rotate > 31 {
                        bail!(
                            "reporter {} komari.month_rotate 必须在 0-31 之间",
                            reporter.id
                        );
                    }
                    validate_patterns("include_nics", &split_list(&komari.include_nics, ','))
                        .with_context(|| {
                            format!("reporter {} komari.include_nics 非法", reporter.id)
                        })?;
                    validate_patterns(
                        "include_mountpoints",
                        &split_list(&komari.include_mountpoints, ';'),
                    )
                    .with_context(|| {
                        format!("reporter {} komari.include_mountpoints 非法", reporter.id)
                    })?;
                    validate_komari_learned_pings(&komari.ext.learned_pings).with_context(
                        || format!("reporter {} komari learned_pings 非法", reporter.id),
                    )?;
                }
                Some(ReporterProtocol::Probe) => {
                    let probe = reporter.probe.as_ref().expect("protocol checked");
                    validate_connection(&probe.server_id, &probe.secret, &probe.worker_url)
                        .with_context(|| format!("reporter {} probe 段非法", reporter.id))?;
                    if probe.report_interval == 0 {
                        bail!("reporter {} probe.report_interval 必须 >= 1", reporter.id);
                    }
                    if probe.reset_day > 31 {
                        bail!("reporter {} probe.reset_day 必须在 0-31 之间", reporter.id);
                    }
                    probe
                        .intervals
                        .validate()
                        .map_err(anyhow::Error::msg)
                        .with_context(|| {
                            format!("reporter {} probe.intervals 非法", reporter.id)
                        })?;
                    validate_patterns("interfaces", &probe.interfaces).with_context(|| {
                        format!("reporter {} probe.interfaces 非法", reporter.id)
                    })?;
                    validate_patterns("disks", &probe.disks)
                        .with_context(|| format!("reporter {} probe.disks 非法", reporter.id))?;
                    validate_pings(&probe.pings)
                        .with_context(|| format!("reporter {} probe.pings 非法", reporter.id))?;
                }
                None => {
                    bail!(
                        "reporter {} 必须恰好包含一个协议段(cf / komari / probe)",
                        reporter.id
                    );
                }
            }
        }
        Ok(())
    }

    pub fn reporter_specs(&self) -> Vec<ReporterSpec> {
        self.reporters
            .iter()
            .map(
                |reporter| match reporter.protocol().expect("validated reporter protocol") {
                    ReporterProtocol::Cf => {
                        let cf = reporter.cf.as_ref().expect("protocol checked");
                        let collect = cf.to_collect_config();
                        ReporterSpec {
                            id: reporter.id.clone(),
                            protocol: ReporterProtocol::Cf,
                            server_id: cf.server_id.clone(),
                            secret: cf.secret.clone(),
                            worker_url: cf.url.clone(),
                            config_version: cf.ext.config_version.clone(),
                            connection_mode: Some(cf.connection_mode),
                            ping_mode: Some(cf.ping_mode),
                            wss_report_interval: Some(cf.wss_report_interval),
                            source_collect_interval: cf.collect_interval,
                            intervals: collect.intervals.with_report(cf.interval),
                            reset_day: cf.reset_day,
                            interfaces: collect.interfaces,
                            disks: collect.disks,
                            report_gpu: collect.report_gpu,
                            // CF wire 没有 errors/self 落点。
                            report_errors: false,
                            report_self: false,
                            pings: scoped_pings(collect.pings),
                        }
                    }
                    ReporterProtocol::Komari => {
                        let komari = reporter.komari.as_ref().expect("protocol checked");
                        let collect = komari.to_collect_config();
                        let mut pings = scoped_pings(collect.pings);
                        for learned in &komari.ext.learned_pings {
                            let mut target = learned_ping_target(learned);
                            let task_id =
                                ping_task_key(&target).expect("validated Komari Ping target");
                            if pings.iter().any(|ping| ping.task_id == task_id) {
                                continue;
                            }
                            target.name = format!("komari:{task_id}");
                            pings.push(ScopedPingTarget { task_id, target });
                        }
                        ReporterSpec {
                            id: reporter.id.clone(),
                            protocol: ReporterProtocol::Komari,
                            // komari 协议没有 server_id 概念,靠 token 识别。
                            server_id: String::new(),
                            secret: komari.token.clone(),
                            worker_url: komari.endpoint.clone(),
                            config_version: String::new(),
                            connection_mode: None,
                            ping_mode: None,
                            wss_report_interval: None,
                            source_collect_interval: collect.intervals.collect,
                            // komari 按采集周期上报。
                            intervals: collect.intervals.with_report(komari.interval),
                            reset_day: komari.month_rotate,
                            interfaces: collect.interfaces,
                            disks: collect.disks,
                            report_gpu: collect.report_gpu,
                            report_errors: true,
                            report_self: false,
                            pings,
                        }
                    }
                    ReporterProtocol::Probe => {
                        let probe = reporter.probe.as_ref().expect("protocol checked");
                        let collect = probe.to_collect_config();
                        ReporterSpec {
                            id: reporter.id.clone(),
                            protocol: ReporterProtocol::Probe,
                            server_id: probe.server_id.clone(),
                            secret: probe.secret.clone(),
                            worker_url: probe.worker_url.clone(),
                            config_version: probe.ext.config_version.clone(),
                            connection_mode: None,
                            ping_mode: None,
                            wss_report_interval: None,
                            source_collect_interval: collect.intervals.collect,
                            intervals: collect.intervals.with_report(probe.report_interval),
                            reset_day: probe.reset_day,
                            interfaces: collect.interfaces,
                            disks: collect.disks,
                            report_gpu: collect.report_gpu,
                            report_errors: probe.report_errors,
                            report_self: probe.report_self,
                            pings: scoped_pings(collect.pings),
                        }
                    }
                },
            )
            .collect()
    }

    pub fn reporter(&self, id: &str) -> Option<ReporterSpec> {
        self.reporter_specs().into_iter().find(|r| r.id == id)
    }

    /// 可安全随 static 上报的全局采集摘要。
    pub fn global_summary(&self) -> GlobalConfigSummary {
        let specs = self.reporter_specs();
        let interfaces: std::collections::BTreeSet<String> = specs
            .iter()
            .flat_map(|spec| spec.interfaces.iter().cloned())
            .collect();
        let disks: std::collections::BTreeSet<String> = specs
            .iter()
            .flat_map(|spec| spec.disks.iter().cloned())
            .collect();
        GlobalConfigSummary {
            intervals: self.effective_collection_intervals(),
            enable_gpu: self.effective_gpu(),
            interfaces: interfaces.into_iter().collect(),
            all_interfaces: specs.iter().any(|spec| spec.interfaces.is_empty()),
            disks: disks.into_iter().collect(),
            all_disks: specs.iter().any(|spec| spec.disks.is_empty()),
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
                source_collect_interval: spec.source_collect_interval,
                connection_mode: spec.connection_mode,
                ping_mode: spec.ping_mode,
                wss_report_interval: spec.wss_report_interval,
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

    /// 合并后的全局采集配置实体:各协议段先各自转换为实体,再按路合并
    /// (基础采集周期取 gcd、其他周期取 min、GPU 取 OR、网卡/磁盘取并集、
    /// Ping 去重取 min)。
    pub fn merged_collect_config(&self) -> CollectConfig {
        CollectConfig {
            intervals: self.effective_collection_intervals(),
            interfaces: self.global_interfaces(),
            disks: self.global_disks(),
            report_gpu: self.effective_gpu(),
            pings: self.effective_pings(),
        }
    }

    /// 网卡并集;任一路为空(= 全部)时结果为空(= 全部)。
    fn global_interfaces(&self) -> Vec<String> {
        let specs = self.reporter_specs();
        if specs.iter().any(|spec| spec.interfaces.is_empty()) {
            return Vec::new();
        }
        let union: std::collections::BTreeSet<String> = specs
            .iter()
            .flat_map(|spec| spec.interfaces.iter().cloned())
            .collect();
        union.into_iter().collect()
    }

    /// 磁盘并集;任一路为空(= 全部)时结果为空(= 全部)。
    fn global_disks(&self) -> Vec<String> {
        let specs = self.reporter_specs();
        if specs.iter().any(|spec| spec.disks.is_empty()) {
            return Vec::new();
        }
        let union: std::collections::BTreeSet<String> = specs
            .iter()
            .flat_map(|spec| spec.disks.iter().cloned())
            .collect();
        union.into_iter().collect()
    }

    pub fn effective_collection_intervals(&self) -> CollectionIntervals {
        self.reporter_specs()
            .iter()
            .map(|spec| CollectionIntervals {
                collect: spec.intervals.collect,
                ping: spec.intervals.ping,
                slow: spec.intervals.slow,
                gpu: spec.intervals.gpu,
                ip: spec.intervals.ip,
                diskio: spec.intervals.diskio,
            })
            .reduce(|a, b| CollectionIntervals {
                collect: gcd(a.collect, b.collect),
                ping: a.ping.min(b.ping),
                slow: a.slow.min(b.slow),
                gpu: a.gpu.min(b.gpu),
                ip: a.ip.min(b.ip),
                diskio: a.diskio.min(b.diskio),
            })
            .unwrap_or_default()
    }
}

fn scoped_pings(pings: Vec<PingTarget>) -> Vec<ScopedPingTarget> {
    pings
        .into_iter()
        .map(|target| ScopedPingTarget {
            task_id: ping_task_key(&target).expect("validated ping target"),
            target,
        })
        .collect()
}

fn learned_ping_target(learned: &KomariLearnedPing) -> PingTarget {
    PingTarget {
        name: "komari:auto".to_string(),
        kind: learned.kind,
        target: learned.target.clone(),
        interval: None,
    }
}

/// 分隔符列表解析:逐项 trim、丢弃空项。
pub(crate) fn split_list(raw: &str, delimiter: char) -> Vec<String> {
    raw.split(delimiter)
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

fn validate_cf_node(name: &str, target: &str, ping_mode: crate::model::CfPingMode) -> Result<()> {
    let ping = crate::model::cf_node_ping(name, target, ping_mode)
        .with_context(|| format!("cf.{name} 不能为空串,不需要时请删除该键"))?;
    ping_task_key(&ping)?;
    Ok(())
}

pub(crate) fn ping_task_key(ping: &PingTarget) -> Result<String> {
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
            let (host, _) = crate::worker::ping::split_host_port(&ping.target)?;
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

/// 全局只读摘要使用 URI 自带类型,Reporter 私有配置仍保留独立 type 字段。
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
            let (host, _) = crate::worker::ping::split_host_port(&ping.target)?;
            if host.is_empty() {
                bail!("非法 ICMP host: {}", ping.target);
            }
            Ok(format!("icmp://{}", authority_host(&host)))
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

fn validate_komari_learned_pings(learned: &[KomariLearnedPing]) -> Result<()> {
    if learned.len() > KOMARI_LEARNED_PING_LIMIT {
        bail!(
            "自动学习目标最多 {} 个,当前 {} 个",
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

/// 共享运行时配置:本地配置 + intervals 变更通知(scheduler 重建 ticker)
/// + 全量变更通知(supervisor 重建 worker;本地热加载与远端下发共用)
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

    /// 原子热加载:持有写锁时读取、校验并提交同一份文件快照。
    ///
    /// 远端应用和 Komari 学习也在该锁内落盘,因此不会在本次读取与提交之间
    /// 插入一个更新;`is_compatible` 检查的正是随后要应用的 `cfg`,连接身份
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

    /// 记录 Komari 面板下发的 Ping 目标。这里只修改该 Reporter 的本地采集需求;
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
        if cfg.reporters[reporter_index].komari.is_none() {
            bail!("Reporter {reporter_id} 不是 Komari 协议");
        }
        let default_interval = CollectionIntervals::default().ping;

        // 该 Reporter 实体已表达同一采集需求时直接复用,不占自动学习的 5 个名额。
        let entity_pings = cfg.reporters[reporter_index]
            .komari
            .as_ref()
            .expect("protocol checked")
            .to_collect_config()
            .pings;
        for configured in &entity_pings {
            if ping_task_key(configured)? == task_id {
                return Ok(KomariPingRegistration {
                    task_id,
                    interval: configured.interval.unwrap_or(default_interval),
                });
            }
        }

        let existing = cfg.reporters[reporter_index]
            .komari
            .as_ref()
            .expect("protocol checked")
            .ext
            .learned_pings
            .iter()
            .position(|ping| {
                ping_task_key(&learned_ping_target(ping)).is_ok_and(|key| key == task_id)
            });
        if let Some(index) = existing {
            // 内存中始终保留精确 LRU;落盘按分钟合并,避免面板高频任务持续写盘。
            let learned = &mut cfg.reporters[reporter_index]
                .komari
                .as_mut()
                .expect("protocol checked")
                .ext
                .learned_pings;
            let previous = learned[index].last_seen_at;
            let current = observed_at.max(previous);
            learned[index].last_seen_at = current;
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
        let learned = &mut next.reporters[reporter_index]
            .komari
            .as_mut()
            .expect("protocol checked")
            .ext
            .learned_pings;
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

        // 只有目标集合变化才通知,普通 touch 不重建 worker,也不重置 Reporter ticker。
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

    /// 远端配置只写入产生它的 Reporter;连接身份永不受上报端影响。
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
        let mut next = cfg.clone();
        let reporter = next
            .reporters
            .iter_mut()
            .find(|r| r.id == reporter_id)
            .ok_or_else(|| anyhow::anyhow!("Reporter 不存在: {reporter_id}"))?;
        match reporter.protocol() {
            Some(ReporterProtocol::Cf) => {
                apply_remote_cf(reporter.cf.as_mut().expect("protocol checked"), &remote)?;
            }
            Some(ReporterProtocol::Probe) => {
                apply_remote_probe(reporter.probe.as_mut().expect("protocol checked"), &remote);
            }
            Some(ReporterProtocol::Komari) => {
                bail!("Komari 协议不支持远端配置下发");
            }
            None => bail!("Reporter {reporter_id} 缺少协议段"),
        }
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
        tracing::info!(
            reporter_id,
            remote.config_version,
            "Reporter 远端配置已应用"
        );
        Ok(true)
    }
}

/// CF 远端配置落点:段形只能表达 collect_interval/connection_mode/
/// interface/ct/cu/cm/bd 等直属字段；落不下的字段整体拒绝,不静默丢弃。
fn apply_remote_cf(section: &mut crate::model::CfSection, remote: &RemoteConfig) -> Result<()> {
    if let Some(intervals) = remote.intervals {
        let entity = section.to_collect_config().intervals;
        if intervals.ping != entity.ping
            || intervals.slow != entity.slow
            || intervals.gpu != entity.gpu
            || intervals.ip != entity.ip
            || intervals.diskio != entity.diskio
        {
            bail!("cf 远端配置仅支持修改 collect_interval");
        }
        section.collect_interval = intervals.collect;
    }
    if let Some(value) = remote.cf_collect_interval {
        section.collect_interval = value;
    }
    if let Some(value) = remote.report_interval {
        section.interval = value;
    }
    if let Some(value) = remote.wss_report_interval {
        section.wss_report_interval = value;
    }
    if let Some(value) = remote.connection_mode {
        section.connection_mode = value;
    }
    if let Some(value) = remote.cf_ping_mode {
        section.ping_mode = value;
    }
    if let Some(value) = remote.reset_day {
        section.reset_day = value;
    }
    if let Some(value) = &remote.interfaces {
        section.interface = value.join(",");
    }
    if let Some(value) = &remote.disks {
        if !value.is_empty() {
            bail!("cf 段不支持 disks 选择");
        }
    }
    if let Some(pings) = &remote.pings {
        for ping in pings {
            if !matches!(ping.name.as_str(), "ct" | "cu" | "cm" | "bd") {
                bail!("cf 远端 Ping 仅支持 ct/cu/cm/bd,收到: {}", ping.name);
            }
        }
        let take = |name: &str| {
            pings
                .iter()
                .find(|ping| ping.name == name)
                .map(|ping| ping.target.clone())
        };
        section.ct = take("ct");
        section.cu = take("cu");
        section.cm = take("cm");
        section.bd = take("bd");
    }
    if let Some(value) = remote.report_gpu {
        if !value {
            bail!("cf 线固定启用 GPU,不能远端关闭");
        }
    }
    // CF wire 没有 errors/self 落点；两项固定为 false，远端推送直接忽略。
    section.ext.config_version = remote.config_version.clone();
    Ok(())
}

/// probe 远端配置落点:段即采集配置实体完整形态,逐字段整体替换。
fn apply_remote_probe(section: &mut crate::model::ProbeSection, remote: &RemoteConfig) {
    if let Some(value) = remote.intervals {
        section.intervals = value;
    }
    if let Some(value) = remote.report_interval {
        section.report_interval = value;
    }
    if let Some(value) = remote.reset_day {
        section.reset_day = value;
    }
    if let Some(value) = &remote.interfaces {
        section.interfaces = value.clone();
    }
    if let Some(value) = &remote.disks {
        section.disks = value.clone();
    }
    if let Some(value) = &remote.pings {
        section.pings = value.clone();
    }
    if let Some(value) = remote.report_gpu {
        section.report_gpu = value;
    }
    if let Some(value) = remote.report_errors {
        section.report_errors = value;
    }
    if let Some(value) = remote.report_self {
        section.report_self = value;
    }
    section.ext.config_version = remote.config_version.clone();
}

/// 远端配置整体校验:任何一项非法则整体拒绝
fn validate_remote(remote: &RemoteConfig) -> Result<()> {
    if let Some(intervals) = remote.intervals {
        intervals.validate().map_err(anyhow::Error::msg)?;
    }
    if remote.report_interval == Some(0) {
        bail!("远端 report_interval 必须 >= 1");
    }
    if remote
        .wss_report_interval
        .is_some_and(|value| !(1..=5).contains(&value))
    {
        bail!("远端 wss_report_interval 必须在 1-5 之间");
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

fn gcd(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left.max(1)
}

fn validate_update_proxy(raw: &str) -> Result<()> {
    let proxy = reqwest::Url::parse(raw).context("auto_update.proxys contains an invalid URL")?;
    if !matches!(proxy.scheme(), "http" | "https") || proxy.host_str().is_none() {
        bail!("auto_update.proxys entries must be absolute HTTP(S) URLs");
    }
    if !proxy.username().is_empty() || proxy.password().is_some() {
        bail!("auto_update.proxys entries must not contain credentials");
    }
    if proxy.query().is_some() || proxy.fragment().is_some() {
        bail!("auto_update.proxys entries must not contain query strings or fragments");
    }
    Ok(())
}

pub(crate) fn validate_update_repository(repository: &str) -> Result<()> {
    let Some((owner, name)) = repository.split_once('/') else {
        bail!("auto_update.repository must use owner/repo");
    };
    let valid_component = |value: &str| {
        !value.is_empty()
            && value != "."
            && value != ".."
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
    };
    if repository.trim() != repository
        || name.contains('/')
        || !valid_component(owner)
        || !valid_component(name)
    {
        bail!("auto_update.repository must use owner/repo with only A-Z, a-z, 0-9, _, . or -");
    }
    Ok(())
}

pub fn load(path: &Path) -> Result<LocalConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("读取配置失败: {}", path.display()))?;
    if is_legacy_schema(&raw)? {
        let (cfg, mut warnings, legacy_net_static_path) =
            crate::config_legacy::migrate_for_load(&raw)?;
        if let Some(source) = legacy_net_static_path {
            if let Some(warning) = migrate_legacy_net_static_file(&source, &cfg.net_static_path())?
            {
                warnings.push(warning);
            }
        }
        let backup_path = path.with_extension("toml.bak");
        persist_bytes(&backup_path, raw.as_bytes())
            .with_context(|| format!("备份旧配置失败: {}", backup_path.display()))?;
        persist(path, &cfg).context("迁移后的配置落盘失败")?;
        for warning in &warnings {
            tracing::warn!(%warning, "旧配置迁移警告");
        }
        tracing::info!(
            path = %path.display(),
            backup = %backup_path.display(),
            "旧版配置已迁移为 schema = 1"
        );
        return Ok(cfg);
    }
    parse_text(&raw)
}

fn migrate_legacy_net_static_file(source: &Path, target: &Path) -> Result<Option<String>> {
    if source == target || !source.exists() {
        return Ok(None);
    }
    if !source.is_file() {
        bail!("旧 net_static_path 不是文件: {}", source.display());
    }
    if target.exists() {
        let same_file = source
            .canonicalize()
            .ok()
            .zip(target.canonicalize().ok())
            .is_some_and(|(source, target)| source == target);
        if same_file {
            return Ok(None);
        }
        bail!(
            "旧流量账本 {} 与新账本 {} 同时存在，拒绝覆盖；请先合并或移走其中一个文件",
            source.display(),
            target.display()
        );
    }
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("创建新账本目录失败: {}", parent.display()))?;
    }
    std::fs::copy(source, target).with_context(|| {
        format!(
            "迁移旧流量账本失败: {} -> {}",
            source.display(),
            target.display()
        )
    })?;
    Ok(Some(format!(
        "旧流量账本已复制到 {}，原文件 {} 保留作为备份",
        target.display(),
        source.display()
    )))
}

/// 顶层缺少 schema 键即视为旧版(schema 0)配置。
fn is_legacy_schema(raw: &str) -> Result<bool> {
    let value: toml::Value = toml::from_str(raw).context("解析配置 TOML 失败")?;
    Ok(value.get("schema").is_none())
}

fn parse_text(raw: &str) -> Result<LocalConfig> {
    let cfg: LocalConfig = toml::from_str(raw).context("解析配置 TOML 失败")?;
    cfg.validate()?;
    Ok(cfg)
}

/// 同目录临时文件 + 原子替换。
///
/// SharedConfig 在其写锁内调用这里;唯一临时文件避免与编辑器或上次异常退出
/// 遗留的固定 `.tmp` 冲突。配置变更路径先同步文件、替换成功后才更新内存,
/// 从而保证磁盘始终是可恢复的事实源。
pub(crate) fn persist(path: &Path, cfg: &LocalConfig) -> Result<()> {
    let data = toml::to_string_pretty(cfg)?;
    persist_bytes(path, data.as_bytes())
}

/// 校验托盘编辑器中的原始 TOML,并在正式配置自打开编辑器后未变化时保存。
///
/// 编辑内容先通过与启动加载完全相同的解析和业务校验,再备份当前正式配置,
/// 最后执行同目录原子替换。任一步失败都不会以未校验内容覆盖正式配置。
#[cfg(any(windows, test))]
pub(crate) fn persist_edited_text(
    path: &Path,
    expected_original: &str,
    edited: &str,
) -> Result<PathBuf> {
    parse_text(edited).context("编辑后的配置未通过校验")?;

    let current = std::fs::read_to_string(path)
        .with_context(|| format!("重新读取正式配置失败: {}", path.display()))?;
    if current != expected_original {
        bail!("正式配置在编辑期间已被其他进程修改;请取消并重新打开编辑器");
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

    // 配置含 secret:替换前固定 0600,避免权限随旧文件或 umask 漂移。
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

    // rename 本身持久化到目录项后,掉电恢复不会回到旧文件名状态。
    #[cfg(unix)]
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{CfSection, KomariSection, ProbeSection};

    fn base_config() -> LocalConfig {
        LocalConfig {
            schema: CONFIG_SCHEMA,
            data_dir: "/tmp/probe-rs-test".into(),
            auto_update: AutoUpdateConfig::default(),
            reporters: vec![ReporterConfig {
                id: "primary".into(),
                cf: None,
                komari: None,
                probe: Some(ProbeSection {
                    server_id: "s1".into(),
                    secret: "sec".into(),
                    worker_url: "https://example.com/report".into(),
                    report_interval: 60,
                    reset_day: 1,
                    report_errors: true,
                    report_self: false,
                    interfaces: vec![],
                    disks: vec![],
                    report_gpu: false,
                    intervals: CollectionIntervals {
                        collect: 10,
                        ..Default::default()
                    },
                    pings: vec![PingTarget {
                        name: "ct".into(),
                        kind: PingKind::Tcp,
                        target: "example.com:80".into(),
                        interval: None,
                    }],
                    ext: Default::default(),
                }),
            }],
        }
    }

    fn komari_reporter(id: &str) -> ReporterConfig {
        ReporterConfig {
            id: id.into(),
            cf: None,
            komari: Some(KomariSection {
                endpoint: "https://komari.example.com".into(),
                token: "token".into(),
                interval: 1,
                month_rotate: 12,
                enable_gpu: true,
                include_nics: String::new(),
                include_mountpoints: String::new(),
                ext: Default::default(),
            }),
            probe: None,
        }
    }

    fn cf_reporter(id: &str) -> ReporterConfig {
        ReporterConfig {
            id: id.into(),
            cf: Some(CfSection {
                server_id: "cf-id".into(),
                secret: "cf-secret".into(),
                url: "https://worker.example/update".into(),
                connection_mode: CfConnectionMode::Auto,
                ping_mode: crate::model::CfPingMode::Tcp,
                interval: 60,
                collect_interval: 1,
                wss_report_interval: 2,
                reset_day: 1,
                interface: String::new(),
                ct: Some("gd-ct.example.com:80".into()),
                cu: None,
                cm: None,
                bd: None,
                ext: Default::default(),
            }),
            komari: None,
            probe: None,
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
            assert_eq!(PathBuf::from(default_data_dir()), base);
        }

        #[cfg(not(windows))]
        {
            assert_eq!(
                default_config_path(),
                PathBuf::from("/etc/probe-rs/config.toml")
            );
            assert_eq!(default_data_dir(), "/var/lib/probe-rs");
        }
    }

    #[test]
    fn net_static_lives_under_data_dir() {
        let cfg = base_config();
        assert_eq!(
            cfg.net_static_path(),
            PathBuf::from("/tmp/probe-rs-test/net_static.json")
        );
    }

    #[test]
    fn rejects_zero_intervals() {
        let mut cfg = base_config();
        cfg.reporters[0].probe.as_mut().unwrap().intervals.collect = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_reporter_without_or_with_multiple_protocol_sections() {
        let mut cfg = base_config();
        cfg.reporters[0].komari = Some(komari_reporter("k").komari.unwrap());
        assert!(cfg.validate().is_err());
        cfg.reporters[0].probe = None;
        cfg.reporters[0].komari = None;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_bad_cf_node_targets() {
        let mut reporter = cf_reporter("cf");
        reporter.cf.as_mut().unwrap().ct = Some("example.com/path".into());
        let mut cfg = base_config();
        cfg.reporters.push(reporter);
        assert!(cfg.validate().is_err());

        let mut reporter = cf_reporter("cf");
        reporter.cf.as_mut().unwrap().cu = Some(String::new());
        let mut cfg = base_config();
        cfg.reporters.push(reporter);
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
        incompatible.reporters[0].probe.as_mut().unwrap().worker_url =
            "https://other.example/report".into();
        incompatible.reporters[0]
            .probe
            .as_mut()
            .unwrap()
            .intervals
            .collect = 5;
        persist(&path, &incompatible).unwrap();

        let result = shared
            .update_local_from_disk(&path, |candidate| {
                candidate.reporters[0].probe.as_ref().unwrap().worker_url
                    == initial.reporters[0].probe.as_ref().unwrap().worker_url
            })
            .unwrap();
        assert_eq!(result, LocalReload::RestartRequired);
        assert_eq!(shared.get(), initial);
        assert!(!intervals_rx.has_changed().unwrap());
        assert!(!config_rx.has_changed().unwrap());

        let mut compatible = initial.clone();
        compatible.reporters[0]
            .probe
            .as_mut()
            .unwrap()
            .intervals
            .collect = 5;
        persist(&path, &compatible).unwrap();

        let result = shared
            .update_local_from_disk(&path, |candidate| {
                candidate.reporters[0].probe.as_ref().unwrap().worker_url
                    == initial.reporters[0].probe.as_ref().unwrap().worker_url
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
        replacement.reporters[0]
            .probe
            .as_mut()
            .unwrap()
            .report_interval = 7;
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
        assert_eq!(
            load(&path).unwrap().reporters[0]
                .probe
                .as_ref()
                .unwrap()
                .report_interval,
            7
        );
    }

    #[test]
    fn edited_config_does_not_overwrite_a_concurrent_change() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let original = toml::to_string_pretty(&base_config()).unwrap();
        std::fs::write(&path, &original).unwrap();

        let mut concurrent = base_config();
        concurrent.reporters[0]
            .probe
            .as_mut()
            .unwrap()
            .report_interval = 11;
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
            schema = 1
            data_dir = "/tmp/x"
            [[reporters]]
            id = "primary"
            [reporters.probe]
            server_id = "s1"
            secret = "sec"
            worker_url = "https://example.com/report"
            report_interval = 60
        "#;
        let cfg: LocalConfig = toml::from_str(text).unwrap();
        assert_eq!(cfg.auto_update, AutoUpdateConfig::default());
        cfg.validate().unwrap();
    }

    #[test]
    fn example_config_stays_valid() {
        let cfg: LocalConfig = toml::from_str(include_str!("../config.example.toml")).unwrap();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.reporter("primary").unwrap().connection_mode,
            Some(CfConnectionMode::Auto)
        );
    }

    #[test]
    fn cf_connection_mode_defaults_to_auto_when_omitted() {
        let text = r#"
schema = 1
data_dir = "/tmp/x"

[[reporters]]
id = "cf"

[reporters.cf]
server_id = "server"
secret = "secret"
url = "https://example.com/update"
"#;
        let cfg: LocalConfig = toml::from_str(text).unwrap();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.reporter("cf").unwrap().connection_mode,
            Some(CfConnectionMode::Auto)
        );
        let cf = cfg.reporters[0].cf.as_ref().unwrap();
        assert_eq!(cf.collect_interval, 10); // preserve the pre-existing omitted-field default
        assert_eq!(cf.wss_report_interval, 2);
        assert_eq!(cf.effective_collect_interval(), 10);
    }

    #[test]
    fn rejects_too_frequent_update_checks() {
        let mut cfg = base_config();
        cfg.auto_update.check_interval = MIN_UPDATE_CHECK_INTERVAL - 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn update_proxys_require_safe_absolute_http_urls() {
        let mut cfg = base_config();
        cfg.auto_update.proxys = vec!["https://proxy.example/prefix".into()];
        cfg.validate().unwrap();

        for invalid in [
            "proxy.example",
            "ftp://proxy.example",
            "https://user:pass@proxy.example",
            "https://proxy.example/?token=secret",
            "https://proxy.example/#fragment",
        ] {
            cfg.auto_update.proxys = vec![invalid.into()];
            assert!(cfg.validate().is_err(), "accepted invalid proxy: {invalid}");
        }
    }

    #[test]
    fn update_repository_accepts_only_github_owner_repo_slugs() {
        let mut cfg = base_config();
        cfg.auto_update.repository = Some("fork-owner/probe.rs".into());
        cfg.validate().unwrap();

        for invalid in [
            "probe-rs",
            "/probe-rs",
            "owner/",
            "owner/repo/extra",
            "https://github.com/owner/repo",
            "owner/repo?ref=main",
            " owner/repo",
        ] {
            cfg.auto_update.repository = Some(invalid.into());
            assert!(
                cfg.validate().is_err(),
                "accepted invalid repository: {invalid}"
            );
        }
    }

    #[test]
    fn rejects_unknown_fields_loudly() {
        let bad_toml = r#"
schema = 1
data_dir = "/tmp/x"

[[reporters]]
id = "primary"
protocol = "probe"

[reporters.probe]
server_id = "s1"
secret = "sec"
worker_url = "https://example.com/report"
report_interval = 60
"#;
        // reporter 级的 protocol 键已不存在,必须报错而不是静默忽略
        assert!(toml::from_str::<LocalConfig>(bad_toml).is_err());
    }

    #[test]
    fn toml_roundtrip_preserves_sections_and_pings() {
        let mut cfg = base_config();
        cfg.reporters.push(cf_reporter("cf-a"));
        cfg.reporters.push(komari_reporter("komari-a"));
        let text = toml::to_string_pretty(&cfg).unwrap();
        let back: LocalConfig = toml::from_str(&text).unwrap();
        assert_eq!(back, cfg);
        assert!(text.contains("[reporters.cf]"));
        assert!(text.contains("[reporters.komari]"));
        assert!(text.contains("[reporters.probe]"));
    }

    #[test]
    fn protocol_sections_convert_to_collect_entities_and_merge() {
        let mut cfg = base_config();
        // probe: collect=10, ping interval 默认 30,report_gpu=false
        cfg.reporters.push(ReporterConfig {
            id: "komari-a".into(),
            cf: None,
            komari: Some(KomariSection {
                endpoint: "https://panel.example".into(),
                token: "token".into(),
                interval: 6,
                month_rotate: 12,
                enable_gpu: true,
                include_nics: "Ethernet*".into(),
                include_mountpoints: "C:*".into(),
                ext: Default::default(),
            }),
            probe: None,
        });
        let mut cf_config = cf_reporter("cf-a");
        let cf_section = cf_config.cf.as_mut().unwrap();
        cf_section.collect_interval = 0;
        cf_section.wss_report_interval = 4;
        cfg.reporters.push(cf_config);

        cfg.validate().unwrap();

        let komari = cfg.reporter("komari-a").unwrap();
        assert_eq!(komari.protocol, ReporterProtocol::Komari);
        assert_eq!(komari.reset_day, 12);
        assert_eq!(komari.intervals.collect, 6);
        assert_eq!(komari.intervals.report, 6); // komari 按采集周期上报
        assert!(komari.report_gpu);
        assert!(komari.report_errors);
        assert!(!komari.report_self);
        assert_eq!(komari.interfaces, vec!["Ethernet*".to_string()]);
        assert_eq!(komari.disks, vec!["C:*".to_string()]);

        let cf = cfg.reporter("cf-a").unwrap();
        assert!(cf.report_gpu); // cf 固定启用 GPU
        assert!(!cf.report_errors); // CF wire 没有错误事件落点
        assert_eq!(cf.source_collect_interval, 0);
        assert_eq!(cf.intervals.collect, 4); // auto + collect=0 跟随 WSS 配置周期
        assert_eq!(cf.pings.len(), 1);
        assert_eq!(cf.pings[0].target.name, "ct");

        let merged = cfg.merged_collect_config();
        assert_eq!(merged.intervals.collect, 2); // gcd(10, 6, 4)
        assert!(merged.report_gpu); // OR
                                    // komari 指定了网卡,但 probe/cf 为空(= 全部)→ 全局为全部
        assert!(merged.interfaces.is_empty());
        assert!(merged.disks.is_empty());
        // probe 的 ct(example.com:80)与 cf 的 ct(gd-ct)不同目标,都保留
        assert_eq!(merged.pings.len(), 2);
        assert_eq!(cfg.effective_intervals().report, 1); // 内部占位,不是上报周期

        let mut http_cfg = cfg.clone();
        http_cfg.reporters[2].cf.as_mut().unwrap().connection_mode = CfConnectionMode::Http;
        assert_eq!(http_cfg.reporter("cf-a").unwrap().intervals.collect, 60);

        let receipt = cf.static_config(cfg.global_summary(), cfg.reporter_summaries());
        assert_eq!(receipt.reporters.len(), 3);
        let json = serde_json::to_string(&receipt).unwrap();
        for private in [
            "sec",
            "token",
            "cf-secret",
            "https://example.com/report",
            "https://panel.example",
            "https://worker.example/update",
            "cf-id",
        ] {
            assert!(!json.contains(private), "摘要泄露了私有字段: {private}");
        }
    }

    #[test]
    fn duplicate_ping_endpoints_merge_with_min_interval() {
        let mut cfg = base_config();
        cfg.reporters[0].probe.as_mut().unwrap().pings = vec![PingTarget {
            name: "a".into(),
            kind: PingKind::Tcp,
            target: "EXAMPLE.com:80".into(),
            interval: Some(10),
        }];
        let mut cf = cf_reporter("cf-a");
        cf.cf.as_mut().unwrap().ct = Some("example.com.:80".into());
        cfg.reporters.push(cf);
        cfg.validate().unwrap();

        let merged = cfg.merged_collect_config();
        assert_eq!(merged.pings.len(), 1); // type + 规范化 endpoint 去重
        assert_eq!(merged.pings[0].interval, Some(10)); // 各消费者取最小周期

        let global = cfg.global_summary();
        assert_eq!(global.pings.len(), 1);
        assert_eq!(global.pings[0].target, "tcp://example.com:80");
        assert_eq!(global.pings[0].interval, 10);
    }

    #[test]
    fn cf_url_nodes_keep_http_ping_type_when_building_specs() {
        let mut cfg = base_config();
        cfg.reporters.clear();
        let mut cf = cf_reporter("cf-url");
        cf.cf.as_mut().unwrap().ct = Some("https://example.com".into());
        cfg.reporters.push(cf);
        cfg.validate().unwrap();

        let spec = cfg.reporter("cf-url").unwrap();
        assert_eq!(spec.pings.len(), 1);
        assert_eq!(spec.pings[0].target.kind, PingKind::Http);
        assert_eq!(spec.pings[0].task_id, "http:https://example.com:443");
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
            key(PingKind::Icmp, "EXAMPLE.com:80"),
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
            global_ping_uri(&target(PingKind::Icmp, "EXAMPLE.com:80")).unwrap(),
            "icmp://example.com"
        );
        assert_eq!(
            global_ping_uri(&target(PingKind::Tcp, "[2001:DB8::1]:443")).unwrap(),
            "tcp://[2001:db8::1]:443"
        );
    }

    #[test]
    fn komari_learned_pings_are_persisted_and_lru_bounded() {
        let dir =
            std::env::temp_dir().join(format!("probe-rs-komari-ping-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        let mut cfg = base_config();
        cfg.reporters[0] = komari_reporter("primary");
        persist(&path, &cfg).unwrap();
        let (shared, _intervals_rx, config_rx) = SharedConfig::new(cfg, path.clone());

        for i in 1..=KOMARI_LEARNED_PING_LIMIT {
            shared
                .learn_komari_ping("primary", PingKind::Icmp, &format!("192.0.2.{i}"), i as i64)
                .unwrap();
        }
        assert!(config_rx.has_changed().unwrap());
        let learned_len = shared.get().reporters[0]
            .komari
            .as_ref()
            .unwrap()
            .ext
            .learned_pings
            .len();
        assert_eq!(learned_len, KOMARI_LEARNED_PING_LIMIT);
        assert_eq!(
            shared.get().effective_pings().len(),
            KOMARI_LEARNED_PING_LIMIT
        );

        // 最近再次出现的第一个目标必须保留;加入第六个时淘汰未使用最久的第二个。
        shared
            .learn_komari_ping("primary", PingKind::Icmp, "192.0.2.1", 100)
            .unwrap();
        shared
            .learn_komari_ping("primary", PingKind::Icmp, "192.0.2.6", 101)
            .unwrap();
        let current = shared.get();
        let learned = &current.reporters[0]
            .komari
            .as_ref()
            .unwrap()
            .ext
            .learned_pings;
        assert_eq!(learned.len(), KOMARI_LEARNED_PING_LIMIT);
        assert!(learned.iter().any(|ping| ping.target == "192.0.2.1"));
        assert!(!learned.iter().any(|ping| ping.target == "192.0.2.2"));
        assert!(learned.iter().any(|ping| ping.target == "192.0.2.6"));

        let on_disk = load(&path).unwrap();
        assert_eq!(
            on_disk.reporters[0]
                .komari
                .as_ref()
                .unwrap()
                .ext
                .learned_pings,
            current.reporters[0]
                .komari
                .as_ref()
                .unwrap()
                .ext
                .learned_pings
        );
        let toml = std::fs::read_to_string(&path).unwrap();
        assert!(toml.contains("[[reporters.komari.ext.learned_pings]]"));
        assert!(shared
            .learn_komari_ping("primary", PingKind::Http, "https://example.com/health", 102,)
            .is_err());
        assert_eq!(
            shared.get().reporters[0]
                .komari
                .as_ref()
                .unwrap()
                .ext
                .learned_pings
                .len(),
            KOMARI_LEARNED_PING_LIMIT
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    fn remote(config_version: &str) -> RemoteConfig {
        RemoteConfig {
            config_version: config_version.into(),
            intervals: None,
            cf_collect_interval: None,
            report_interval: None,
            wss_report_interval: None,
            connection_mode: None,
            cf_ping_mode: None,
            reset_day: None,
            interfaces: None,
            disks: None,
            pings: None,
            report_gpu: None,
            report_errors: None,
            report_self: None,
        }
    }

    #[test]
    fn remote_config_applied_atomically() {
        let dir = std::env::temp_dir().join(format!("probe-rs-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        let cfg = base_config();
        persist(&path, &cfg).unwrap();
        let (shared, rx, _config_rx) = SharedConfig::new(cfg, path.clone());

        // 版本相同或为空:忽略(!= 语义,空版本号视为无版本)
        let mut push = remote("");
        push.report_interval = Some(1);
        push.reset_day = Some(5);
        assert!(!shared.apply_remote_for("primary", push).unwrap());
        assert_eq!(shared.get().reporter("primary").unwrap().reset_day, 1);

        // 零值间隔:整体拒绝
        let mut push = remote("2026-08-06T15:00:00+08:00");
        push.report_interval = Some(0);
        assert!(shared.apply_remote(push).is_err());
        assert_eq!(shared.get().reporter("primary").unwrap().config_version, "");

        // 合法:应用并落盘
        let mut push = remote("2026-08-06T15:00:00+08:00");
        push.report_interval = Some(20);
        push.reset_day = Some(15);
        assert!(shared.apply_remote_for("primary", push).unwrap());
        let after = shared.get();
        let primary = after.reporter("primary").unwrap();
        assert_eq!(primary.config_version, "2026-08-06T15:00:00+08:00");
        assert_eq!(primary.reset_day, 15);
        assert_eq!(primary.intervals.report, 20);
        assert_eq!(after.effective_intervals().collect, 10);
        assert!(!rx.has_changed().unwrap());
        let on_disk = load(&path).unwrap();
        assert_eq!(
            on_disk.reporters[0]
                .probe
                .as_ref()
                .unwrap()
                .ext
                .config_version,
            "2026-08-06T15:00:00+08:00"
        );
        assert_eq!(
            on_disk.reporters[0].probe.as_ref().unwrap().report_interval,
            20
        );

        // 相同 config_version 幂等,不重复触发一次性动作。
        let mut push = remote("2026-08-06T15:00:00+08:00");
        push.report_interval = Some(30);
        assert!(!shared.apply_remote_for("primary", push).unwrap());
        assert_eq!(
            shared.get().reporter("primary").unwrap().intervals.report,
            20
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cf_remote_config_maps_into_section_slots() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let mut cfg = base_config();
        cfg.reporters[0] = cf_reporter("primary");
        persist(&path, &cfg).unwrap();
        let (shared, _rx, _config_rx) = SharedConfig::new(cfg, path.clone());

        let mut push = remote("v1");
        push.cf_collect_interval = Some(0);
        push.report_interval = Some(30);
        push.wss_report_interval = Some(4);
        push.connection_mode = Some(CfConnectionMode::Http);
        push.cf_ping_mode = Some(crate::model::CfPingMode::Icmp);
        push.interfaces = Some(vec!["eth0".into()]);
        push.pings = Some(vec![
            PingTarget {
                name: "ct".into(),
                kind: PingKind::Tcp,
                target: "new-ct.example.com:80".into(),
                interval: None,
            },
            PingTarget {
                name: "cu".into(),
                kind: PingKind::Tcp,
                target: "new-cu.example.com:80".into(),
                interval: None,
            },
        ]);
        assert!(shared.apply_remote_for("primary", push).unwrap());

        let cf = shared.get().reporters[0].cf.clone().unwrap();
        assert_eq!(cf.collect_interval, 0);
        assert_eq!(cf.interval, 30);
        assert_eq!(cf.wss_report_interval, 4);
        assert_eq!(cf.connection_mode, CfConnectionMode::Http);
        assert_eq!(cf.ping_mode, crate::model::CfPingMode::Icmp);
        assert_eq!(cf.effective_collect_interval(), 30);
        assert_eq!(cf.interface, "eth0");
        assert_eq!(cf.ct.as_deref(), Some("new-ct.example.com:80"));
        assert_eq!(cf.cu.as_deref(), Some("new-cu.example.com:80"));
        assert_eq!(cf.cm, None); // 缺席 = 清除
        assert_eq!(cf.ext.config_version, "v1");

        let on_disk = load(&path).unwrap();
        assert_eq!(
            on_disk.reporters[0].cf.as_ref().unwrap().connection_mode,
            CfConnectionMode::Http
        );
        assert_eq!(
            on_disk.reporters[0].cf.as_ref().unwrap().ping_mode,
            crate::model::CfPingMode::Icmp
        );

        // 非 collect 的 intervals 字段:整体拒绝
        let mut push = remote("v2");
        push.intervals = Some(CollectionIntervals {
            ping: 99,
            ..Default::default()
        });
        assert!(shared.apply_remote_for("primary", push).is_err());
        assert_eq!(
            shared.get().reporters[0]
                .cf
                .as_ref()
                .unwrap()
                .ext
                .config_version,
            "v1"
        );

        // 非四大线路的 Ping 名:整体拒绝
        let mut push = remote("v2");
        push.pings = Some(vec![PingTarget {
            name: "homepage".into(),
            kind: PingKind::Tcp,
            target: "example.com:80".into(),
            interval: None,
        }]);
        assert!(shared.apply_remote_for("primary", push).is_err());

        // cf 固定 GPU:远端关闭被拒绝
        let mut push = remote("v2");
        push.report_gpu = Some(false);
        assert!(shared.apply_remote_for("primary", push).is_err());

        // 非空 disks:整体拒绝
        let mut push = remote("v2");
        push.disks = Some(vec!["C:*".into()]);
        assert!(shared.apply_remote_for("primary", push).is_err());
    }

    #[test]
    fn komari_reporter_rejects_remote_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let mut cfg = base_config();
        cfg.reporters[0] = komari_reporter("primary");
        persist(&path, &cfg).unwrap();
        let (shared, _rx, _config_rx) = SharedConfig::new(cfg, path.clone());

        let mut push = remote("v1");
        push.report_interval = Some(5);
        assert!(shared.apply_remote_for("primary", push).is_err());
    }
}
