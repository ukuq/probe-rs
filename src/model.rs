use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// 上报报文，对应 REPORT.md §完整报文示例
#[derive(Debug, Serialize)]
pub struct Report {
    pub server_id: String,
    /// 人类可读的 UTC+8 时间戳版本（如 2026-08-06T15:30:45.123+08:00）
    pub config_version: String,
    /// 本机墙钟与原生服务端校准后的时间；首次校准前准确时间相关字段为 null。
    pub time: ReportTime,
    #[serde(rename = "static", skip_serializing_if = "Option::is_none")]
    pub static_info: Option<StaticInfo>,
    pub dynamic: Vec<DynamicRecord>,
    #[serde(rename = "async")]
    pub async_records: Vec<AsyncRecord>,
    /// 采集/上报错误事件（空数组 = 无错误）
    pub errors: Vec<ErrorRecord>,
}

/// 原生 probe 协议的时间状态。offset_ms = accurate_ts - local_ts：
/// 正数表示本机时间偏慢，负数表示本机时间偏快。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReportTime {
    pub local_ts: i64,
    pub accurate_ts: Option<i64>,
    pub offset_ms: Option<i64>,
    /// ntp:<host>；UDP NTP 不可用时回退为 server；首次校准前为 null。
    pub source: Option<String>,
    pub round_trip_ms: Option<u64>,
    pub sample_age_ms: Option<u64>,
}

/// 一条错误事件：来源 + 信息。同源同文去重（不重复刷屏）
#[derive(Debug, Clone, Serialize)]
pub struct ErrorRecord {
    /// 发生时刻，毫秒时间戳
    pub ts: i64,
    /// 线上协议来源串：gpu / ip / reporter / ping:<组名> ...
    /// （由 Reporter 出口从类型化 ErrorOrigin 生成，见 buffer.rs）
    pub source: String,
    pub msg: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StaticInfo {
    /// static 信息采集时刻，毫秒时间戳
    pub ts: i64,
    pub os: String,
    pub kernel: String,
    pub arch: String,
    pub cpu_name: String,
    pub cpu_cores: u32,
    pub cpu_physical_cores: Option<u32>,
    pub mem_total: u64,
    pub swap_total: u64,
    pub disk_total: u64,
    /// 当前 Reporter 选中的逐卷容量快照；disk_total 为这些卷的合计。
    pub disks: Vec<DiskVolume>,
    pub gpu_name: Option<String>,
    pub virtualization: Option<String>,
    /// 毫秒时间戳;采集失败为 null(绝不用"当前时刻"伪装成刚开机)
    pub boot_time: Option<i64>,
    pub ipv4: Option<String>,
    pub ipv6: Option<String>,
    pub agent_version: String,
    /// 当前生效配置（供服务端展示/核对）
    pub config: StaticConfig,
}

/// static 内嵌的当前生效配置回执（static.config.*）
/// 字段顺序与「配置样例」保持一致
#[derive(Debug, Clone, Serialize)]
pub struct StaticConfig {
    /// Agent 全局实际采集配置的脱敏摘要。
    pub global: GlobalConfigSummary,
    /// 本机全部 Reporter 的脱敏摘要；不包含 server_id、secret、worker_url、config_version。
    pub reporters: Vec<ReporterSummary>,
    pub reset_day: u8,
    pub intervals: Intervals,
    pub interfaces: Vec<String>,
    pub disks: Vec<String>,
    pub enable_gpu: bool,
    pub report_errors: bool,
    pub report_self: bool,
    pub pings: Vec<PingTarget>,
}

/// 一条 = 一次 collect tick，只含 fast 字段，ts 即 tick 测量时刻
#[derive(Debug, Clone, Default, Serialize)]
pub struct DynamicRecord {
    /// 采集时刻，毫秒时间戳
    pub ts: i64,
    /// 采集时由 Agent 级时钟校准得到的毫秒时间；首次校准前缺席。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accurate_ts: Option<i64>,
    pub cpu_usage: Option<f64>,
    pub mem_used: Option<u64>,
    pub swap_used: Option<u64>,
    /// [load1, load5, load15]
    pub load: Option<[f64; 3]>,
    pub net_rx: Option<u64>,
    pub net_tx: Option<u64>,
    pub net_rx_speed: Option<u64>,
    pub net_tx_speed: Option<u64>,
    pub net_rx_monthly: Option<u64>,
    pub net_tx_monthly: Option<u64>,
    /// 当前 Reporter 选中的逐网卡快照；兼容合计字段由这些网卡求和。
    pub net_interfaces: BTreeMap<String, NetInterfaceSample>,
}

impl DynamicRecord {
    /// 出站与账期归属使用采集时保存的校准时间，不使用发送时偏差重算。
    pub fn report_ts(&self) -> i64 {
        self.accurate_ts.unwrap_or(self.ts)
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct NetInterfaceSample {
    pub rx: u64,
    pub tx: u64,
    pub rx_speed: u64,
    pub tx_speed: u64,
    pub rx_monthly: Option<u64>,
    pub tx_monthly: Option<u64>,
}

/// 文件系统卷容量。Windows 通常按盘符，Linux 按去重后的块设备/挂载点。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DiskVolume {
    pub id: String,
    pub name: String,
    pub mount_point: String,
    pub file_system: String,
    pub total: u64,
    pub used: u64,
}

/// 慢变指标块：由 slow worker 测量，ts 为真实测量时刻
#[derive(Debug, Clone, Serialize)]
pub struct SlowBlock {
    pub ts: i64,
    pub disk_used: Option<u64>,
    /// 当前 Reporter 选中的逐卷容量；disk_used 为这些卷的合计。
    pub disks: Vec<DiskVolume>,
    pub tcp_conn: Option<u64>,
    pub udp_conn: Option<u64>,
    pub processes: Option<u64>,
}

/// 探针自身资源占用（report_self=true 时由 slow worker 同节奏产出）
#[derive(Debug, Clone, Serialize)]
pub struct SelfRecord {
    pub ts: i64,
    /// 自身 CPU 使用率（整机容量百分比，0-100；进程线程总量按核数归一）
    pub cpu_usage: Option<f64>,
    /// 自身常驻内存 RSS，字节
    pub mem_rss: Option<u64>,
}

/// 磁盘 IO：由 diskio worker 测量，ts 为真实测量时刻。
/// 速率/等待为相邻两次采样的差值比（首轮无前值为 null）；
/// macOS 无"io 进行中总时长"计数器，usage 恒 null（单项不可得置 null 语义）
#[derive(Debug, Clone, Serialize)]
pub struct DiskIoRecord {
    pub ts: i64,
    /// 读速率，字节/秒
    pub read_bps: Option<f64>,
    /// 写速率，字节/秒
    pub write_bps: Option<f64>,
    pub read_iops: Option<f64>,
    pub write_iops: Option<f64>,
    /// 平均等待，毫秒
    pub await_ms: Option<f64>,
    /// IO 利用率 %（0-100，各盘取最大）；macOS 为 null
    pub usage: Option<f64>,
    /// 当前 Reporter 选中的逐物理盘 IO；上方字段为这些盘的聚合值。
    pub disks: Vec<DiskIoDeviceRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiskIoDeviceRecord {
    pub name: String,
    pub read_bps: Option<f64>,
    pub write_bps: Option<f64>,
    pub read_iops: Option<f64>,
    pub write_iops: Option<f64>,
    pub await_ms: Option<f64>,
    pub usage: Option<f64>,
}

/// 异步记录：kind 区分来源，每条 ts 为各自真实测量时刻。
/// kind 按数据语义划分（DESIGN.md §2.3）：slow = 每台机器必有的系统慢指标；
/// gpu = 仅部分机器有的可选硬件指标；ping = 主动探测结果；self = 探针自身占用；
/// diskio = 磁盘 IO 速率（各平台节奏不同：Linux 便宜可高频，macOS 走子进程降频）。
/// （公网 IP 是身份信息，在 static 里，不在此列。）
/// 新增异步源只需加一个 kind，协议不变
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum AsyncRecord {
    Ping(PingRecord),
    Slow(SlowBlock),
    Gpu(GpuRecord),
    DiskIo(DiskIoRecord),
    #[serde(rename = "self")]
    Self_(SelfRecord),
}

/// 一条 = 一个目标的一轮探测
#[derive(Debug, Clone, Serialize)]
pub struct PingRecord {
    /// 测量时刻，毫秒时间戳
    pub ts: i64,
    pub name: String,
    /// 毫秒；-1 = 探测失败
    pub rtt: i64,
    /// 0-100
    pub loss: u32,
}

/// 一条 = 一轮 GPU 采集（多卡时每卡一条）
/// macOS 无独立显存且温度需 root：mem_*/temp 为 null（单项不可得置 null 语义）
#[derive(Debug, Clone, Serialize)]
pub struct GpuRecord {
    /// 测量时刻，毫秒时间戳
    pub ts: i64,
    /// 采集端稳定设备标识；NVIDIA 使用 nvidia-smi index，其他平台使用可复现后备标识。
    pub id: String,
    pub name: String,
    /// 利用率 0-100
    pub usage: Option<f64>,
    /// 显存总量，字节
    pub mem_total: Option<u64>,
    /// 显存已用，字节
    pub mem_used: Option<u64>,
    /// 温度 ℃
    pub temp: Option<f64>,
}

/// 探测目标：逻辑名称 + 显式类型 + url/host + 独立间隔（缺省跟随 intervals.ping）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PingTarget {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: PingKind,
    pub target: String,
    #[serde(default)]
    pub interval: Option<u64>,
}

/// Komari 面板下发后由 Agent 自动学习的 Ping 目标。
///
/// 它没有 Reporter 内的逻辑名称和独立周期：采集周期始终跟随该
/// Komari Reporter 的 `intervals.ping`，`last_seen_at` 仅用于 LRU 淘汰。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KomariLearnedPing {
    #[serde(rename = "type")]
    pub kind: PingKind,
    pub target: String,
    pub last_seen_at: i64,
}

/// 去重后的全局实际 Ping worker 配置；逻辑名称仅属于各 Reporter，不进入这里。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GlobalPingTarget {
    /// 带协议的规范化采集端点，如 tcp://host:80、https://host:443、icmp://host。
    pub target: String,
    pub interval: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PingKind {
    Http,
    Tcp,
    Icmp,
}

/// 全局实际采集/异步 worker 周期。上报周期不属于这里。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionIntervals {
    pub collect: u64,
    #[serde(default = "default_ping_interval")]
    pub ping: u64,
    #[serde(default = "default_slow_interval")]
    pub slow: u64,
    #[serde(default = "default_gpu_interval")]
    pub gpu: u64,
    #[serde(default = "default_ip_interval")]
    pub ip: u64,
    #[serde(default = "default_diskio_interval")]
    pub diskio: u64,
}

impl Default for CollectionIntervals {
    fn default() -> Self {
        Self {
            collect: 10,
            ping: default_ping_interval(),
            slow: default_slow_interval(),
            gpu: default_gpu_interval(),
            ip: default_ip_interval(),
            diskio: default_diskio_interval(),
        }
    }
}

impl CollectionIntervals {
    pub fn validate(&self) -> Result<(), String> {
        for (key, value) in [
            ("collect", self.collect),
            ("ping", self.ping),
            ("slow", self.slow),
            ("gpu", self.gpu),
            ("ip", self.ip),
            ("diskio", self.diskio),
        ] {
            if value == 0 {
                return Err(format!("intervals.{key} 必须 >= 1 秒"));
            }
        }
        Ok(())
    }

    pub fn with_report(self, report: u64) -> Intervals {
        Intervals {
            collect: self.collect,
            report,
            ping: self.ping,
            slow: self.slow,
            gpu: self.gpu,
            ip: self.ip,
            diskio: self.diskio,
        }
    }
}

/// Reporter 使用的上报协议。配置与线上回执仍序列化为小写字符串。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReporterProtocol {
    Probe,
    Cf,
    Komari,
}

impl ReporterProtocol {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Probe => "probe",
            Self::Cf => "cf",
            Self::Komari => "komari",
        }
    }
}

impl std::fmt::Display for ReporterProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 一条独立上报线路：协议由出现的协议段（cf / komari / probe）决定，
/// 校验时要求恰好一个段非 None。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReporterConfig {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cf: Option<CfSection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub komari: Option<KomariSection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe: Option<ProbeSection>,
}

impl ReporterConfig {
    /// 恰好一个协议段时返回协议；零个或多个返回 None（由校验报错）。
    pub fn protocol(&self) -> Option<ReporterProtocol> {
        match (
            self.cf.is_some(),
            self.komari.is_some(),
            self.probe.is_some(),
        ) {
            (true, false, false) => Some(ReporterProtocol::Cf),
            (false, true, false) => Some(ReporterProtocol::Komari),
            (false, false, true) => Some(ReporterProtocol::Probe),
            _ => None,
        }
    }
}

/// 采集配置实体：各协议段先转换为此形态，再按路合并出全局真实执行的
/// 采集计划。形态与 probe 段的采集字段一致。
#[derive(Debug, Clone, PartialEq)]
pub struct CollectConfig {
    pub intervals: CollectionIntervals,
    /// 网卡 glob；空 = 全部。
    pub interfaces: Vec<String>,
    /// 磁盘卷/物理盘 glob；空 = 全部。
    pub disks: Vec<String>,
    pub report_gpu: bool,
    pub pings: Vec<PingTarget>,
}

/// CF 协议段（命名对齐 cfsm-agent：server_id/secret/url/interval/
/// collect_interval/connection_mode/ping_mode/reset_day/interface/ct/cu/cm/bd）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CfSection {
    pub server_id: String,
    pub secret: String,
    pub url: String,
    /// auto = WSS 实时上报，连接不可用时按 interval 回退 POST；
    /// http = 仅使用 POST /update。
    #[serde(default)]
    pub connection_mode: CfConnectionMode,
    /// 四大线路的探测模式。HTTP(S) 扩展节点在 tcp 模式下仍保持 HTTP 探测。
    #[serde(default)]
    pub ping_mode: CfPingMode,
    /// 上报周期（原版 report_interval）。
    #[serde(default = "default_report_interval")]
    pub interval: u64,
    /// 原版 CF 采集周期；0 表示不额外高频采集。
    #[serde(default = "default_collect_interval")]
    pub collect_interval: u64,
    /// WSS 配置上报周期。collect_interval=0 且 connection_mode=auto 时，
    /// 用它映射为机器级实际采集周期。
    #[serde(default = "default_cf_wss_report_interval")]
    pub wss_report_interval: u64,
    #[serde(default = "default_reset_day")]
    pub reset_day: u8,
    /// 逗号分隔的 Reporter 网卡白名单；空 = 使用默认出口过滤。
    #[serde(default)]
    pub interface: String,
    /// 四大线路 Ping 节点；探测类型由 ping_mode 控制，缺席/空 = 不探测。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ct: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cu: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bd: Option<String>,
    #[serde(default, skip_serializing_if = "CfExt::is_empty")]
    pub ext: CfExt,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CfConnectionMode {
    #[default]
    Auto,
    Http,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CfPingMode {
    #[default]
    Tcp,
    Icmp,
}

impl std::fmt::Display for CfPingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp => f.write_str("tcp"),
            Self::Icmp => f.write_str("icmp"),
        }
    }
}

impl std::fmt::Display for CfConnectionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => f.write_str("auto"),
            Self::Http => f.write_str("http"),
        }
    }
}

impl CfSection {
    pub fn effective_collect_interval(&self) -> u64 {
        if self.collect_interval > 0 {
            return self.collect_interval;
        }
        match self.connection_mode {
            CfConnectionMode::Auto => self.wss_report_interval,
            CfConnectionMode::Http => self.interval,
        }
        .max(1)
    }

    pub fn to_collect_config(&self) -> CollectConfig {
        let pings = [
            ("ct", &self.ct),
            ("cu", &self.cu),
            ("cm", &self.cm),
            ("bd", &self.bd),
        ]
        .into_iter()
        .filter_map(|(name, target)| {
            target
                .as_deref()
                .and_then(|target| cf_node_ping(name, target, self.ping_mode))
        })
        .collect();
        CollectConfig {
            intervals: CollectionIntervals {
                collect: self.effective_collect_interval(),
                ..Default::default()
            },
            interfaces: split_list(&self.interface, ','),
            disks: Vec::new(),
            // CF 线固定启用 GPU（沿用旧版 protocol="cf" 的缺省行为）。
            report_gpu: true,
            pings,
        }
    }
}

/// CF 四大线路节点按全局 ping_mode 选择 ICMP 或 TCP；TCP 模式下的
/// HTTP(S) URL 仍推断为 HTTP Ping。空值由配置校验负责报错。
pub(crate) fn cf_node_ping(name: &str, target: &str, ping_mode: CfPingMode) -> Option<PingTarget> {
    if target.trim().is_empty() {
        return None;
    }
    let lowercase = target.to_ascii_lowercase();
    let kind = match ping_mode {
        CfPingMode::Icmp => PingKind::Icmp,
        CfPingMode::Tcp
            if lowercase.starts_with("http://") || lowercase.starts_with("https://") =>
        {
            PingKind::Http
        }
        CfPingMode::Tcp => PingKind::Tcp,
    };
    Some(PingTarget {
        name: name.to_string(),
        kind,
        target: target.to_string(),
        interval: None,
    })
}

/// CF 段的 Agent 托管状态；不出现在示例配置中。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CfExt {
    #[serde(
        default,
        deserialize_with = "de_config_version",
        skip_serializing_if = "String::is_empty"
    )]
    pub config_version: String,
}

impl CfExt {
    fn is_empty(&self) -> bool {
        self.config_version.is_empty()
    }
}

/// Komari 协议段（命名对齐 komari-agent：endpoint/token/interval/
/// month_rotate/enable_gpu/include_nics/include_mountpoints）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KomariSection {
    pub endpoint: String,
    pub token: String,
    /// 采集周期；komari 按采集周期上报。
    #[serde(default = "default_komari_interval")]
    pub interval: u64,
    /// 流量统计月份重置日（0 = 禁用）。
    #[serde(default)]
    pub month_rotate: u8,
    #[serde(default)]
    pub enable_gpu: bool,
    /// 逗号分隔的网卡通配符；空 = 全部。
    #[serde(default)]
    pub include_nics: String,
    /// 分号分隔的挂载点列表；空 = 全部。
    #[serde(default)]
    pub include_mountpoints: String,
    #[serde(default, skip_serializing_if = "KomariExt::is_empty")]
    pub ext: KomariExt,
}

impl KomariSection {
    pub fn to_collect_config(&self) -> CollectConfig {
        CollectConfig {
            intervals: CollectionIntervals {
                collect: self.interval,
                ..Default::default()
            },
            interfaces: split_list(&self.include_nics, ','),
            disks: split_list(&self.include_mountpoints, ';'),
            report_gpu: self.enable_gpu,
            pings: Vec::new(),
        }
    }
}

/// Komari 段的探针自生成状态。`learned_pings` 由 Agent 管理，不接受面板
/// 直接改写，也非 komari 面板配置。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KomariExt {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub learned_pings: Vec<KomariLearnedPing>,
}

impl KomariExt {
    fn is_empty(&self) -> bool {
        self.learned_pings.is_empty()
    }
}

/// probe 协议段（probe-rs 原生协议；采集配置实体的完整形态）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeSection {
    pub server_id: String,
    pub secret: String,
    pub worker_url: String,
    pub report_interval: u64,
    #[serde(default = "default_reset_day")]
    pub reset_day: u8,
    #[serde(default = "default_true")]
    pub report_errors: bool,
    #[serde(default)]
    pub report_self: bool,
    #[serde(default)]
    pub interfaces: Vec<String>,
    /// 磁盘卷/物理盘 glob；空 = 全部。
    #[serde(default)]
    pub disks: Vec<String>,
    #[serde(default)]
    pub report_gpu: bool,
    #[serde(default)]
    pub intervals: CollectionIntervals,
    /// 此 Reporter 声明的 Ping 任务；与其他线路按 type+规范化目标去重。
    #[serde(default)]
    pub pings: Vec<PingTarget>,
    #[serde(default, skip_serializing_if = "ProbeExt::is_empty")]
    pub ext: ProbeExt,
}

impl ProbeSection {
    pub fn to_collect_config(&self) -> CollectConfig {
        CollectConfig {
            intervals: self.intervals,
            interfaces: self.interfaces.clone(),
            disks: self.disks.clone(),
            report_gpu: self.report_gpu,
            pings: self.pings.clone(),
        }
    }
}

/// probe 段的 Agent 托管状态。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeExt {
    #[serde(
        default,
        deserialize_with = "de_config_version",
        skip_serializing_if = "String::is_empty"
    )]
    pub config_version: String,
}

impl ProbeExt {
    fn is_empty(&self) -> bool {
        self.config_version.is_empty()
    }
}

/// 分隔符列表解析：逐项 trim、丢弃空项。
fn split_list(raw: &str, delimiter: char) -> Vec<String> {
    raw.split(delimiter)
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

/// Agent 全局 collector/async worker 配置摘要。
#[derive(Debug, Clone, Default, Serialize)]
pub struct GlobalConfigSummary {
    pub intervals: CollectionIntervals,
    pub enable_gpu: bool,
    /// 所有 Reporter 网卡选择的并集；任一路为空时 all_interfaces=true。
    pub interfaces: Vec<String>,
    pub all_interfaces: bool,
    /// 所有 Reporter 磁盘选择的并集；任一路为空时 all_disks=true。
    pub disks: Vec<String>,
    pub all_disks: bool,
    /// 按 type + 规范化目标聚合后的实际 Ping worker 配置；无逻辑名称和独立 type，
    /// 类型编码进 target URI，周期取各路最小值。
    pub pings: Vec<GlobalPingTarget>,
}

/// 一条 Reporter 的脱敏输出策略摘要。
#[derive(Debug, Clone, Serialize)]
pub struct ReporterSummary {
    pub id: String,
    pub protocol: ReporterProtocol,
    /// 协议原始采集值；CF 可为 0。
    pub source_collect_interval: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_mode: Option<CfConnectionMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ping_mode: Option<CfPingMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wss_report_interval: Option<u64>,
    pub intervals: CollectionIntervals,
    pub report_interval: u64,
    pub reset_day: u8,
    pub interfaces: Vec<String>,
    pub disks: Vec<String>,
    pub report_gpu: bool,
    pub report_errors: bool,
    pub report_self: bool,
    /// 此 Reporter 的原始 Ping 配置，包含 type/name/target/interval。
    pub pings: Vec<PingTarget>,
}

/// 服务端通过上报响应下发的远端配置（config 一级内，config_version 必填，
/// 其余字段出现才应用）。🔒 连接身份永不下发。
#[derive(Debug, Clone, Deserialize)]
pub struct RemoteConfig {
    /// 版本字符串；不等才应用（>= 语义对人类可读时间戳不可靠）
    #[serde(deserialize_with = "de_config_version")]
    pub config_version: String,
    /// 修改当前 Reporter 的采集需求；全局 worker 会重新聚合。
    #[serde(default)]
    pub intervals: Option<CollectionIntervals>,
    /// CF 原生 collect_interval 可为 0；它不能复用要求 >=1 的通用
    /// CollectionIntervals，因此只在 CF 响应适配层内部传递。
    #[serde(skip)]
    pub cf_collect_interval: Option<u64>,
    #[serde(default)]
    pub report_interval: Option<u64>,
    /// CF Schema 5+ 的配置 WSS 周期。
    #[serde(default)]
    pub wss_report_interval: Option<u64>,
    /// CF Reporter 的连接模式；其他协议忽略。
    #[serde(default)]
    pub connection_mode: Option<CfConnectionMode>,
    /// CF Schema 6 的四线路 Ping 模式；仅 CF 响应适配层使用。
    #[serde(skip)]
    pub cf_ping_mode: Option<CfPingMode>,
    #[serde(default)]
    pub reset_day: Option<u8>,
    #[serde(default)]
    pub interfaces: Option<Vec<String>>,
    #[serde(default)]
    pub disks: Option<Vec<String>>,
    /// 修改当前 Reporter 的 Ping 任务。服务端应避免使用内网敏感目标。
    #[serde(default)]
    pub pings: Option<Vec<PingTarget>>,
    #[serde(default)]
    pub report_gpu: Option<bool>,
    /// 是否上报 errors 错误事件（默认 true；仅 probe 线可配）
    #[serde(default)]
    pub report_errors: Option<bool>,
    /// 是否上报探针自身资源占用 kind:"self"（默认 false；仅 probe 线可配）
    #[serde(default)]
    pub report_self: Option<bool>,
}

fn default_true() -> bool {
    true
}

fn default_reset_day() -> u8 {
    1
}

fn default_report_interval() -> u64 {
    60
}

fn default_cf_wss_report_interval() -> u64 {
    2
}

fn default_collect_interval() -> u64 {
    10
}

fn default_komari_interval() -> u64 {
    1
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intervals {
    pub collect: u64,
    pub report: u64,
    #[serde(default = "default_ping_interval")]
    pub ping: u64,
    #[serde(default = "default_slow_interval")]
    pub slow: u64,
    #[serde(default = "default_gpu_interval")]
    pub gpu: u64,
    #[serde(default = "default_ip_interval")]
    pub ip: u64,
    #[serde(default = "default_diskio_interval")]
    pub diskio: u64,
}

fn default_ping_interval() -> u64 {
    30
}
fn default_slow_interval() -> u64 {
    60
}
fn default_gpu_interval() -> u64 {
    60
}
fn default_ip_interval() -> u64 {
    600
}
fn default_diskio_interval() -> u64 {
    10
}

impl Default for Intervals {
    fn default() -> Self {
        Self {
            collect: 10,
            report: 60,
            ping: default_ping_interval(),
            slow: default_slow_interval(),
            gpu: default_gpu_interval(),
            ip: default_ip_interval(),
            diskio: default_diskio_interval(),
        }
    }
}

#[cfg(test)]
impl Intervals {
    /// 仅要求各项 >= 1 秒。各间隔之间没有任何关系约束：
    /// report 时把缓冲全部发出即可，异步源按自己的 ts 新鲜度去重
    pub fn validate(&self) -> Result<(), String> {
        for (k, v) in [
            ("collect", self.collect),
            ("report", self.report),
            ("ping", self.ping),
            ("slow", self.slow),
            ("gpu", self.gpu),
            ("ip", self.ip),
            ("diskio", self.diskio),
        ] {
            if v == 0 {
                return Err(format!("intervals.{k} 必须 >= 1 秒"));
            }
        }
        Ok(())
    }
}

pub fn now_millis() -> i64 {
    chrono::Local::now().timestamp_millis()
}

/// config_version 兼容反序列化：接受字符串或旧版整数（统一转字符串）
pub fn de_config_version<'de, D>(d: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct V;
    impl serde::de::Visitor<'_> for V {
        type Value = String;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("string or integer")
        }
        fn visit_str<E>(self, v: &str) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_string<E>(self, v: String) -> Result<String, E> {
            Ok(v)
        }
        fn visit_u64<E>(self, v: u64) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_i64<E>(self, v: i64) -> Result<String, E> {
            Ok(v.to_string())
        }
    }
    d.deserialize_any(V)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intervals_validation() {
        let ok = Intervals {
            collect: 10,
            report: 60,
            ping: 30,
            ..Default::default()
        };
        assert!(ok.validate().is_ok());

        // report < collect 也合法：多余的上报只是空数组心跳
        let heartbeat = Intervals {
            collect: 60,
            report: 10,
            ping: 30,
            ..Default::default()
        };
        assert!(heartbeat.validate().is_ok());

        let zero = Intervals {
            collect: 0,
            report: 60,
            ping: 30,
            ..Default::default()
        };
        assert!(zero.validate().is_err());
    }

    #[test]
    fn reporter_protocol_keeps_lowercase_wire_values() {
        for (protocol, value) in [
            (ReporterProtocol::Probe, "probe"),
            (ReporterProtocol::Cf, "cf"),
            (ReporterProtocol::Komari, "komari"),
        ] {
            assert_eq!(
                serde_json::to_string(&protocol).unwrap(),
                format!("\"{value}\"")
            );
            assert_eq!(
                serde_json::from_str::<ReporterProtocol>(&format!("\"{value}\"")).unwrap(),
                protocol
            );
            assert_eq!(protocol.as_str(), value);
            assert_eq!(protocol.to_string(), value);
        }
        assert!(serde_json::from_str::<ReporterProtocol>("\"unknown\"").is_err());
    }
}
