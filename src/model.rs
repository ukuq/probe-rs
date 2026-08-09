use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// 上报报文，对应 REPORT.md §完整报文示例
#[derive(Debug, Serialize)]
pub struct Report {
    pub server_id: String,
    /// 人类可读的 UTC+8 时间戳版本（如 2026-08-06T15:30:45.123+08:00）
    pub config_version: String,
    #[serde(rename = "static", skip_serializing_if = "Option::is_none")]
    pub static_info: Option<StaticInfo>,
    pub dynamic: Vec<DynamicRecord>,
    #[serde(rename = "async")]
    pub async_records: Vec<AsyncRecord>,
    /// 采集/上报错误事件（空数组 = 无错误）
    pub errors: Vec<ErrorRecord>,
}

/// 一条错误事件：来源 + 信息。同源同文去重（不重复刷屏）
#[derive(Debug, Clone, Serialize)]
pub struct ErrorRecord {
    /// 发生时刻，毫秒时间戳
    pub ts: i64,
    /// 来源：gpu / ip / reporter / ping:<组名> ...
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
    pub gpu_name: Option<String>,
    pub virtualization: Option<String>,
    /// 毫秒时间戳
    pub boot_time: i64,
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
    pub reset_day: u8,
    pub intervals: Intervals,
    pub interfaces: Vec<String>,
    pub enable_gpu: bool,
    pub report_errors: bool,
    pub report_self: bool,
    pub pings: Vec<PingTarget>,
    /// 协议扩展（ext.*）
    pub ext: ExtConfig,
}

/// 一条 = 一次 collect tick，只含 fast 字段，ts 即 tick 测量时刻
#[derive(Debug, Clone, Default, Serialize)]
pub struct DynamicRecord {
    /// 采集时刻，毫秒时间戳
    pub ts: i64,
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
    /// 内部逐网卡快照：多 Reporter 在出口按各自 interfaces 聚合，不进入任何协议报文。
    #[serde(skip)]
    pub net_interfaces: BTreeMap<String, NetInterfaceSample>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NetInterfaceSample {
    pub rx: u64,
    pub tx: u64,
    pub rx_speed: u64,
    pub tx_speed: u64,
}

/// 慢变指标块：由 slow worker 测量，ts 为真实测量时刻
#[derive(Debug, Clone, Serialize)]
pub struct SlowBlock {
    pub ts: i64,
    pub disk_used: Option<u64>,
    pub tcp_conn: Option<u64>,
    pub udp_conn: Option<u64>,
    pub processes: Option<u64>,
}

/// 探针自身资源占用（report_self=true 时由 slow worker 同节奏产出）
#[derive(Debug, Clone, Serialize)]
pub struct SelfRecord {
    pub ts: i64,
    /// 自身 CPU 使用率（单核百分比，0-100）
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

/// 探测目标：key（唯一键）+ url/host + 独立间隔（缺省跟随 intervals.ping）
/// target 以 http(s):// 开头 → HTTP 探测；否则 TCP（host[:port]，默认 80）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PingTarget {
    pub name: String,
    pub target: String,
    #[serde(default)]
    pub interval: Option<u64>,
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

/// 一条独立上报线路；所有连接信息和输出策略都只存在于 Reporter 内。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReporterConfig {
    pub id: String,
    pub protocol: String,
    pub server_id: String,
    pub secret: String,
    pub worker_url: String,
    #[serde(default, deserialize_with = "de_config_version")]
    pub config_version: String,
    pub report_interval: u64,
    #[serde(default = "default_reset_day")]
    pub reset_day: u8,
    #[serde(default)]
    pub interfaces: Vec<String>,
    #[serde(default)]
    pub report_gpu: Option<bool>,
    #[serde(default = "default_true")]
    pub report_errors: bool,
    #[serde(default)]
    pub report_self: bool,
    /// 缺席 = 上报全部全局 ping；空数组 = 不上报 ping。
    #[serde(default)]
    pub ping_names: Option<Vec<String>>,
    #[serde(default)]
    pub ext: ExtConfig,
}

/// 服务端通过上报响应下发的远端配置（config 一级内，config_version 必填，
/// 其余字段出现才应用）。🔒 全局采集字段与连接身份永不下发。
#[derive(Debug, Clone, Deserialize)]
pub struct RemoteConfig {
    /// 版本字符串；不等才应用（>= 语义对人类可读时间戳不可靠）
    #[serde(deserialize_with = "de_config_version")]
    pub config_version: String,
    #[serde(default)]
    pub report_interval: Option<u64>,
    #[serde(default)]
    pub reset_day: Option<u8>,
    #[serde(default)]
    pub interfaces: Option<Vec<String>>,
    #[serde(default)]
    pub report_gpu: Option<bool>,
    /// 是否上报 errors 错误事件（默认 true）
    #[serde(default)]
    pub report_errors: Option<bool>,
    /// 是否上报探针自身资源占用 kind:"self"（默认 false）
    #[serde(default)]
    pub report_self: Option<bool>,
    /// 协议扩展配置（ext.*；仅对应协议启用时生效）
    #[serde(default)]
    pub ext: Option<RemoteExt>,
}

/// 远端下发的协议扩展容器
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteExt {
    #[serde(default)]
    pub cf: Option<RemoteCfExt>,
}

/// 远端下发的 CF 扩展（全 Option，缺席保持现值）
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteCfExt {
    #[serde(default)]
    pub correction: Option<bool>,
    #[serde(default)]
    pub batch: Option<bool>,
}

/// 本地配置中的协议扩展容器（ext.*）
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtConfig {
    #[serde(default)]
    pub cf: CfExt,
}

/// CF 协议扩展（ext.cf.*）：仅 protocol="cf" 时生效
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CfExt {
    /// 是否执行流量校正回路（应用 + 回传确认），缺省 true
    #[serde(default = "default_cf_true")]
    pub correction: bool,
    /// 上报形状：true = samples[] 批量（带 ts）；false = 单条 metrics，缺省 true
    #[serde(default = "default_cf_true")]
    pub batch: bool,
}

impl Default for CfExt {
    fn default() -> Self {
        Self {
            correction: true,
            batch: true,
        }
    }
}

fn default_cf_true() -> bool {
    true
}

fn default_true() -> bool {
    true
}

fn default_reset_day() -> u8 {
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
}
