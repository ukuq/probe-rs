//! schema 0(无 schema 键)旧配置的一次性迁移。
//!
//! 旧格式把连接身份、上报策略、采集需求混在扁平的 [[reporters]] 里;
//! 新格式拆成协议段(cf / komari / probe),采集字段按协议段的原版命名
//! 重新归位。无法在目标段形中表达的旧字段尽力迁移并逐条产生警告。

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::config::{AutoUpdateConfig, LocalConfig, CONFIG_SCHEMA};
use crate::model::{
    CfConnectionMode, CfSection, CollectionIntervals, KomariSection, PingKind, PingTarget,
    ProbeSection, ReporterConfig, ReporterProtocol,
};

#[derive(Debug, Deserialize)]
struct LegacyLocalConfig {
    #[serde(default)]
    net_static_path: String,
    #[serde(default)]
    auto_update: AutoUpdateConfig,
    #[serde(default)]
    reporters: Vec<LegacyReporterConfig>,
}

#[derive(Debug, Deserialize)]
struct LegacyReporterConfig {
    id: String,
    protocol: ReporterProtocol,
    #[serde(default)]
    server_id: String,
    #[serde(default)]
    secret: String,
    #[serde(default)]
    worker_url: String,
    #[serde(default, deserialize_with = "crate::model::de_config_version")]
    config_version: String,
    #[serde(default)]
    intervals: CollectionIntervals,
    #[serde(default)]
    report_interval: u64,
    #[serde(default = "default_reset_day")]
    reset_day: u8,
    #[serde(default)]
    interfaces: Vec<String>,
    #[serde(default)]
    disks: Vec<String>,
    #[serde(default)]
    report_gpu: Option<bool>,
    #[serde(default = "default_true")]
    report_errors: bool,
    #[serde(default)]
    report_self: bool,
    #[serde(default)]
    pings: Vec<PingTarget>,
    #[serde(default)]
    ext: LegacyExtConfig,
}

fn default_reset_day() -> u8 {
    1
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Default, Deserialize)]
struct LegacyExtConfig {
    #[serde(default)]
    cf: LegacyCfExt,
    #[serde(default)]
    komari: crate::model::KomariExt,
}

#[derive(Debug, Deserialize)]
struct LegacyCfExt {
    #[serde(default = "default_true")]
    correction: bool,
    #[serde(default = "default_true")]
    batch: bool,
    #[serde(default)]
    connection_mode: CfConnectionMode,
}

impl Default for LegacyCfExt {
    fn default() -> Self {
        Self {
            correction: true,
            batch: true,
            connection_mode: CfConnectionMode::default(),
        }
    }
}

/// 把旧版配置文本转换为新结构;返回配置与逐条迁移警告。
pub fn migrate(raw: &str) -> Result<(LocalConfig, Vec<String>)> {
    let (config, warnings, _) = migrate_for_load(raw)?;
    Ok((config, warnings))
}

/// 配置加载路径额外返回旧账本的精确文件名，供调用方在写入 schema 1
/// 前迁移到新的固定 data_dir/net_static.json。
pub(crate) fn migrate_for_load(raw: &str) -> Result<(LocalConfig, Vec<String>, Option<PathBuf>)> {
    let legacy: LegacyLocalConfig = toml::from_str(raw).context("解析旧版配置失败")?;
    let mut warnings = Vec::new();
    let legacy_net_static_path = (!legacy.net_static_path.trim().is_empty())
        .then(|| PathBuf::from(legacy.net_static_path.trim()));

    let data_dir = {
        let path = Path::new(legacy.net_static_path.trim());
        match path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            Some(parent) => parent.to_string_lossy().into_owned(),
            None => {
                if legacy.net_static_path.trim().is_empty() {
                    crate::config::default_data_dir()
                } else {
                    warnings.push(format!(
                        "net_static_path={:?} 没有父目录,data_dir 回退为平台默认",
                        legacy.net_static_path
                    ));
                    crate::config::default_data_dir()
                }
            }
        }
    };

    let mut reporters = Vec::with_capacity(legacy.reporters.len());
    for reporter in legacy.reporters {
        reporters.push(convert_reporter(reporter, &mut warnings));
    }

    let cfg = LocalConfig {
        schema: CONFIG_SCHEMA,
        data_dir,
        auto_update: legacy.auto_update,
        reporters,
    };
    cfg.validate().context("旧配置迁移结果非法")?;
    Ok((cfg, warnings, legacy_net_static_path))
}

fn convert_reporter(legacy: LegacyReporterConfig, warnings: &mut Vec<String>) -> ReporterConfig {
    let id = legacy.id.clone();
    match legacy.protocol {
        ReporterProtocol::Probe => ReporterConfig {
            id: legacy.id,
            cf: None,
            komari: None,
            probe: Some(ProbeSection {
                server_id: legacy.server_id,
                secret: legacy.secret,
                worker_url: legacy.worker_url,
                report_interval: legacy.report_interval,
                reset_day: legacy.reset_day,
                report_errors: legacy.report_errors,
                report_self: legacy.report_self,
                interfaces: legacy.interfaces,
                disks: legacy.disks,
                report_gpu: legacy.report_gpu.unwrap_or(false),
                intervals: legacy.intervals,
                pings: legacy.pings,
                ext: crate::model::ProbeExt {
                    config_version: legacy.config_version,
                },
            }),
        },
        ReporterProtocol::Cf => {
            if legacy.report_gpu == Some(false) {
                warnings.push(format!(
                    "reporter {id}: report_gpu=false 无法表达,cf 线固定启用 GPU"
                ));
            }
            if !legacy.disks.is_empty() {
                warnings.push(format!(
                    "reporter {id}: disks 选择被丢弃,cf 段不支持磁盘过滤"
                ));
            }
            let defaults = CollectionIntervals::default();
            for (field, old, new) in [
                ("ping", legacy.intervals.ping, defaults.ping),
                ("slow", legacy.intervals.slow, defaults.slow),
                ("gpu", legacy.intervals.gpu, defaults.gpu),
                ("ip", legacy.intervals.ip, defaults.ip),
                ("diskio", legacy.intervals.diskio, defaults.diskio),
            ] {
                if old != new {
                    warnings.push(format!(
                        "reporter {id}: intervals.{field}={old} 被丢弃,cf 段只保留 collect_interval"
                    ));
                }
            }
            if !legacy.ext.cf.correction {
                warnings.push(format!(
                    "reporter {id}: ext.cf.correction=false 已失效,校正回路固定启用"
                ));
            }
            if !legacy.ext.cf.batch {
                warnings.push(format!(
                    "reporter {id}: ext.cf.batch=false 已失效,上报固定为 samples[] 批量"
                ));
            }
            let (ct, cu, cm, bd) = convert_cf_pings(&id, legacy.pings, warnings);
            ReporterConfig {
                id: legacy.id,
                cf: Some(CfSection {
                    server_id: legacy.server_id,
                    secret: legacy.secret,
                    url: legacy.worker_url,
                    connection_mode: legacy.ext.cf.connection_mode,
                    interval: legacy.report_interval.max(1),
                    collect_interval: legacy.intervals.collect.max(1),
                    wss_report_interval: 2,
                    reset_day: legacy.reset_day,
                    interface: legacy.interfaces.join(","),
                    ct,
                    cu,
                    cm,
                    bd,
                    ext: crate::model::CfExt {
                        config_version: legacy.config_version,
                    },
                }),
                komari: None,
                probe: None,
            }
        }
        ReporterProtocol::Komari => {
            if !legacy.pings.is_empty() {
                warnings.push(format!(
                    "reporter {id}: {} 个手工 Ping 被丢弃,komari 段只支持面板下发目标",
                    legacy.pings.len()
                ));
            }
            if legacy.report_interval != legacy.intervals.collect {
                warnings.push(format!(
                    "reporter {id}: report_interval={} 与采集周期不一致,komari 统一按 interval={} 上报",
                    legacy.report_interval, legacy.intervals.collect
                ));
            }
            if !legacy.report_errors {
                warnings.push(format!(
                    "reporter {id}: report_errors=false 已失效,komari 线固定把错误映射为 message"
                ));
            }
            if legacy.report_self {
                warnings.push(format!(
                    "reporter {id}: report_self=true 已失效,komari 线固定不上报 self"
                ));
            }
            ReporterConfig {
                id: legacy.id,
                cf: None,
                komari: Some(KomariSection {
                    endpoint: legacy.worker_url,
                    token: legacy.secret,
                    interval: legacy.intervals.collect.max(1),
                    month_rotate: legacy.reset_day,
                    enable_gpu: legacy.report_gpu.unwrap_or(false),
                    include_nics: legacy.interfaces.join(","),
                    include_mountpoints: legacy.disks.join(";"),
                    ext: legacy.ext.komari,
                }),
                probe: None,
            }
        }
    }
}

/// 旧 cf reporter 的 pings 归入 ct/cu/cm/bd 四个槽位;同名槽位取第一个,
/// 非四大线路的 Ping 丢弃并警告。
fn convert_cf_pings(
    id: &str,
    pings: Vec<PingTarget>,
    warnings: &mut Vec<String>,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let mut slots: [Option<String>; 4] = [None, None, None, None];
    for ping in pings {
        let index = match ping.name.as_str() {
            "ct" => 0,
            "cu" => 1,
            "cm" => 2,
            "bd" | "bgp" => 3,
            _ => {
                warnings.push(format!(
                    "reporter {id}: Ping {:?} 被丢弃,cf 段只有 ct/cu/cm/bd 四个槽位",
                    ping.name
                ));
                continue;
            }
        };
        if ping.kind != PingKind::Tcp
            && !ping.target.to_ascii_lowercase().starts_with("http://")
            && !ping.target.to_ascii_lowercase().starts_with("https://")
        {
            warnings.push(format!(
                "reporter {id}: Ping {:?} 不是 TCP/HTTP 目标,已丢弃",
                ping.name
            ));
            continue;
        }
        if ping.interval.is_some() {
            warnings.push(format!(
                "reporter {id}: Ping {:?} 的独立周期被丢弃,cf 槽位跟随全局 ping 周期",
                ping.name
            ));
        }
        if slots[index].is_none() {
            slots[index] = Some(ping.target);
        } else {
            warnings.push(format!(
                "reporter {id}: 重复的 Ping {:?} 被丢弃,槽位已占用",
                ping.name
            ));
        }
    }
    (
        slots[0].take(),
        slots[1].take(),
        slots[2].take(),
        slots[3].take(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEGACY_PROBE: &str = r#"
net_static_path = "/var/lib/probe-rs/net_static.json"

[auto_update]
enabled = true
channel = "prerelease"
check_interval = 21600

[[reporters]]
id = "local-demo"
protocol = "probe"
server_id = "my-host"
secret = "change-me"
worker_url = "http://127.0.0.1:8080/report"
config_version = "v9"
report_interval = 5
reset_day = 3
interfaces = ["eth0"]
disks = ["C:*"]
report_gpu = true
report_errors = true
report_self = true

[reporters.intervals]
collect = 2
ping = 15
slow = 60
gpu = 60
ip = 600
diskio = 10

[[reporters.pings]]
name = "homepage"
type = "http"
target = "https://example.com"
interval = 60
"#;

    #[test]
    fn probe_reporter_migrates_losslessly() {
        let (cfg, warnings) = migrate(LEGACY_PROBE).unwrap();
        assert!(warnings.is_empty());
        assert_eq!(cfg.schema, CONFIG_SCHEMA);
        assert_eq!(cfg.data_dir, "/var/lib/probe-rs");
        assert!(cfg.auto_update.enabled);
        let probe = cfg.reporters[0].probe.as_ref().unwrap();
        assert_eq!(probe.server_id, "my-host");
        assert_eq!(probe.report_interval, 5);
        assert_eq!(probe.reset_day, 3);
        assert_eq!(probe.interfaces, vec!["eth0".to_string()]);
        assert_eq!(probe.disks, vec!["C:*".to_string()]);
        assert!(probe.report_gpu);
        assert!(probe.report_self);
        assert_eq!(probe.intervals.collect, 2);
        assert_eq!(probe.intervals.ping, 15);
        assert_eq!(probe.pings.len(), 1);
        assert_eq!(probe.ext.config_version, "v9");
    }

    #[test]
    fn cf_reporter_migrates_into_section_with_warnings() {
        let raw = r#"
net_static_path = "/var/lib/probe-rs/net_static.json"

[[reporters]]
id = "primary"
protocol = "cf"
server_id = "cf-server-uuid"
secret = "cf-api-secret"
worker_url = "https://monitor.example.com/update"
config_version = "abc"
report_interval = 30
reset_day = 1
interfaces = ["eth0", "eth1"]
disks = ["sda"]
report_gpu = false
report_errors = true
report_self = false

[reporters.intervals]
collect = 1
ping = 45
slow = 60
gpu = 60
ip = 600
diskio = 10

[[reporters.pings]]
name = "ct"
type = "tcp"
target = "gd-ct.example.com:80"

[[reporters.pings]]
name = "homepage"
type = "http"
target = "https://example.com"

[reporters.ext.cf]
correction = false
batch = true
connection_mode = "http"
"#;
        let (cfg, warnings) = migrate(raw).unwrap();
        let cf = cfg.reporters[0].cf.as_ref().unwrap();
        assert_eq!(cf.server_id, "cf-server-uuid");
        assert_eq!(cf.url, "https://monitor.example.com/update");
        assert_eq!(cf.connection_mode, CfConnectionMode::Http);
        assert_eq!(cf.interval, 30);
        assert_eq!(cf.collect_interval, 1);
        assert_eq!(cf.interface, "eth0,eth1");
        assert_eq!(cf.ct.as_deref(), Some("gd-ct.example.com:80"));
        assert_eq!(cf.cu, None);
        assert_eq!(cf.ext.config_version, "abc");
        // disks、report_gpu=false、ping=45、homepage、correction=false
        assert_eq!(warnings.len(), 5, "warnings: {warnings:?}");
    }

    #[test]
    fn komari_reporter_migrates_with_learned_pings() {
        let raw = r#"
net_static_path = "/var/lib/probe-rs/net_static.json"

[[reporters]]
id = "komari"
protocol = "komari"
server_id = "node-a"
secret = "komari-token"
worker_url = "https://komari.example.com"
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

[[reporters.ext.komari.learned_pings]]
type = "icmp"
target = "1.1.1.1"
last_seen_at = 1786252800000
"#;
        let (cfg, warnings) = migrate(raw).unwrap();
        assert!(warnings.is_empty(), "warnings: {warnings:?}");
        let komari = cfg.reporters[0].komari.as_ref().unwrap();
        assert_eq!(komari.endpoint, "https://komari.example.com");
        assert_eq!(komari.token, "komari-token");
        assert_eq!(komari.interval, 1);
        assert_eq!(komari.month_rotate, 12);
        assert!(komari.enable_gpu);
        assert_eq!(komari.ext.learned_pings.len(), 1);
        assert_eq!(komari.ext.learned_pings[0].target, "1.1.1.1");
    }

    #[test]
    fn legacy_config_roundtrip_through_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, LEGACY_PROBE).unwrap();

        let cfg = crate::config::load(&path).unwrap();
        assert_eq!(cfg.schema, CONFIG_SCHEMA);
        // 迁移结果已回写,再次读取走的是新格式路径
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("schema = 1"));
        assert!(text.contains("[reporters.probe]"));
        assert!(path.with_extension("toml.bak").exists());
        assert_eq!(
            std::fs::read_to_string(path.with_extension("toml.bak")).unwrap(),
            LEGACY_PROBE
        );
        // 幂等:第二次加载不再触发迁移
        let again = crate::config::load(&path).unwrap();
        assert_eq!(again, cfg);
    }

    #[test]
    fn custom_legacy_ledger_filename_is_copied_to_the_canonical_path() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let legacy_ledger = dir.path().join("monthly.json");
        let legacy_toml_path = legacy_ledger.to_string_lossy().replace('\\', "/");
        let raw = LEGACY_PROBE.replace("/var/lib/probe-rs/net_static.json", &legacy_toml_path);
        std::fs::write(&config_path, raw).unwrap();
        std::fs::write(&legacy_ledger, b"legacy-ledger").unwrap();

        let cfg = crate::config::load(&config_path).unwrap();
        let canonical = cfg.net_static_path();
        assert_eq!(canonical, dir.path().join("net_static.json"));
        assert_eq!(std::fs::read(&canonical).unwrap(), b"legacy-ledger");
        assert_eq!(std::fs::read(&legacy_ledger).unwrap(), b"legacy-ledger");
    }

    #[test]
    fn invalid_legacy_config_fails_loudly() {
        assert!(migrate("not [valid").is_err());
        // 迁移结果非法(空 reporters)同样报错
        assert!(migrate("net_static_path = \"/tmp/x.json\"\n").is_err());
    }
}
