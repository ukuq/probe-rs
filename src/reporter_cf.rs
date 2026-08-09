//! CF-Server-Monitor 协议出口（POST /update）
//!
//! 映射规则（对应 API.md §1.1）：
//! - 顶层 {id, secret, metrics, samples[]}；id/secret 复用本地 server_id/secret
//! - ram/swap/disk 单位 MB（字节 ÷ 2^20）；load_avg 为空格分隔字符串
//! - GPU → gpu_info:[{id,name,info}]（只有占用率；显存/温度 CF 无落点，丢弃）
//! - ping 按组名落 ping_ct/cu/cm/bd + loss_*（bgp 视作 bd 别名）；未配置=false，失败=null
//! - errors / self / virtualization / cpu_physical_cores：CF 无落点，CF 模式下不产生
//! - 配置下发：响应头 X-Agent-Config-Md5 + URL-encoded body → 合成 RemoteConfig
//!   （config_version 直接取 MD5 头，缺失时取原始配置串，== 幂等语义不变）
//! - 流量校正：body 尾部 rx_correction/tx_correction（GB，覆盖当月），不参与 MD5；
//!   确认必须用**独立请求**回传（CfConfirm）——CF 服务端见到 correction 字段会把
//!   整个请求当确认处理并丢弃其中的 metrics（handlers/update.js）

use std::collections::HashMap;

use serde::Serialize;

use crate::model::{
    DiskIoRecord, DynamicRecord, GpuRecord, PingRecord, PingTarget, SlowBlock, StaticInfo,
};

/// 一次 /update 请求体
#[derive(Debug, Serialize)]
pub struct CfUpdate {
    pub id: String,
    pub secret: String,
    pub metrics: CfMetrics,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub samples: Vec<CfSample>,
}

/// 校正确认请求（必须独立发送：CF 服务端见到 rx/tx_correction 字段
/// 会把整个请求当确认处理、丢弃其中的 metrics）
#[derive(Debug, Serialize)]
pub struct CfConfirm {
    pub id: String,
    pub secret: String,
    pub rx_correction: f64,
    pub tx_correction: f64,
}

#[derive(Debug, Default, Serialize)]
pub struct CfMetrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<f64>,
    pub ram_total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ram_used: Option<u64>,
    pub swap_total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap_used: Option<u64>,
    pub disk_total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_used: Option<u64>,
    /// 磁盘 IO（diskio 快照；首轮/不支持平台整个字段缺席）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk: Option<CfDisk>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_avg: Option<String>,
    pub boot_time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_rx: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_tx: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_rx_monthly: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_tx_monthly: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_in_speed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_out_speed: Option<u64>,
    pub os: String,
    pub arch: String,
    pub kernel_version: String,
    pub cpu_info: String,
    pub cpu_cores: u32,
    /// 探针标识（CF 存库展示；形如 0.0.0_probe-rs_0.1.0，可与官方探针区分）
    pub agent_version: String,
    /// 上报时刻（毫秒），CF 映射为 last_updated
    pub timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu_info: Option<Vec<CfGpu>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_conn: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub udp_conn: Option<u64>,
    /// 公网 IPv4；数值 0 = 不可达（字符串 "0" 会被 CF 当成 truthy 存成 1，必须用数值）
    pub ip_v4: serde_json::Value,
    /// 公网 IPv6；数值 0 = 不可达
    pub ip_v6: serde_json::Value,
    /// CF 用 false 表示该线路未配置、null 表示已配置但尚无成功测量。
    pub ping_ct: serde_json::Value,
    pub ping_cu: serde_json::Value,
    pub ping_cm: serde_json::Value,
    pub ping_bd: serde_json::Value,
    pub loss_ct: serde_json::Value,
    pub loss_cu: serde_json::Value,
    pub loss_cm: serde_json::Value,
    pub loss_bd: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct CfGpu {
    pub id: String,
    pub name: String,
    pub info: f64,
}

/// CF 磁盘 IO 对象（CF 侧全 0/缺失时不展示，故全 None 则不产出该字段）
#[derive(Debug, Serialize)]
pub struct CfDisk {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_bps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_bps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_iops: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_iops: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub await_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub util: Option<f64>,
}

/// samples[] 元素：{ts, metrics:{动态字段}}
#[derive(Debug, Serialize)]
pub struct CfSample {
    pub ts: i64,
    pub metrics: CfDynMetrics,
}

#[derive(Debug, Default, Serialize)]
pub struct CfDynMetrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ram_used: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap_used: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_avg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_rx: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_tx: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_in_speed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_out_speed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_rx_monthly: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_tx_monthly: Option<u64>,
}

const MIB: u64 = 1024 * 1024;

/// CF 服务端展示使用的探针版本前缀。
/// 上报标识 = "{CF_COMPAT_VERSION}_probe-rs_{本版本}"（CF 允许的字符集：0-9A-Za-z.+_-）
pub const CF_COMPAT_VERSION: &str = "0.0.0";

/// CF 探针标识：头部与 body 的 agent_version 统一用它
pub fn cf_agent_version(our_version: &str) -> String {
    format!("{CF_COMPAT_VERSION}_probe-rs_{our_version}")
}

fn mb(bytes: u64) -> u64 {
    bytes / MIB
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn load_str(load: [f64; 3]) -> String {
    format!("{:.2} {:.2} {:.2}", load[0], load[1], load[2])
}

/// 组装顶层 metrics：static（缓存复用）+ 最新 dynamic + 各异步快照
pub fn build_metrics(
    st: &StaticInfo,
    dyn_latest: Option<&DynamicRecord>,
    slow: Option<&SlowBlock>,
    gpus: &[GpuRecord],
    pings: &HashMap<String, PingRecord>,
    diskio: Option<&DiskIoRecord>,
    ping_targets: &[PingTarget],
) -> CfMetrics {
    let disk = diskio.and_then(|d| {
        let any = d.read_bps.is_some() || d.write_bps.is_some() || d.read_iops.is_some();
        any.then(|| CfDisk {
            read_bps: d.read_bps.map(round2),
            write_bps: d.write_bps.map(round2),
            read_iops: d.read_iops.map(round2),
            write_iops: d.write_iops.map(round2),
            await_ms: d.await_ms.map(round2),
            util: d.usage.map(round2),
        })
    });
    let enabled = |names: &[&str]| -> bool {
        ping_targets
            .iter()
            .any(|target| names.contains(&target.name.as_str()))
    };
    let ping = |names: &[&str]| -> serde_json::Value {
        if !enabled(names) {
            return false.into();
        }
        names
            .iter()
            .find_map(|n| pings.get(*n))
            .filter(|r| r.rtt >= 0)
            .map_or(serde_json::Value::Null, |r| r.rtt.into())
    };
    let ping_loss = |names: &[&str]| -> serde_json::Value {
        if !enabled(names) {
            return false.into();
        }
        names
            .iter()
            .find_map(|n| pings.get(*n))
            .map_or(serde_json::Value::Null, |r| r.loss.into())
    };
    let gpu_info = if gpus.is_empty() {
        None
    } else {
        Some(
            gpus.iter()
                .enumerate()
                .map(|(i, g)| CfGpu {
                    id: i.to_string(),
                    name: g.name.clone(),
                    info: g.usage.map(round2).unwrap_or(0.0),
                })
                .collect(),
        )
    };
    CfMetrics {
        cpu: dyn_latest.and_then(|d| d.cpu_usage).map(round2),
        ram_total: mb(st.mem_total),
        ram_used: dyn_latest.and_then(|d| d.mem_used).map(mb),
        swap_total: mb(st.swap_total),
        swap_used: dyn_latest.and_then(|d| d.swap_used).map(mb),
        disk_total: mb(st.disk_total),
        disk_used: slow.and_then(|s| s.disk_used).map(mb),
        disk,
        load_avg: dyn_latest.and_then(|d| d.load).map(load_str),
        boot_time: st.boot_time,
        net_rx: dyn_latest.and_then(|d| d.net_rx),
        net_tx: dyn_latest.and_then(|d| d.net_tx),
        net_rx_monthly: dyn_latest.and_then(|d| d.net_rx_monthly),
        net_tx_monthly: dyn_latest.and_then(|d| d.net_tx_monthly),
        net_in_speed: dyn_latest.and_then(|d| d.net_rx_speed),
        net_out_speed: dyn_latest.and_then(|d| d.net_tx_speed),
        os: st.os.clone(),
        arch: st.arch.clone(),
        kernel_version: st.kernel.clone(),
        cpu_info: st.cpu_name.clone(),
        cpu_cores: st.cpu_cores,
        agent_version: cf_agent_version(&st.agent_version),
        timestamp: crate::model::now_millis(),
        gpu_info,
        processes: slow.and_then(|s| s.processes),
        tcp_conn: slow.and_then(|s| s.tcp_conn),
        udp_conn: slow.and_then(|s| s.udp_conn),
        ip_v4: st
            .ipv4
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or_else(|| 0.into()),
        ip_v6: st
            .ipv6
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or_else(|| 0.into()),
        ping_ct: ping(&["ct"]),
        ping_cu: ping(&["cu"]),
        ping_cm: ping(&["cm"]),
        ping_bd: ping(&["bd", "bgp"]),
        loss_ct: ping_loss(&["ct"]),
        loss_cu: ping_loss(&["cu"]),
        loss_cm: ping_loss(&["cm"]),
        loss_bd: ping_loss(&["bd", "bgp"]),
    }
}

/// dynamic 记录 → samples[] 元素
pub fn build_sample(d: &DynamicRecord) -> CfSample {
    CfSample {
        ts: d.ts,
        metrics: CfDynMetrics {
            cpu: d.cpu_usage.map(round2),
            ram_used: d.mem_used.map(mb),
            swap_used: d.swap_used.map(mb),
            load_avg: d.load.map(load_str),
            net_rx: d.net_rx,
            net_tx: d.net_tx,
            net_in_speed: d.net_rx_speed,
            net_out_speed: d.net_tx_speed,
            net_rx_monthly: d.net_rx_monthly,
            net_tx_monthly: d.net_tx_monthly,
        },
    }
}

/// 本地 batch 开关可以显式关闭批量回放。
pub fn build_samples(dynamic: &[DynamicRecord], batch: bool) -> Vec<CfSample> {
    if !batch {
        return Vec::new();
    }
    dynamic.iter().map(build_sample).collect()
}

/// CF 配置推送（解析自 URL-encoded body）
#[derive(Debug, Clone, PartialEq)]
pub struct CfPush {
    /// 配置版本（= 响应头 MD5；缺头时用原始 body 串，!= 幂等语义不变）
    pub version: String,
    pub collect: Option<u64>,
    pub report: Option<u64>,
    pub reset_day: Option<u8>,
    /// custom_ct/cu/cm/bd 目标（host[:port] 或 URL），缺席的组不替换
    pub custom: [Option<String>; 4],
    pub interface: Option<String>,
}

/// 把 CF 推送合成为通用 RemoteConfig（走 apply_remote 同一条热应用管线）。
/// cur = 当前生效 intervals（CF 只下发 collect/report，其余四项保持现值）
pub fn synthesize_remote(
    push: &CfPush,
    cur: &crate::model::Intervals,
) -> crate::model::RemoteConfig {
    let intervals =
        (push.collect.is_some() || push.report.is_some()).then(|| crate::model::Intervals {
            // 项目内部采集/上报严格分离，采集间隔至少 1 秒；兼容 CF 的 0 输入。
            collect: push.collect.unwrap_or(cur.collect).max(1),
            report: push.report.unwrap_or(cur.report),
            ping: cur.ping,
            slow: cur.slow,
            gpu: cur.gpu,
            ip: cur.ip,
            diskio: cur.diskio,
        });
    let pings = push.custom.iter().any(Option::is_some).then(|| {
        ["ct", "cu", "cm", "bd"]
            .iter()
            .zip(push.custom.iter())
            .filter_map(|(name, target)| {
                let target = target.as_ref()?;
                // 空值 = 该组清空（不下发该组）
                if target.is_empty() {
                    return None;
                }
                Some(crate::model::PingTarget {
                    name: (*name).to_string(),
                    target: target.clone(),
                    interval: None,
                })
            })
            .collect()
    });
    crate::model::RemoteConfig {
        config_version: push.version.clone(),
        reset_day: push.reset_day,
        intervals,
        interfaces: push.interface.as_ref().map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .collect()
        }),
        enable_gpu: None,
        report_errors: None,
        report_self: None,
        pings,
        ext: None,
    }
}

/// /update 响应解析结果
#[derive(Debug, Default)]
pub struct CfResponse {
    /// 配置推送（body 含配置字段时）
    pub push: Option<CfPush>,
    /// 流量校正（GB，覆盖当月；不参与配置 MD5）
    pub correction: Option<(f64, f64)>,
}

/// 校正 GB 值合法性（对齐官方 MAX_TRAFFIC_CORRECTION_GB 思路）
fn valid_gb(v: f64) -> bool {
    v.is_finite() && (0.0..=1_000_000.0).contains(&v)
}

/// 解析 200 响应体：纯文本 "OK" → 空；否则按 URL-encoded 解析。
/// md5_header = 响应头 X-Agent-Config-Md5（可能为空）
pub fn parse_response_body(body: &str, md5_header: Option<&str>) -> CfResponse {
    let body = body.trim();
    if body.is_empty() || body == "OK" {
        return CfResponse::default();
    }
    let mut push = CfPush {
        version: String::new(), // 解析完后统一计算
        collect: None,
        report: None,
        reset_day: None,
        custom: [None, None, None, None],
        interface: None,
    };
    let mut has_config = false;
    let mut correction: Option<(f64, f64)> = None;
    let mut rx_gb: Option<f64> = None;
    let mut tx_gb: Option<f64> = None;
    for (k, v) in url::form_urlencoded::parse(body.as_bytes()) {
        match k.as_ref() {
            "collect_interval" => {
                push.collect = v.parse::<u64>().ok().map(|n| n.max(1));
                has_config = true;
            }
            "report_interval" => {
                push.report = v.parse::<u64>().ok().map(|n| n.max(1));
                has_config = true;
            }
            "reset_day" => {
                push.reset_day = v.parse::<u8>().ok().filter(|d| *d <= 31);
                has_config = true;
            }
            "custom_ct" => {
                push.custom[0] = Some(v.into_owned());
                has_config = true;
            }
            "custom_cu" => {
                push.custom[1] = Some(v.into_owned());
                has_config = true;
            }
            "custom_cm" => {
                push.custom[2] = Some(v.into_owned());
                has_config = true;
            }
            "custom_bd" => {
                push.custom[3] = Some(v.into_owned());
                has_config = true;
            }
            "interface" => {
                push.interface = Some(v.into_owned());
                has_config = true;
            }
            "rx_correction" => rx_gb = v.parse::<f64>().ok().filter(|v| valid_gb(*v)),
            "tx_correction" => tx_gb = v.parse::<f64>().ok().filter(|v| valid_gb(*v)),
            // schema_version / update=1：忽略（自升级不做）
            _ => {}
        }
    }
    if let (Some(rx), Some(tx)) = (rx_gb, tx_gb) {
        correction = Some((rx, tx));
    }
    if has_config {
        push.version = match md5_header.filter(|s| !s.is_empty()) {
            Some(h) => h.to_string(),
            // 缺 MD5 头（非官方服务端）：从配置字段重建版本串。
            // 不能用原始 body——校正/update 字段的出现或消失会造成版本空转
            None => format!(
                "ci={:?}&ri={:?}&rd={:?}&ct={:?}&cu={:?}&cm={:?}&bd={:?}&if={:?}",
                push.collect,
                push.report,
                push.reset_day,
                push.custom[0],
                push.custom[1],
                push.custom[2],
                push.custom[3],
                push.interface
            ),
        };
    }
    CfResponse {
        push: has_config.then_some(push),
        correction,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::StaticInfo;

    fn static_stub() -> StaticInfo {
        StaticInfo {
            ts: 1,
            os: "Debian 13".into(),
            kernel: "7.0.11".into(),
            arch: "aarch64".into(),
            cpu_name: "aarch64".into(),
            cpu_cores: 14,
            cpu_physical_cores: Some(14),
            mem_total: 13510758768,
            swap_total: 14682169548,
            disk_total: 80412332032,
            gpu_name: None,
            virtualization: None,
            boot_time: 1786000000000,
            ipv4: Some("38.147.161.207".into()),
            ipv6: None,
            agent_version: "0.1.0".into(),
            config: crate::model::StaticConfig {
                reset_day: 1,
                intervals: crate::model::Intervals::default(),
                interfaces: vec![],
                enable_gpu: false,
                report_errors: true,
                report_self: true,
                pings: vec![],
                ext: Default::default(),
            },
        }
    }

    #[test]
    fn metrics_mapping() {
        let st = static_stub();
        let d = DynamicRecord {
            ts: 1000,
            cpu_usage: Some(12.345),
            mem_used: Some(970_000_000),
            swap_used: Some(0),
            load: Some([0.1, 0.2, 0.3]),
            net_rx: Some(419_900_000),
            net_tx: Some(9_400_000),
            net_rx_speed: Some(11_200),
            net_tx_speed: Some(7_600),
            net_rx_monthly: Some(594_000),
            net_tx_monthly: Some(467_000),
        };
        let slow = SlowBlock {
            ts: 999,
            disk_used: Some(5_000_000_000),
            tcp_conn: Some(1),
            udp_conn: Some(6),
            processes: Some(14),
        };
        let mut pings = HashMap::new();
        pings.insert(
            "ct".to_string(),
            PingRecord {
                ts: 999,
                name: "ct".into(),
                rtt: 42,
                loss: 0,
            },
        );
        pings.insert(
            "bgp".to_string(),
            PingRecord {
                ts: 999,
                name: "bgp".into(),
                rtt: 6,
                loss: 25,
            },
        );
        pings.insert(
            "cu".to_string(),
            PingRecord {
                ts: 999,
                name: "cu".into(),
                rtt: -1,
                loss: 100,
            },
        );
        let ping_targets = ["ct", "cu", "bgp"]
            .into_iter()
            .map(|name| PingTarget {
                name: name.into(),
                target: "example.com".into(),
                interval: None,
            })
            .collect::<Vec<_>>();
        let m = build_metrics(&st, Some(&d), Some(&slow), &[], &pings, None, &ping_targets);
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["cpu"], 12.35);
        assert_eq!(v["agent_version"], "0.0.0_probe-rs_0.1.0");
        assert!(v["timestamp"].as_i64().unwrap() > 0);
        assert_eq!(v["ram_total"], 12884); // 13510758768 / 2^20（向下取整）
        assert_eq!(v["ram_used"], 925);
        assert_eq!(v["load_avg"], "0.10 0.20 0.30");
        assert_eq!(v["ip_v4"], "38.147.161.207");
        assert_eq!(v["ip_v6"], 0); // None → 数值 0（字符串 "0" 会被 CF 存成 1）
        assert_eq!(v["ping_ct"], 42);
        assert_eq!(v["ping_bd"], 6); // bgp → bd
        assert_eq!(v["loss_bd"], 25);
        assert!(v["ping_cu"].is_null()); // 已配置但失败 → null
        assert_eq!(v["loss_cu"], 100); // loss 仍上报
        assert_eq!(v["ping_cm"], false); // 未配置 → false（CF 的禁用语义）
        assert_eq!(v["loss_cm"], false);
        assert_eq!(v["tcp_conn"], 1);
        assert!(v.get("gpu_info").is_none()); // 无 GPU → 缺席
        let s = build_sample(&d);
        let sv = serde_json::to_value(&s).unwrap();
        assert_eq!(sv["ts"], 1000);
        assert_eq!(sv["metrics"]["net_in_speed"], 11_200);
        assert!(build_samples(std::slice::from_ref(&d), false).is_empty());
        assert_eq!(build_samples(&[d], true).len(), 1);
    }

    #[test]
    fn parse_config_push() {
        let body = "collect_interval=0&report_interval=60&reset_day=15&schema_version=3\
                    &custom_ct=gd-ct-dualstack.ip.zstaticcdn.com&custom_cu=&custom_cm=m.example.com\
                    &custom_bd=ip.zstaticcdn.com&interface=eth0";
        let r = parse_response_body(body, Some("5f4dcc3b"));
        let p = r.push.unwrap();
        assert_eq!(p.version, "5f4dcc3b");
        assert_eq!(p.collect, Some(1)); // CF 的 0 输入兼容映射为内部 1 秒
        assert_eq!(p.report, Some(60));
        assert_eq!(p.reset_day, Some(15));
        assert_eq!(
            p.custom[0].as_deref(),
            Some("gd-ct-dualstack.ip.zstaticcdn.com")
        );
        assert_eq!(p.custom[1].as_deref(), Some("")); // 空值保留语义：该组清空
        assert_eq!(p.interface.as_deref(), Some("eth0"));
        assert!(r.correction.is_none());
    }

    #[test]
    fn synthesize_remote_maps_collect_zero_and_splits_interfaces() {
        let push = CfPush {
            version: "v1".into(),
            collect: Some(0),
            report: Some(60),
            reset_day: None,
            custom: [None, None, None, None],
            interface: Some("eth0, eth1,,bond*".into()),
        };
        let remote = synthesize_remote(&push, &crate::model::Intervals::default());
        assert_eq!(remote.intervals.unwrap().collect, 1);
        assert_eq!(remote.interfaces.unwrap(), vec!["eth0", "eth1", "bond*"]);
    }

    #[test]
    fn parse_correction_and_ok() {
        let r = parse_response_body(
            "collect_interval=5&report_interval=60&rx_correction=10&tx_correction=2.5",
            None,
        );
        assert_eq!(r.correction, Some((10.0, 2.5)));
        // MD5 头缺失时 version 从配置字段重建（不含校正字段，避免空转）
        let v = r.push.unwrap().version;
        assert!(v.starts_with("ci=Some(5)&ri=Some(60)"), "got: {v}");
        assert!(!v.contains("correction"), "got: {v}");
        assert!(parse_response_body("OK", None).push.is_none());
        assert!(parse_response_body("", None).correction.is_none());
        // 非法校正值：忽略
        let r2 = parse_response_body("rx_correction=-3&tx_correction=abc", None);
        assert!(r2.correction.is_none());
    }
}
