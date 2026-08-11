//! komari 协议映射（komari-monitor 面板，JSON-RPC 2.0 over WebSocket）
//!
//! - agent.report：内层是扁平的指标对象（字节单位、无 ts/批量语义，只报最新值）
//! - agent.basicInfo：静态信息（连接建立 + 信息变化时发送）
//! - komari 的月流量由面板侧自算；服务端 Ping 任务只读取 Agent 本地采集缓存；
//!   exec/terminal 等远程控制请求只回传禁用结果，绝不执行命令

use serde_json::{json, Value};

use crate::model::{DynamicRecord, ErrorRecord, GpuRecord, SlowBlock, StaticInfo};

/// komari agent.report 的内层指标对象（只含最新值；缺失字段不输出）
pub fn build_report(
    st: &StaticInfo,
    dyn_latest: Option<&DynamicRecord>,
    slow: Option<&SlowBlock>,
    gpus: &[GpuRecord],
    errors: &[ErrorRecord],
    now_ms: i64,
) -> Value {
    let d = dyn_latest.cloned().unwrap_or_default();
    let mut network = json!({});
    if let Some(v) = d.net_tx_speed {
        network["up"] = json!(v);
    }
    if let Some(v) = d.net_rx_speed {
        network["down"] = json!(v);
    }
    if let Some(v) = d.net_tx {
        network["totalUp"] = json!(v);
    }
    if let Some(v) = d.net_rx {
        network["totalDown"] = json!(v);
    }
    let mut out = json!({
        "cpu": { "usage": d.cpu_usage.unwrap_or(0.0) },
        "ram": { "total": st.mem_total, "used": d.mem_used.unwrap_or(0) },
        "swap": { "total": st.swap_total, "used": d.swap_used.unwrap_or(0) },
        "disk": { "total": st.disk_total },
        "network": network,
        "uptime": ((now_ms - st.boot_time).max(0) / 1000) as u64,
        "message": errors.iter().map(|e| format!("[{}] {}", e.source, e.msg))
            .collect::<Vec<_>>().join("; "),
    });
    if let Some(l) = d.load {
        out["load"] = json!({ "load1": l[0], "load5": l[1], "load15": l[2] });
    }
    if let Some(s) = slow {
        if let Some(value) = s.disk_used {
            out["disk"]["used"] = json!(value);
        }
        if let Some(value) = s.processes {
            out["process"] = json!(value);
        }
        let mut connections = json!({});
        if let Some(value) = s.tcp_conn {
            connections["tcp"] = json!(value);
        }
        if let Some(value) = s.udp_conn {
            connections["udp"] = json!(value);
        }
        if connections
            .as_object()
            .is_some_and(|value| !value.is_empty())
        {
            out["connections"] = connections;
        }
    }
    if !gpus.is_empty() {
        let usages: Vec<_> = gpus.iter().filter_map(|g| g.usage).collect();
        let detailed_info: Vec<_> = gpus
            .iter()
            .map(|g| {
                let mut detail = json!({ "name": g.name });
                if let Some(value) = g.mem_total {
                    detail["memory_total"] = json!(value);
                }
                if let Some(value) = g.mem_used {
                    detail["memory_used"] = json!(value);
                }
                if let Some(value) = g.usage {
                    detail["utilization"] = json!(value);
                }
                if let Some(value) = g.temp {
                    detail["temperature"] = json!(value as u64);
                }
                detail
            })
            .collect();
        let mut gpu = json!({
            "count": gpus.len(),
            "detailed_info": detailed_info,
        });
        if !usages.is_empty() {
            gpu["average_usage"] = json!(usages.iter().sum::<f64>() / usages.len() as f64);
        }
        out["gpu"] = gpu;
    }
    out
}

/// komari agent.basicInfo 的 info 对象
pub fn build_basic_info(st: &StaticInfo, agent_version: &str) -> Value {
    json!({
        "cpu_name": st.cpu_name,
        "cpu_cores": st.cpu_cores,
        "cpu_physical_cores": st.cpu_physical_cores.unwrap_or(0),
        "arch": st.arch,
        "os": st.os,
        "kernel_version": st.kernel,
        "ipv4": st.ipv4.clone().unwrap_or_default(),
        "ipv6": st.ipv6.clone().unwrap_or_default(),
        "mem_total": st.mem_total,
        "swap_total": st.swap_total,
        "disk_total": st.disk_total,
        "gpu_name": st.gpu_name.clone().unwrap_or_default(),
        "virtualization": st.virtualization.clone().unwrap_or_default(),
        "version": format!("probe-rs_{agent_version}"),
    })
}

/// v2 JSON-RPC notification 帧
pub fn notification(method: &str, params: Value) -> String {
    json!({ "jsonrpc": "2.0", "method": method, "params": params }).to_string()
}

pub fn report_frame(report: Value) -> String {
    notification("agent.report", json!({ "report": report }))
}

pub fn basic_info_frame(info: Value) -> String {
    notification("agent.basicInfo", json!({ "info": info }))
}

/// 仅用于维持 Komari WebSocket 读循环，不携带指标、不拉取事件。
///
/// 这是没有 `id` 和 `params` 的 JSON-RPC notification。现有 Komari
/// 服务端读取文本帧后会刷新读超时，并静默忽略未知 notification。
pub fn heartbeat_frame() -> String {
    json!({ "jsonrpc": "2.0", "method": "agent.heartbeat" }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::StaticInfo;

    fn stub() -> StaticInfo {
        StaticInfo {
            ts: 1,
            os: "Debian 13".into(),
            kernel: "7.0".into(),
            arch: "aarch64".into(),
            cpu_name: "aarch64".into(),
            cpu_cores: 14,
            cpu_physical_cores: Some(14),
            mem_total: 16_000_000_000,
            swap_total: 1_000_000_000,
            disk_total: 80_000_000_000,
            disks: vec![],
            gpu_name: None,
            virtualization: Some("kvm".into()),
            boot_time: 1200,
            ipv4: Some("1.2.3.4".into()),
            ipv6: None,
            agent_version: "0.1.1".into(),
            config: crate::model::StaticConfig {
                global: Default::default(),
                reporters: vec![],
                reset_day: 1,
                intervals: crate::model::Intervals::default(),
                interfaces: vec![],
                disks: vec![],
                enable_gpu: false,
                report_errors: true,
                report_self: true,
                pings: vec![],
                ext: None,
            },
        }
    }

    #[test]
    fn report_shape() {
        let st = stub();
        let d = DynamicRecord {
            ts: 1000,
            accurate_ts: Some(1300),
            cpu_usage: Some(12.3),
            mem_used: Some(8_000_000_000),
            swap_used: Some(5),
            load: Some([0.1, 0.2, 0.3]),
            net_rx: Some(100),
            net_tx: Some(50),
            net_rx_speed: Some(1024),
            net_tx_speed: Some(512),
            net_rx_monthly: Some(1),
            net_tx_monthly: Some(2),
            net_interfaces: Default::default(),
        };
        let slow = SlowBlock {
            ts: 999,
            disk_used: Some(4_000_000_000),
            disks: vec![],
            tcp_conn: Some(3),
            udp_conn: Some(1),
            processes: Some(99),
        };
        let errs = vec![ErrorRecord {
            ts: 1,
            source: "gpu".into(),
            msg: "x".into(),
        }];
        let v = build_report(&st, Some(&d), Some(&slow), &[], &errs, 2200);
        assert_eq!(v["cpu"]["usage"], 12.3);
        assert_eq!(v["ram"]["total"], 16_000_000_000u64);
        assert_eq!(v["ram"]["used"], 8_000_000_000u64);
        assert_eq!(v["disk"]["used"], 4_000_000_000u64);
        assert_eq!(v["load"]["load1"], 0.1);
        assert_eq!(v["network"]["up"], 512); // up = tx
        assert_eq!(v["network"]["totalDown"], 100);
        assert_eq!(v["connections"]["tcp"], 3);
        assert_eq!(v["uptime"], 1); // (accurate 2200 - corrected boot 1200)/1000
        assert_eq!(v["process"], 99);
        assert_eq!(v["message"], "[gpu] x");
        assert!(v.get("gpu").is_none());
        let bi = build_basic_info(&st, "0.1.1");
        assert_eq!(bi["version"], "probe-rs_0.1.1");
        assert_eq!(bi["virtualization"], "kvm");
        assert_eq!(bi["ipv6"], "");
    }

    #[test]
    fn gpu_average_and_details_ignore_unknown_values() {
        let gpus = vec![
            GpuRecord {
                ts: 1,
                id: "0".into(),
                name: "measured".into(),
                usage: Some(50.0),
                mem_total: Some(8_000),
                mem_used: None,
                temp: Some(45.9),
            },
            GpuRecord {
                ts: 1,
                id: "1".into(),
                name: "unknown".into(),
                usage: None,
                mem_total: None,
                mem_used: None,
                temp: None,
            },
        ];
        let report = build_report(&stub(), None, None, &gpus, &[], 1_900);
        assert_eq!(report["gpu"]["count"], 2);
        assert_eq!(report["gpu"]["average_usage"], 50.0);
        assert_eq!(report["gpu"]["detailed_info"][0]["memory_total"], 8_000);
        assert_eq!(report["gpu"]["detailed_info"][0]["temperature"], 45);
        assert!(report["gpu"]["detailed_info"][0]
            .get("memory_used")
            .is_none());
        assert!(report["gpu"]["detailed_info"][1]
            .get("utilization")
            .is_none());
        assert!(report["gpu"]["detailed_info"][1]
            .get("memory_total")
            .is_none());

        let report = build_report(&stub(), None, None, &gpus[1..], &[], 1_900);
        assert!(report["gpu"].get("average_usage").is_none());
    }

    #[test]
    fn missing_slow_snapshot_omits_only_slow_fields() {
        let dynamic = DynamicRecord {
            cpu_usage: Some(12.5),
            mem_used: Some(4_000),
            ..Default::default()
        };
        let report = build_report(&stub(), Some(&dynamic), None, &[], &[], 1_900);

        assert_eq!(report["cpu"]["usage"], 12.5);
        assert_eq!(report["ram"]["used"], 4_000);
        assert!(report["disk"].get("used").is_none());
        assert!(report.get("process").is_none());
        assert!(report.get("connections").is_none());
    }

    #[test]
    fn frames_are_jsonrpc_notifications() {
        let f: Value = serde_json::from_str(&report_frame(json!({"a": 1}))).unwrap();
        assert_eq!(f["jsonrpc"], "2.0");
        assert_eq!(f["method"], "agent.report");
        assert_eq!(f["params"]["report"]["a"], 1);
        assert!(f.get("id").is_none());

        let heartbeat: Value = serde_json::from_str(&heartbeat_frame()).unwrap();
        assert_eq!(heartbeat["jsonrpc"], "2.0");
        assert_eq!(heartbeat["method"], "agent.heartbeat");
        assert!(heartbeat.get("id").is_none());
        assert!(heartbeat.get("params").is_none());
        assert_ne!(heartbeat["method"], "agent.pull");
    }
}
