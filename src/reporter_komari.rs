//! komari 协议映射（komari-monitor 面板，JSON-RPC 2.0 over WebSocket）
//!
//! - agent.report：内层是扁平的指标对象（字节单位、无 ts/批量语义，只报最新值）
//! - agent.basicInfo：静态信息（连接建立 + 周期刷新时发送）
//! - komari 的月流量由面板侧自算；ping 是服务端任务制，我们的 [[pings]] 无落点；
//!   服务端下行方法（exec/terminal 等）一律忽略（安全：不做远程执行）

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
        "disk": { "total": st.disk_total, "used": slow.and_then(|s| s.disk_used).unwrap_or(0) },
        "network": network,
        "uptime": ((now_ms - st.boot_time).max(0) / 1000) as u64,
        "process": slow.and_then(|s| s.processes).unwrap_or(0),
        "message": errors.iter().map(|e| format!("[{}] {}", e.source, e.msg))
            .collect::<Vec<_>>().join("; "),
    });
    if let Some(l) = d.load {
        out["load"] = json!({ "load1": l[0], "load5": l[1], "load15": l[2] });
    }
    if let Some(s) = slow {
        out["connections"] = json!({
            "tcp": s.tcp_conn.unwrap_or(0),
            "udp": s.udp_conn.unwrap_or(0),
        });
    }
    if !gpus.is_empty() {
        let avg = gpus.iter().filter_map(|g| g.usage).sum::<f64>() / gpus.len() as f64;
        out["gpu"] = json!({
            "count": gpus.len(),
            "average_usage": avg,
            "detailed_info": gpus.iter().map(|g| json!({
                "name": g.name,
                "memory_total": g.mem_total.unwrap_or(0),
                "memory_used": g.mem_used.unwrap_or(0),
                "utilization": g.usage.unwrap_or(0.0),
                "temperature": g.temp.unwrap_or(0.0) as u64,
            })).collect::<Vec<_>>(),
        });
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
            gpu_name: None,
            virtualization: Some("kvm".into()),
            boot_time: 900,
            ipv4: Some("1.2.3.4".into()),
            ipv6: None,
            agent_version: "0.1.1".into(),
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
    fn report_shape() {
        let st = stub();
        let d = DynamicRecord {
            ts: 1000,
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
            tcp_conn: Some(3),
            udp_conn: Some(1),
            processes: Some(99),
        };
        let errs = vec![ErrorRecord {
            ts: 1,
            source: "gpu".into(),
            msg: "x".into(),
        }];
        let v = build_report(&st, Some(&d), Some(&slow), &[], &errs, 1900);
        assert_eq!(v["cpu"]["usage"], 12.3);
        assert_eq!(v["ram"]["total"], 16_000_000_000u64);
        assert_eq!(v["ram"]["used"], 8_000_000_000u64);
        assert_eq!(v["disk"]["used"], 4_000_000_000u64);
        assert_eq!(v["load"]["load1"], 0.1);
        assert_eq!(v["network"]["up"], 512); // up = tx
        assert_eq!(v["network"]["totalDown"], 100);
        assert_eq!(v["connections"]["tcp"], 3);
        assert_eq!(v["uptime"], 1); // (1900-900)/1000
        assert_eq!(v["process"], 99);
        assert_eq!(v["message"], "[gpu] x");
        assert!(v.get("gpu").is_none());
        let bi = build_basic_info(&st, "0.1.1");
        assert_eq!(bi["version"], "probe-rs_0.1.1");
        assert_eq!(bi["virtualization"], "kvm");
        assert_eq!(bi["ipv6"], "");
    }

    #[test]
    fn frames_are_jsonrpc_notifications() {
        let f: Value = serde_json::from_str(&report_frame(json!({"a": 1}))).unwrap();
        assert_eq!(f["jsonrpc"], "2.0");
        assert_eq!(f["method"], "agent.report");
        assert_eq!(f["params"]["report"]["a"], 1);
        assert!(f.get("id").is_none());
    }
}
