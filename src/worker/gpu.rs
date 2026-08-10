//! GPU worker：可选硬件指标（仅部分机器有显卡，enable_gpu 开关）
//!
//! 采集靠 fork 外部命令（Linux/Windows 用 nvidia-smi；macOS 用
//! system_profiler + ioreg），比文件读取重得多，且可挂可失败——
//! 因此独立于 slow：不同存在性、不同故障域、多卡时每卡一条记录。
//! 只发布最近一次测量的快照（带真实测量 ts）；GPU 型号经另一个 channel 供 static。
//! 失败即缺席：快照不更新，采集端看到 ts 停滞便知异常。

use std::time::Duration;

use tokio::sync::watch;
use tokio::time::interval;

use std::sync::Arc;

use crate::buffer::Buffers;
use crate::model::{GpuRecord, Intervals};

/// 一卡一条的采样结果；mem/temp 仅 nvidia 路径有，macOS 为 None
#[derive(Debug, Clone)]
pub struct GpuSample {
    pub id: String,
    pub name: String,
    pub usage: Option<f64>,
    pub mem_total: Option<u64>,
    pub mem_used: Option<u64>,
    pub temp: Option<f64>,
}

/// 启动 GPU 采集任务。channel 由调用方持有：任务可随 enable_gpu 热切换
/// 启停重建，scheduler 侧的 rx 不受影响
pub fn start(
    name_tx: watch::Sender<Option<String>>,
    gpu_tx: watch::Sender<Vec<GpuRecord>>,
    buffers: Arc<Buffers>,
    mut intervals_rx: watch::Receiver<Intervals>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(intervals_rx.borrow().gpu.max(1)));
        loop {
            tokio::select! {
                _ = ticker.tick() => run_once(&name_tx, &gpu_tx, &buffers).await,
                r = intervals_rx.changed() => {
                    if r.is_err() { return; }
                    ticker = interval(Duration::from_secs(intervals_rx.borrow().gpu.max(1)));
                }
            }
        }
    })
}

async fn run_once(
    name_tx: &watch::Sender<Option<String>>,
    gpu_tx: &watch::Sender<Vec<GpuRecord>>,
    buffers: &Buffers,
) {
    match query_gpu().await {
        Ok(mut gpus) if !gpus.is_empty() => {
            gpus.sort_by(|a, b| a.id.cmp(&b.id));
            let ts = crate::model::now_millis();
            name_tx.send_if_modified(|cur| {
                let name = Some(gpus[0].name.clone());
                if *cur != name {
                    *cur = name;
                    true
                } else {
                    false
                }
            });
            gpu_tx.send_replace(
                gpus.into_iter()
                    .map(|g| GpuRecord {
                        ts,
                        id: g.id,
                        name: g.name,
                        usage: g.usage,
                        mem_total: g.mem_total,
                        mem_used: g.mem_used,
                        temp: g.temp,
                    })
                    .collect(),
            );
        }
        Ok(_) => {
            tracing::debug!("无 GPU 数据");
        }
        Err(e) => {
            // 失败即缺席：快照不更新；错误事件入队（同源同文去重）
            buffers.push_error("gpu", e.to_string());
            tracing::debug!(error = %e, "GPU 查询失败");
        }
    }
}

#[cfg(not(target_os = "macos"))]
async fn query_gpu() -> anyhow::Result<Vec<GpuSample>> {
    query_nvidia_smi().await
}

/// Linux/Windows：nvidia-smi（利用率 + 显存 + 温度，显存 MiB → 字节）
#[cfg(not(target_os = "macos"))]
async fn query_nvidia_smi() -> anyhow::Result<Vec<GpuSample>> {
    let output = tokio::process::Command::new("nvidia-smi")
        .args([
            "--query-gpu=index,name,utilization.gpu,memory.total,memory.used,temperature.gpu",
            "--format=csv,noheader,nounits",
        ])
        .output()
        .await?;
    if !output.status.success() {
        anyhow::bail!("nvidia-smi exit {}", output.status);
    }
    Ok(parse_nvidia_smi(&String::from_utf8_lossy(&output.stdout)))
}

#[cfg(any(not(target_os = "macos"), test))]
fn parse_nvidia_smi(out: &str) -> Vec<GpuSample> {
    out.lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.split(',').map(str::trim).collect();
            if parts.len() < 6 {
                return None;
            }
            if parts[0].is_empty() {
                return None;
            }
            let n = parts.len();
            let num = |s: &str| s.parse::<f64>().ok();
            let mib = |s: &str| s.parse::<u64>().ok().map(|v| v * 1024 * 1024);
            Some(GpuSample {
                id: parts[0].to_string(),
                name: parts[1..n - 4].join(", "),
                usage: num(parts[n - 4]).map(|v| v.clamp(0.0, 100.0)),
                mem_total: mib(parts[n - 3]),
                mem_used: mib(parts[n - 2]),
                temp: num(parts[n - 1]),
            })
        })
        .collect()
}

/// macOS：型号取 system_profiler，利用率取 ioreg IOAccelerator（无需 root）
#[cfg(target_os = "macos")]
async fn query_gpu() -> anyhow::Result<Vec<GpuSample>> {
    let names = gpu_names_macos().await;
    let usages = gpu_usages_macos().await?;
    if usages.is_empty() {
        anyhow::bail!("ioreg 无 GPU 利用率数据");
    }
    Ok(usages
        .into_iter()
        .enumerate()
        .map(|(i, usage)| {
            let name = names
                .get(i)
                .or_else(|| names.last())
                .cloned()
                .unwrap_or_else(|| "Apple GPU".to_string());
            GpuSample {
                id: fallback_gpu_id(&name, i),
                name,
                usage: Some(usage),
                // Apple Silicon 统一内存无独立显存；温度需 sudo powermetrics
                mem_total: None,
                mem_used: None,
                temp: None,
            }
        })
        .collect())
}

#[cfg(any(target_os = "macos", test))]
fn fallback_gpu_id(name: &str, occurrence: usize) -> String {
    let mut slug = String::with_capacity(name.len());
    let mut separator = false;
    for ch in name.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            separator = false;
        } else if !separator && !slug.is_empty() {
            slug.push('-');
            separator = true;
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    if slug.is_empty() {
        slug.push_str("gpu");
    }
    format!("macos:{slug}:{occurrence}")
}

#[cfg(target_os = "macos")]
async fn gpu_names_macos() -> Vec<String> {
    let Ok(output) = tokio::process::Command::new("system_profiler")
        .arg("SPDisplaysDataType")
        .output()
        .await
    else {
        return vec![];
    };
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|l| {
            l.trim()
                .strip_prefix("Chipset Model:")
                .map(|s| s.trim().to_string())
        })
        .collect()
}

#[cfg(target_os = "macos")]
async fn gpu_usages_macos() -> anyhow::Result<Vec<f64>> {
    let output = tokio::process::Command::new("ioreg")
        .args(["-r", "-d", "1", "-w", "0", "-c", "IOAccelerator"])
        .output()
        .await?;
    let text = String::from_utf8_lossy(&output.stdout);
    let mut usages = parse_ioreg_numbers(&text, "Device Utilization %");
    if usages.is_empty() {
        usages = parse_ioreg_numbers(&text, "Renderer Utilization %");
    }
    Ok(usages.into_iter().map(|u| u.clamp(0.0, 100.0)).collect())
}

#[cfg(target_os = "macos")]
fn parse_ioreg_numbers(out: &str, key: &str) -> Vec<f64> {
    let pattern = format!("\"{key}\"=");
    let mut values = Vec::new();
    let mut rest = out;
    while let Some(idx) = rest.find(&pattern) {
        let after = &rest[idx + pattern.len()..];
        let end = after
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(after.len());
        if end > 0 {
            if let Ok(v) = after[..end].parse::<f64>() {
                values.push(v);
            }
        }
        rest = &after[end..];
    }
    values
}

#[cfg(test)]
mod tests {
    #[test]
    #[cfg(target_os = "macos")]
    fn parses_ioreg_values() {
        let out = r#""Device Utilization %"=42 "Device Utilization %"=7"#;
        assert_eq!(
            super::parse_ioreg_numbers(out, "Device Utilization %"),
            vec![42.0, 7.0]
        );
    }

    #[test]
    fn parse_smi_output() {
        let out = "0, NVIDIA A100-SXM4-80GB, 42, 81920, 10240, 55\n1, NVIDIA A100-SXM4-80GB, 0, 81920, 128, 41\n";
        let gpus = super::parse_nvidia_smi(out);
        assert_eq!(gpus.len(), 2);
        assert_eq!(gpus[0].id, "0");
        assert_eq!(gpus[1].id, "1");
        assert_eq!(gpus[0].name, "NVIDIA A100-SXM4-80GB");
        assert!((gpus[0].usage.unwrap() - 42.0).abs() < f64::EPSILON);
        assert_eq!(gpus[0].mem_total, Some(81920 * 1024 * 1024));
        assert_eq!(gpus[0].mem_used, Some(10240 * 1024 * 1024));
        assert_eq!(gpus[0].temp, Some(55.0));
    }

    #[test]
    fn fallback_id_is_reproducible_and_distinguishes_identical_names() {
        assert_eq!(
            super::fallback_gpu_id("Apple M2 Max", 0),
            "macos:apple-m2-max:0"
        );
        assert_eq!(
            super::fallback_gpu_id("Apple M2 Max", 1),
            "macos:apple-m2-max:1"
        );
    }
}
