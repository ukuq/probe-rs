//! CPU 使用率：/proc/stat 差值

use super::scan_file;

#[derive(Debug, Clone, Copy, Default)]
pub struct CpuTimes {
    pub total: u64,
    pub idle: u64,
}

pub fn read_cpu_times() -> Option<CpuTimes> {
    let mut result = None;
    scan_file("/proc/stat", |line| {
        if result.is_some() || !line.starts_with("cpu ") {
            return;
        }
        let fields: Vec<u64> = line[3..]
            .split_whitespace()
            .take(8)
            .filter_map(|s| s.parse().ok())
            .collect();
        if fields.len() < 5 {
            return;
        }
        let total: u64 = fields.iter().sum();
        // idle + iowait
        let idle = fields[3] + fields[4];
        result = Some(CpuTimes { total, idle });
    })
    .ok()?;
    result
}

/// 两次采样做差；计数器回退或无增量时返回 None
pub fn usage_percent(prev: CpuTimes, current: CpuTimes) -> Option<f64> {
    if current.total < prev.total || current.idle < prev.idle {
        return None;
    }
    let total_delta = current.total - prev.total;
    let idle_delta = current.idle - prev.idle;
    if total_delta == 0 || idle_delta > total_delta {
        return None;
    }
    Some((total_delta - idle_delta) as f64 / total_delta as f64 * 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_calc() {
        let prev = CpuTimes {
            total: 1000,
            idle: 500,
        };
        let cur = CpuTimes {
            total: 2000,
            idle: 750,
        };
        let usage = usage_percent(prev, cur).unwrap();
        assert!((usage - 75.0).abs() < 0.001);
    }

    #[test]
    fn counter_regression() {
        let prev = CpuTimes {
            total: 2000,
            idle: 750,
        };
        let cur = CpuTimes {
            total: 100,
            idle: 50,
        };
        assert!(usage_percent(prev, cur).is_none());
    }
}
