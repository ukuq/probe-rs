//! 探针自身资源占用：/proc/self 读取（CPU 差值 + VmRSS）

/// Linux 的 CLK_TCK 实际全平台都是 100
const CLK_TCK: u64 = 100;

#[derive(Debug, Default)]
pub struct SelfMonitor {
    prev: Option<(u64, std::time::Instant)>,
}

#[derive(Debug, Clone, Copy)]
pub struct SelfStats {
    pub cpu_usage: Option<f64>,
    pub mem_rss: Option<u64>,
}

fn self_ticks() -> Option<u64> {
    let data = std::fs::read_to_string("/proc/self/stat").ok()?;
    // comm 可能含空格/括号：取最后一个 ')' 之后
    let after = data.rfind(')')?;
    let fields: Vec<&str> = data[after + 1..].split_whitespace().collect();
    // after ')' 起：state ppid pgrp session tty_nr tpgid flags minflt cminflt majflt cmajflt utime stime
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

fn self_rss_bytes() -> Option<u64> {
    let data = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in data.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.trim().trim_end_matches(" kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

impl SelfMonitor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sample(&mut self) -> SelfStats {
        let mem_rss = self_rss_bytes();
        let cpu_usage = match self_ticks() {
            Some(ticks) => {
                let now = std::time::Instant::now();
                let usage = self.prev.and_then(|(prev_ticks, prev_at)| {
                    let dt = now.duration_since(prev_at).as_secs_f64();
                    if dt <= 0.0 || ticks < prev_ticks {
                        return None;
                    }
                    Some((ticks - prev_ticks) as f64 / CLK_TCK as f64 / dt * 100.0)
                });
                self.prev = Some((ticks, now));
                usage
            }
            None => None,
        };
        SelfStats { cpu_usage, mem_rss }
    }
}
