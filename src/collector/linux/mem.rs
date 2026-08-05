//! 内存/Swap：/proc/meminfo

use std::collections::HashMap;

use super::scan_file;

fn read_meminfo() -> HashMap<String, u64> {
    let mut map = HashMap::new();
    let _ = scan_file("/proc/meminfo", |line| {
        if let Some((key, rest)) = line.split_once(':') {
            let kb: u64 = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            map.insert(key.to_string(), kb);
        }
    });
    map
}

fn get(map: &HashMap<String, u64>, key: &str) -> u64 {
    map.get(key).copied().unwrap_or(0)
}

/// (mem_total, mem_used, swap_total, swap_used)，字节
pub fn collect() -> (u64, u64, u64, u64) {
    let m = read_meminfo();
    let total = get(&m, "MemTotal");
    let mut available = get(&m, "MemAvailable");
    if available == 0 {
        available = get(&m, "MemFree") + get(&m, "Buffers") + get(&m, "Cached");
    }
    let used = total.saturating_sub(available);
    let swap_total = get(&m, "SwapTotal");
    let swap_used = swap_total.saturating_sub(get(&m, "SwapFree"));
    (
        total * 1024,
        used * 1024,
        swap_total * 1024,
        swap_used * 1024,
    )
}
