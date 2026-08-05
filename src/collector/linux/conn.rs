//! TCP/UDP 连接数：/proc/net/{tcp,tcp6,udp,udp6} 行计数（TCP 全状态）

use super::scan_file;

fn count_entries(path: &str) -> u64 {
    let mut count = 0u64;
    let mut header = true;
    let _ = scan_file(path, |line| {
        if header {
            header = false;
            return;
        }
        if !line.trim().is_empty() {
            count += 1;
        }
    });
    count
}

/// (tcp_conn, udp_conn)
pub fn collect() -> (u64, u64) {
    let tcp = count_entries("/proc/net/tcp") + count_entries("/proc/net/tcp6");
    let udp = count_entries("/proc/net/udp") + count_entries("/proc/net/udp6");
    (tcp, udp)
}
