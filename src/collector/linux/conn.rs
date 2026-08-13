//! TCP/UDP 连接数：/proc/net/{tcp,tcp6,udp,udp6} 行计数（TCP 全状态）

use super::scan_file;

fn count_entries(path: &str) -> std::io::Result<u64> {
    let mut count = 0u64;
    let mut header = true;
    scan_file(path, |line| {
        if header {
            header = false;
            return;
        }
        if !line.trim().is_empty() {
            count += 1;
        }
    })?;
    Ok(count)
}

/// (tcp_conn, udp_conn)。同类两个文件(tcp/tcp6、udp/udp6)全部不可读才报错;
/// 单个文件缺失(如内核未启用 IPv6)按 0 计。绝不把采集故障伪装成"0 条连接"。
pub fn collect() -> Result<(u64, u64), String> {
    let tcp = match (
        count_entries("/proc/net/tcp"),
        count_entries("/proc/net/tcp6"),
    ) {
        (Ok(v4), Ok(v6)) => v4 + v6,
        (Ok(count), Err(_)) | (Err(_), Ok(count)) => count,
        (Err(error), Err(_)) => return Err(format!("TCP 连接表不可读: {error}")),
    };
    let udp = match (
        count_entries("/proc/net/udp"),
        count_entries("/proc/net/udp6"),
    ) {
        (Ok(v4), Ok(v6)) => v4 + v6,
        (Ok(count), Err(_)) | (Err(_), Ok(count)) => count,
        (Err(error), Err(_)) => return Err(format!("UDP 连接表不可读: {error}")),
    };
    Ok((tcp, udp))
}
