//! 进程数：/proc 下数字目录计数

pub fn collect() -> Option<u64> {
    let entries = std::fs::read_dir("/proc").ok()?;
    let mut count = 0u64;
    for entry in entries.flatten() {
        if entry
            .file_name()
            .to_str()
            .is_some_and(|n| n.bytes().next().is_some_and(|b| b.is_ascii_digit()))
        {
            count += 1;
        }
    }
    Some(count)
}
