//! Linux 磁盘 IO：/proc/diskstats 差值。
//! 只统计整盘（跳过分区/loop/ram/dm/md，避免与整盘重复计数）；
//! sector 固定 512 字节；io 进行中时长（第 13 列）按盘保留，usage 取各盘最大

use super::super::{DiskIoCounters, DiskIoDeviceCounters};

/// 整盘判定：nvme0n1 / mmcblk0 / sda / vda / xvda / hda；
/// 分区（nvme0n1p1、sda1、mmcblk0p1）与其余虚拟设备排除
fn is_whole_disk(name: &str) -> bool {
    if name.starts_with("nvme") || name.starts_with("mmcblk") {
        return !name.contains('p');
    }
    if name.starts_with("loop")
        || name.starts_with("ram")
        || name.starts_with("dm-")
        || name.starts_with("md")
        || name.starts_with("zram")
        || name.starts_with("sr")
    {
        return false;
    }
    // sdX / vdX / xvdX / hdX：以字母结尾为整盘
    name.bytes().last().is_some_and(|b| b.is_ascii_lowercase())
        && name.bytes().next().is_some_and(|b| b.is_ascii_lowercase())
}

pub fn parse_diskstats(data: &str) -> DiskIoCounters {
    let mut out = DiskIoCounters::default();
    for line in data.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 14 || !is_whole_disk(f[2]) {
            continue;
        }
        let v = |i: usize| f[i].parse::<u64>().unwrap_or(0);
        out.devices.insert(
            f[2].to_string(),
            DiskIoDeviceCounters {
                read_ops: v(3),
                read_bytes: v(5) * 512,
                write_ops: v(7),
                write_bytes: v(9) * 512,
                total_time_ms: v(6) + v(10),
                io_time_ms: Some(v(12)),
            },
        );
        out.read_ops += v(3);
        out.read_bytes += v(5) * 512;
        out.write_ops += v(7);
        out.write_bytes += v(9) * 512;
        out.total_time_ms += v(6) + v(10);
        out.io_ms_per_dev.insert(f[2].to_string(), v(12));
    }
    out
}

pub fn read_counters() -> Option<DiskIoCounters> {
    let data = std::fs::read_to_string("/proc/diskstats").ok()?;
    Some(parse_diskstats(&data))
}

#[cfg(test)]
mod tests {
    use super::super::super::{disk_io_diff, DiskIoCounters};
    use super::*;

    const SAMPLE: &str = "\
   8       0 sda 100 0 2048 50 200 0 4096 100 0 60 70 0 0 0
   8       1 sda1 5 0 40 1 2 0 16 2 0 3 3 0 0 0
 259       0 nvme0n1 300 0 6144 150 400 0 8192 200 0 120 130 0 0 0
 259       1 nvme0n1p1 9 0 72 1 4 0 32 4 0 5 5 0 0 0
   7       0 loop0 1 0 8 0 0 0 0 0 0 0 0 0 0 0
";

    #[test]
    fn parses_whole_disks_only() {
        let c = parse_diskstats(SAMPLE);
        // sda + nvme0n1；分区与 loop 被排除
        assert_eq!(c.read_ops, 400);
        assert_eq!(c.read_bytes, (2048 + 6144) * 512);
        assert_eq!(c.write_bytes, (4096 + 8192) * 512);
        assert_eq!(c.total_time_ms, 50 + 100 + 150 + 200);
        assert_eq!(c.io_ms_per_dev.get("sda"), Some(&60));
        assert_eq!(c.io_ms_per_dev.get("nvme0n1"), Some(&120));
        assert!(!c.io_ms_per_dev.contains_key("sda1"));
    }

    fn counters(io: &[(&str, u64)]) -> DiskIoCounters {
        DiskIoCounters {
            read_bytes: 0,
            write_bytes: 0,
            read_ops: 0,
            write_ops: 0,
            total_time_ms: 0,
            io_ms_per_dev: io.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
            devices: Default::default(),
        }
    }

    #[test]
    fn diff_rates() {
        let mut a = counters(&[("sda", 200)]);
        a.read_bytes = 1000;
        a.write_bytes = 500;
        a.read_ops = 10;
        a.write_ops = 5;
        a.total_time_ms = 150;
        let mut b = counters(&[("sda", 500)]);
        b.read_bytes = 3000;
        b.write_bytes = 900;
        b.read_ops = 20;
        b.write_ops = 10;
        b.total_time_ms = 450;
        let r = disk_io_diff(Some((a, 10_000)), b, 12_000);
        assert_eq!(r.read_bps, Some(1000.0));
        assert_eq!(r.write_bps, Some(200.0));
        assert_eq!(r.read_iops, Some(5.0));
        assert_eq!(r.write_iops, Some(2.5));
        assert_eq!(r.await_ms, Some(20.0)); // (450-150)ms / 15 ops
        assert_eq!(r.usage, Some(15.0)); // 300ms / 2000ms
    }

    #[test]
    fn usage_takes_max_not_sum_across_disks() {
        // 两盘各 30%：usage 应为 30% 而非 60%/100%
        let a = counters(&[("sda", 0), ("sdb", 0)]);
        let b = counters(&[("sda", 600), ("sdb", 600)]);
        let r = disk_io_diff(Some((a, 0)), b, 2000);
        assert_eq!(r.usage, Some(30.0));
    }

    #[test]
    fn short_interval_guarded() {
        let a = counters(&[("sda", 0)]);
        let mut b = counters(&[("sda", 50)]);
        b.read_bytes = 1_000_000;
        // ticker 重建后立即 tick：50ms 差值直接丢弃，不报幻影尖峰
        let r = disk_io_diff(Some((a, 1000)), b, 1050);
        assert_eq!(r.read_bps, None);
        assert_eq!(r.usage, None);
    }

    #[test]
    fn diff_first_round_is_none() {
        let r = disk_io_diff(None, DiskIoCounters::default(), 1000);
        assert_eq!(r.read_bps, None);
    }
}
