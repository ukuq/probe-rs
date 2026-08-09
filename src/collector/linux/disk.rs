//! 磁盘：遍历 /proc/mounts + statfs，按设备去重求和（移植 cfsm 口径）

use std::collections::HashMap;
use std::path::Path;

use super::scan_file;
use crate::model::DiskVolume;

const EXCLUDED_MOUNT_PREFIXES: &[&str] = &[
    "/tmp",
    "/var/tmp",
    "/dev",
    "/run",
    "/var/lib/containers",
    "/var/lib/docker",
    "/proc",
    "/sys",
    "/sys/fs/cgroup",
    "/etc/resolv.conf",
    "/etc/host",
    "/nix/store",
];

const EXCLUDED_FS_PREFIXES: &[&str] = &[
    "tmpfs",
    "devtmpfs",
    "udev",
    "nfs",
    "cifs",
    "smb",
    "vboxsf",
    "virtiofs",
    "9p",
    "fuse",
    "overlay",
    "proc",
    "devpts",
    "sysfs",
    "cgroup",
    "mqueue",
    "hugetlbfs",
    "debugfs",
    "binfmt_misc",
    "securityfs",
];

pub fn collect_volumes() -> Vec<DiskVolume> {
    let mut devices: HashMap<String, DiskVolume> = HashMap::new();
    let _ = scan_file("/proc/mounts", |line| {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 3 {
            return;
        }
        let dev = fields[0];
        let mount_point = unescape_mount_point(fields[1]);
        let fs_type = fields[2].to_lowercase();
        let opts = fields.get(3).copied().unwrap_or("").to_lowercase();
        if !include_mount(dev, &mount_point, &fs_type, &opts) {
            return;
        }
        let Ok(st) = nix::sys::statfs::statfs(Path::new(&mount_point)) else {
            return;
        };
        let bsize = st.block_size().max(0) as u64;
        let blocks = st.blocks();
        let bfree = st.blocks_free();
        if bsize == 0 || blocks < bfree {
            return;
        }
        let total = blocks * bsize;
        let used = (blocks - bfree) * bsize;
        if total == 0 {
            return;
        }
        // ZFS 按 pool 名截断（pool/dataset → pool）
        let device_id = if fs_type == "zfs" {
            dev.split('/').next().unwrap_or(dev).to_string()
        } else {
            dev.to_string()
        };
        // 同设备多挂载点取 total 最大的（处理 quota 等情况）
        match devices.get(&device_id) {
            Some(existing) if existing.total >= total => {}
            _ => {
                devices.insert(
                    device_id.clone(),
                    DiskVolume {
                        id: device_id,
                        name: dev.to_string(),
                        mount_point,
                        file_system: fs_type,
                        total,
                        used,
                    },
                );
            }
        }
    });
    let mut volumes: Vec<_> = devices.into_values().collect();
    volumes.sort_by(|a, b| a.id.cmp(&b.id));
    volumes
}

fn include_mount(dev: &str, mount_point: &str, fs_type: &str, opts: &str) -> bool {
    if mount_point == "/" {
        return true;
    }
    let mp = mount_point.to_lowercase();
    for prefix in EXCLUDED_MOUNT_PREFIXES {
        if mp.starts_with(prefix) {
            return false;
        }
    }
    if fs_type == "autofs" && !dev.starts_with("/dev/") {
        return false;
    }
    if fs_type == "fuseblk" {
        return true;
    }
    for excluded in EXCLUDED_FS_PREFIXES {
        if fs_type.starts_with(excluded) {
            return false;
        }
    }
    if opts.contains("remote") || opts.contains("network") {
        return false;
    }
    if dev.starts_with("/dev/loop") {
        return false;
    }
    true
}

fn unescape_mount_point(raw: &str) -> String {
    raw.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_filter() {
        assert!(include_mount("/dev/sda1", "/", "ext4", "rw"));
        assert!(include_mount("/dev/sdb1", "/data", "xfs", "rw"));
        assert!(!include_mount("tmpfs", "/tmp", "tmpfs", "rw"));
        assert!(!include_mount(
            "overlay",
            "/var/lib/docker/overlay2/x",
            "overlay",
            "rw"
        ));
        assert!(!include_mount("//nas/share", "/mnt/nas", "nfs", "rw"));
        assert!(!include_mount(
            "/dev/loop0",
            "/snap/core/123",
            "squashfs",
            "ro"
        ));
        assert!(include_mount("/dev/sdc1", "/mnt/ntfs", "fuseblk", "rw"));
    }

    #[test]
    fn unescape() {
        assert_eq!(unescape_mount_point("/mnt/my\\040disk"), "/mnt/my disk");
    }
}
