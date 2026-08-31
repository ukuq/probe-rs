use std::collections::BTreeSet;

pub(super) fn detect() -> Vec<String> {
    platform_conflicts().into_iter().collect()
}

fn process_kind(name: &str, command_line: &str) -> Option<&'static str> {
    let executable = name.rsplit(['/', '\\']).next().unwrap_or(name);
    if executable.eq_ignore_ascii_case("cf-probe")
        || executable.eq_ignore_ascii_case("cf-probe.exe")
    {
        return Some("running Go cf-probe process");
    }

    let command_line = command_line.to_ascii_lowercase();
    if command_line.contains("cf-probe.sh") {
        return Some("running legacy Shell cf-probe process");
    }
    if command_line.contains("cf-server-monitor.ps1")
        && command_line
            .split(|c: char| c.is_whitespace() || matches!(c, '\'' | '"'))
            .any(|part| part.eq_ignore_ascii_case("run"))
    {
        return Some("running legacy PowerShell CFProbe process");
    }
    None
}

#[cfg(target_os = "linux")]
fn platform_conflicts() -> BTreeSet<String> {
    linux_platform_conflicts()
}

// This implementation intentionally uses only portable std APIs. Keeping it
// type-checked on Windows catches regressions even when a Linux linker is not
// available in the local development environment.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn linux_platform_conflicts() -> BTreeSet<String> {
    use std::fs;
    use std::path::{Path, PathBuf};

    let mut conflicts = BTreeSet::new();
    for (kind, path) in [
        ("Go cf-probe executable", "/usr/local/bin/cf-probe"),
        ("Go cf-probe executable", "/usr/bin/cf-probe"),
        ("legacy Shell cf-probe script", "/usr/local/bin/cf-probe.sh"),
        (
            "legacy Shell cf-probe control file",
            "/usr/local/bin/cf-probe.sh.ctl",
        ),
        (
            "cf-probe systemd service",
            "/etc/systemd/system/cf-probe.service",
        ),
        (
            "cf-probe systemd service",
            "/usr/lib/systemd/system/cf-probe.service",
        ),
        (
            "cf-probe systemd service",
            "/lib/systemd/system/cf-probe.service",
        ),
        ("cf-probe OpenRC/SysV service", "/etc/init.d/cf-probe"),
        ("cf-probe Upstart service", "/etc/init/cf-probe.conf"),
    ] {
        if Path::new(path).exists() {
            conflicts.insert(format!("{kind}: {path}"));
        }
    }

    for home in linux_home_dirs() {
        for (kind, relative) in [
            ("user Go cf-probe executable", ".cf-probe/bin/cf-probe"),
            (
                "user cf-probe systemd service",
                ".config/systemd/user/cf-probe.service",
            ),
        ] {
            let path = home.join(relative);
            if path.exists() {
                conflicts.insert(format!("{kind}: {}", path.display()));
            }
        }
    }

    let Ok(entries) = fs::read_dir("/proc") else {
        return conflicts;
    };
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == std::process::id() || !same_linux_namespaces(pid) {
            continue;
        }
        let proc_dir = entry.path();
        let name = fs::read_to_string(proc_dir.join("comm"))
            .unwrap_or_default()
            .trim()
            .to_string();
        let command_line = fs::read(proc_dir.join("cmdline"))
            .map(|raw| {
                String::from_utf8_lossy(&raw)
                    .replace('\0', " ")
                    .trim()
                    .to_string()
            })
            .unwrap_or_default();
        if let Some(kind) = process_kind(&name, &command_line) {
            conflicts.insert(format!("{kind}: PID {pid}"));
        }
    }
    return conflicts;

    fn linux_home_dirs() -> BTreeSet<PathBuf> {
        let mut homes = BTreeSet::new();
        if let Some(home) = std::env::var_os("HOME").filter(|value| !value.is_empty()) {
            homes.insert(PathBuf::from(home));
        }
        if let Ok(passwd) = fs::read_to_string("/etc/passwd") {
            for line in passwd.lines() {
                let fields = line.split(':').collect::<Vec<_>>();
                if let Some(home) = fields.get(5).filter(|home| home.starts_with('/')) {
                    homes.insert(PathBuf::from(home));
                }
            }
        }
        homes
    }

    fn same_linux_namespaces(pid: u32) -> bool {
        for namespace in ["pid", "mnt"] {
            let Ok(current) = fs::read_link(format!("/proc/self/ns/{namespace}")) else {
                return true;
            };
            let Ok(candidate) = fs::read_link(format!("/proc/{pid}/ns/{namespace}")) else {
                return false;
            };
            if current != candidate {
                return false;
            }
        }
        true
    }
}

#[cfg(windows)]
fn platform_conflicts() -> BTreeSet<String> {
    use std::path::PathBuf;
    use std::process::{Command, Stdio};

    use std::os::windows::process::CommandExt;
    use sysinfo::{ProcessesToUpdate, System};

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    let mut conflicts = BTreeSet::new();
    let mut install_roots = BTreeSet::new();
    for variable in ["ProgramFiles", "ProgramW6432"] {
        if let Some(root) = std::env::var_os(variable).filter(|value| !value.is_empty()) {
            install_roots.insert(PathBuf::from(root));
        }
    }
    if install_roots.is_empty() {
        install_roots.insert(PathBuf::from(r"C:\Program Files"));
    }
    for root in install_roots {
        let binary = root.join("cf-probe").join("cf-probe.exe");
        if binary.exists() {
            conflicts.insert(format!("Go cf-probe executable: {}", binary.display()));
        }
    }

    for (task, kind) in [
        ("cf-probe", "Go cf-probe scheduled task"),
        ("CFProbe", "legacy PowerShell CFProbe scheduled task"),
    ] {
        let present = Command::new("schtasks.exe")
            .args(["/Query", "/TN", task])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .creation_flags(CREATE_NO_WINDOW)
            .status()
            .is_ok_and(|status| status.success());
        if present {
            conflicts.insert(format!("{kind}: {task}"));
        }
    }

    let service_present = Command::new("sc.exe")
        .args(["query", "cf-probe"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .creation_flags(CREATE_NO_WINDOW)
        .status()
        .is_ok_and(|status| status.success());
    if service_present {
        conflicts.insert("Go cf-probe Windows service: cf-probe".to_string());
    }

    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::All, true);
    for (pid, process) in system.processes() {
        let name = process.name().to_string_lossy();
        let command_line = process
            .cmd()
            .iter()
            .map(|part| part.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ");
        if let Some(kind) = process_kind(&name, &command_line) {
            conflicts.insert(format!("{kind}: PID {pid}"));
        }
    }
    conflicts
}

#[cfg(not(any(target_os = "linux", windows)))]
fn platform_conflicts() -> BTreeSet<String> {
    BTreeSet::new()
}

#[cfg(test)]
mod tests {
    use super::process_kind;

    #[test]
    fn recognizes_official_cf_agent_processes_without_matching_installers() {
        assert_eq!(
            process_kind(
                "cf-probe.exe",
                r#"C:\Program Files\cf-probe\cf-probe.exe run"#
            ),
            Some("running Go cf-probe process")
        );
        assert_eq!(
            process_kind("CF-PROBE.EXE", "CF-PROBE.EXE run"),
            Some("running Go cf-probe process")
        );
        assert_eq!(
            process_kind("bash", "/bin/bash /usr/local/bin/cf-probe.sh -debug=0"),
            Some("running legacy Shell cf-probe process")
        );
        assert_eq!(
            process_kind(
                "powershell.exe",
                r#"powershell.exe -File C:\Agent\cf-server-monitor.ps1 run -STA"#,
            ),
            Some("running legacy PowerShell CFProbe process")
        );
        assert_eq!(
            process_kind(
                "powershell.exe",
                r#"powershell.exe -File C:\Agent\cf-server-monitor.ps1 install"#,
            ),
            None
        );
        assert_eq!(process_kind("probe-rs.exe", "probe-rs.exe"), None);
    }
}
