use std::time::Duration;

use anyhow::{bail, Context, Result};
use semver::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::config::{validate_update_repository, AutoUpdateConfig, LocalConfig, UpdateChannel};

/// Release 工作流将当前 `${{ github.repository }}` 写入构建环境。源码构建未提供
/// 该值时不猜测上游仓库；只有本地配置显式指定来源后才允许自动更新。
const EMBEDDED_UPDATE_REPOSITORY: Option<&str> = option_env!("PROBE_RS_UPDATE_REPOSITORY");
const CHECKSUM_ASSET: &str = "SHA256SUMS";
const MAX_BINARY_BYTES: usize = 100 * 1024 * 1024;
const MAX_CHECKSUM_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
}

#[derive(Debug, Clone, Deserialize)]
struct GithubRelease {
    tag_name: String,
    #[serde(default)]
    draft: bool,
    #[serde(default)]
    prerelease: bool,
    #[serde(default)]
    assets: Vec<GithubAsset>,
}

#[derive(Debug)]
struct Candidate {
    version: Version,
    tag: String,
    binary_url: String,
    checksum_url: String,
}

enum CheckOutcome {
    Unchanged,
    Restart,
    #[cfg(windows)]
    InstalledPendingRestart,
}

pub fn spawn(
    mut config_rx: watch::Receiver<LocalConfig>,
    mut check_trigger_rx: watch::Receiver<u64>,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut settings = config_rx.borrow().auto_update.clone();
        let mut check_now = settings.enabled;

        loop {
            if *shutdown_rx.borrow() {
                return;
            }

            if !settings.enabled {
                tokio::select! {
                    changed = config_rx.changed() => {
                        if changed.is_err() { return; }
                        let desired = config_rx.borrow().auto_update.clone();
                        check_now = desired.enabled && desired != settings;
                        settings = desired;
                    }
                    changed = check_trigger_rx.changed() => {
                        if changed.is_err() { return; }
                        tracing::debug!("update trigger ignored because automatic updates are disabled");
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() { return; }
                    }
                }
                continue;
            }

            if check_now {
                tracing::info!(
                    current = env!("CARGO_PKG_VERSION"),
                    channel = %settings.channel,
                    repository = effective_update_repository(&settings).unwrap_or("<unconfigured>"),
                    "checking for updates"
                );
                match check_and_apply(&settings).await {
                    Ok(CheckOutcome::Unchanged) => {
                        tracing::debug!(
                            current = env!("CARGO_PKG_VERSION"),
                            channel = %settings.channel,
                            "no newer release available"
                        );
                    }
                    Ok(CheckOutcome::Restart) => {
                        shutdown_tx.send_replace(true);
                        return;
                    }
                    #[cfg(windows)]
                    Ok(CheckOutcome::InstalledPendingRestart) => {
                        tracing::warn!(
                            "update installed but automatic restart could not be scheduled; the new version will run after the next service restart"
                        );
                        return;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "automatic update check failed");
                    }
                }
                check_now = false;
            }

            let delay = tokio::time::sleep(Duration::from_secs(settings.check_interval));
            tokio::pin!(delay);
            tokio::select! {
                _ = &mut delay => check_now = true,
                changed = config_rx.changed() => {
                    if changed.is_err() { return; }
                    let desired = config_rx.borrow().auto_update.clone();
                    if desired != settings {
                        check_now = desired.enabled;
                        settings = desired;
                        tracing::info!(
                            enabled = settings.enabled,
                            channel = %settings.channel,
                            interval = settings.check_interval,
                            "automatic update settings changed"
                        );
                    }
                }
                changed = check_trigger_rx.changed() => {
                    if changed.is_err() { return; }
                    check_now = true;
                    tracing::info!("automatic update check requested by CF config change");
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() { return; }
                }
            }
        }
    })
}

async fn check_and_apply(settings: &AutoUpdateConfig) -> Result<CheckOutcome> {
    let repository = effective_update_repository(settings)?;
    let asset_name = platform_asset_name(std::env::consts::OS, std::env::consts::ARCH)
        .context("automatic updates are not published for this platform")?;
    let current = Version::parse(env!("CARGO_PKG_VERSION"))
        .context("the compiled package version is not valid SemVer")?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent(concat!("probe-rs/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("failed to create update HTTP client")?;
    let releases = fetch_releases(&client, repository, settings.channel).await?;
    let Some(candidate) = select_candidate(&releases, &current, settings.channel, asset_name)
    else {
        return Ok(CheckOutcome::Unchanged);
    };

    validate_release_download_url(&candidate.binary_url, repository, &candidate.tag)?;
    validate_release_download_url(&candidate.checksum_url, repository, &candidate.tag)?;
    tracing::info!(
        current = %current,
        available = %candidate.version,
        channel = %settings.channel,
        repository,
        asset = asset_name,
        "new release available"
    );

    let checksum_data = download_with_proxys(
        &client,
        &candidate.checksum_url,
        &settings.proxys,
        MAX_CHECKSUM_BYTES,
    )
    .await
    .context("failed to download SHA256SUMS")?;
    let expected = checksum_for_asset(&checksum_data, asset_name)?;
    let binary = download_with_proxys(
        &client,
        &candidate.binary_url,
        &settings.proxys,
        MAX_BINARY_BYTES,
    )
    .await
    .with_context(|| format!("failed to download {asset_name}"))?;
    verify_sha256(&binary, &expected, asset_name)?;

    let suffix = if asset_name.ends_with(".exe") {
        ".exe"
    } else {
        ""
    };
    let staging = tokio::task::spawn_blocking(move || -> Result<tempfile::NamedTempFile> {
        use std::io::Write;

        let mut file = tempfile::Builder::new()
            .prefix("probe-rs-update-")
            .suffix(suffix)
            .tempfile()
            .context("failed to create private update staging file")?;
        file.write_all(&binary)
            .context("failed to write update staging file")?;
        file.as_file()
            .sync_all()
            .context("failed to flush update staging file")?;
        Ok(file)
    })
    .await
    .context("update staging task panicked")??;

    let outcome = install_staged_update(&staging).await?;

    tracing::info!(
        previous = %current,
        installed = %candidate.version,
        "automatic update installed"
    );

    Ok(outcome)
}

/// 把已校验的暂存二进制替换为运行中的可执行文件，并安排重启。
/// 每平台一个实现，stage/replace/restart 的顺序不变量集中在这里。
#[cfg(windows)]
async fn install_staged_update(staging: &tempfile::NamedTempFile) -> Result<CheckOutcome> {
    let replace_source = staging.path().to_owned();
    let previous_executable =
        tokio::task::spawn_blocking(move || replace_windows_executable(&replace_source))
            .await
            .context("update replacement task panicked")?
            .context("failed to replace the running executable")?;

    if let Err(error) = refresh_windows_tray_companion(staging.path()) {
        tracing::warn!(%error, "failed to refresh Windows tray companion");
    }

    if let Err(error) = spawn_windows_restart_helper(&previous_executable) {
        tracing::error!(%error, "failed to start Windows update restart helper");
        return Ok(CheckOutcome::InstalledPendingRestart);
    }
    Ok(CheckOutcome::Restart)
}

/// 把已校验的暂存二进制替换为运行中的可执行文件，并安排重启。
/// 每平台一个实现，stage/replace/restart 的顺序不变量集中在这里。
#[cfg(not(windows))]
async fn install_staged_update(staging: &tempfile::NamedTempFile) -> Result<CheckOutcome> {
    let replace_source = staging.path().to_owned();
    tokio::task::spawn_blocking(move || self_replace::self_replace(replace_source))
        .await
        .context("update replacement task panicked")?
        .context("failed to replace the running executable")?;
    Ok(CheckOutcome::Restart)
}

fn effective_update_repository(settings: &AutoUpdateConfig) -> Result<&str> {
    let repository = settings
        .repository
        .as_deref()
        .or(EMBEDDED_UPDATE_REPOSITORY)
        .context(
            "automatic update repository is not configured; set auto_update.repository or use a release build",
        )?;
    validate_update_repository(repository)?;
    Ok(repository)
}

fn releases_api(repository: &str) -> String {
    format!("https://api.github.com/repos/{repository}/releases")
}

async fn fetch_releases(
    client: &reqwest::Client,
    repository: &str,
    channel: UpdateChannel,
) -> Result<Vec<GithubRelease>> {
    let api = releases_api(repository);
    let request = match channel {
        UpdateChannel::Stable => client.get(format!("{api}/latest")),
        UpdateChannel::Prerelease => client.get(format!("{api}?per_page=30")),
    }
    .header("Accept", "application/vnd.github+json")
    .header("X-GitHub-Api-Version", "2022-11-28");
    let response = request
        .send()
        .await
        .context("GitHub Releases request failed")?
        .error_for_status()
        .context("GitHub Releases returned an error")?;
    match channel {
        UpdateChannel::Stable => Ok(vec![response
            .json()
            .await
            .context("invalid GitHub latest release response")?]),
        UpdateChannel::Prerelease => response
            .json()
            .await
            .context("invalid GitHub releases response"),
    }
}

fn select_candidate(
    releases: &[GithubRelease],
    current: &Version,
    channel: UpdateChannel,
    asset_name: &str,
) -> Option<Candidate> {
    releases
        .iter()
        .filter(|release| !release.draft)
        .filter_map(|release| {
            let version = Version::parse(release.tag_name.trim_start_matches('v')).ok()?;
            let allowed = match channel {
                UpdateChannel::Stable => !release.prerelease && version.pre.is_empty(),
                UpdateChannel::Prerelease => true,
            };
            if !allowed || !version.cmp_precedence(current).is_gt() {
                return None;
            }
            let binary_url = release
                .assets
                .iter()
                .find(|asset| asset.name == asset_name)?
                .browser_download_url
                .clone();
            let checksum_url = release
                .assets
                .iter()
                .find(|asset| asset.name == CHECKSUM_ASSET)?
                .browser_download_url
                .clone();
            Some(Candidate {
                version,
                tag: release.tag_name.clone(),
                binary_url,
                checksum_url,
            })
        })
        .max_by(|left, right| left.version.cmp_precedence(&right.version))
}

async fn download_limited(
    client: &reqwest::Client,
    url: &str,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let mut response = client
        .get(url)
        .send()
        .await
        .context("download request failed")?
        .error_for_status()
        .context("download returned an error")?;
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        bail!("download exceeds {max_bytes} bytes");
    }
    let mut bytes = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or_default()
            .min(max_bytes as u64) as usize,
    );
    while let Some(chunk) = response.chunk().await.context("failed to read download")? {
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            bail!("download exceeds {max_bytes} bytes");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Release assets always try the authenticated GitHub URL first. Configured
/// proxy prefixes are fallbacks only, in declaration order.
async fn download_with_proxys(
    client: &reqwest::Client,
    direct_url: &str,
    proxys: &[String],
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let candidates = download_candidates(direct_url, proxys);
    let mut failures = Vec::new();
    for (index, url) in candidates.iter().enumerate() {
        match download_limited(client, url, max_bytes).await {
            Ok(bytes) => {
                if index > 0 {
                    tracing::info!(proxy = %proxys[index - 1], "release asset downloaded through fallback proxy");
                }
                return Ok(bytes);
            }
            Err(error) => {
                tracing::warn!(url, %error, "release asset download attempt failed");
                failures.push(format!("{url}: {error:#}"));
            }
        }
    }
    bail!(
        "all release asset download attempts failed: {}",
        failures.join("; ")
    )
}

fn download_candidates(direct_url: &str, proxys: &[String]) -> Vec<String> {
    std::iter::once(direct_url.to_owned())
        .chain(
            proxys
                .iter()
                .map(|proxy| format!("{}/{}", proxy.trim_end_matches('/'), direct_url)),
        )
        .collect()
}

fn validate_release_download_url(url: &str, repository: &str, tag: &str) -> Result<()> {
    validate_update_repository(repository)?;
    let url = reqwest::Url::parse(url).context("invalid release asset URL")?;
    let expected_prefix = format!("/{repository}/releases/download/{tag}/");
    if url.scheme() != "https"
        || url.host_str() != Some("github.com")
        || !url.path().starts_with(&expected_prefix)
    {
        bail!("release asset URL is outside the expected GitHub release: {url}");
    }
    Ok(())
}

fn checksum_for_asset(data: &[u8], asset_name: &str) -> Result<String> {
    let text = std::str::from_utf8(data).context("SHA256SUMS is not UTF-8")?;
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        let Some(hash) = fields.next() else { continue };
        let Some(name) = fields.next() else { continue };
        if name.trim_start_matches('*') == asset_name {
            if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                bail!("invalid SHA-256 entry for {asset_name}");
            }
            return Ok(hash.to_ascii_lowercase());
        }
    }
    bail!("SHA256SUMS has no entry for {asset_name}")
}

fn verify_sha256(data: &[u8], expected: &str, asset_name: &str) -> Result<()> {
    let actual = format!("{:x}", Sha256::digest(data));
    if actual != expected {
        bail!("SHA-256 mismatch for {asset_name}: expected {expected}, got {actual}");
    }
    Ok(())
}

fn platform_asset_name(os: &str, arch: &str) -> Option<&'static str> {
    match (os, arch) {
        ("linux", "x86_64") => Some("probe-rs-linux-x86_64"),
        ("linux", "aarch64") => Some("probe-rs-linux-aarch64"),
        ("linux", "loongarch64") => Some("probe-rs-linux-loong64"),
        ("windows", "x86_64") => Some("probe-rs-windows-x86_64.exe"),
        _ => None,
    }
}

#[cfg(windows)]
const WINDOWS_UPDATE_HELPER_ARG: &str = "--probe-rs-finish-update";

#[cfg(windows)]
fn replace_windows_executable(new_executable: &std::path::Path) -> Result<std::path::PathBuf> {
    use std::io::{Read, Write};

    let executable = std::env::current_exe()
        .context("failed to locate the running executable")?
        .canonicalize()
        .context("failed to resolve the running executable")?;
    let directory = executable
        .parent()
        .context("running executable has no parent directory")?;

    // Put the complete replacement on the destination volume before moving the
    // canonical executable. This makes disk-space and copy failures harmless.
    let mut source = std::fs::File::open(new_executable)
        .with_context(|| format!("failed to open {}", new_executable.display()))?;
    let mut incoming = tempfile::Builder::new()
        .prefix(".probe-rs-update-")
        .suffix(".exe")
        .tempfile_in(directory)
        .context("failed to create update file beside the running executable")?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = source
            .read(&mut buffer)
            .context("failed to read the staged update")?;
        if read == 0 {
            break;
        }
        incoming
            .write_all(&buffer[..read])
            .context("failed to stage the update beside the running executable")?;
    }
    incoming
        .as_file()
        .sync_all()
        .context("failed to flush the incoming executable")?;

    let incoming_name = incoming
        .path()
        .file_name()
        .and_then(|name| name.to_str())
        .context("incoming executable has an invalid file name")?
        .to_owned();
    let (_, incoming_path) = incoming
        .keep()
        .map_err(|error| error.error)
        .context("failed to preserve the incoming executable")?;
    let previous = directory.join(format!("{incoming_name}.previous.exe"));

    swap_windows_executable(&executable, &incoming_path, &previous)?;
    Ok(previous)
}

#[cfg(windows)]
fn swap_windows_executable(
    executable: &std::path::Path,
    incoming: &std::path::Path,
    previous: &std::path::Path,
) -> Result<()> {
    if let Err(error) = std::fs::rename(executable, previous) {
        let _ = std::fs::remove_file(incoming);
        return Err(error)
            .with_context(|| format!("failed to move {} aside", executable.display()));
    }
    if let Err(install_error) = std::fs::rename(incoming, executable) {
        let restore_result = std::fs::rename(previous, executable);
        let _ = std::fs::remove_file(incoming);
        return match restore_result {
            Ok(()) => Err(install_error).with_context(|| {
                format!(
                    "failed to install the incoming executable as {}; restored the previous one",
                    executable.display()
                )
            }),
            Err(restore_error) => bail!(
                "failed to install the incoming executable as {} ({install_error}); also failed to restore the previous one ({restore_error})",
                executable.display()
            ),
        };
    }
    Ok(())
}

#[cfg(windows)]
fn spawn_windows_restart_helper(previous_executable: &std::path::Path) -> Result<()> {
    use std::os::windows::process::CommandExt;
    use std::process::Command;
    use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW};

    let executable = std::env::current_exe().context("failed to locate updated executable")?;
    let previous_name = previous_executable
        .file_name()
        .context("previous executable has no file name")?;
    let mut command = Command::new(executable);
    command
        .arg(WINDOWS_UPDATE_HELPER_ARG)
        .arg(std::process::id().to_string())
        .arg(previous_name)
        .arg("--")
        .args(std::env::args_os().skip(1))
        .creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    command
        .spawn()
        .context("failed to spawn updated executable helper")?;
    Ok(())
}

#[cfg(windows)]
fn refresh_windows_tray_companion(new_executable: &std::path::Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_DELAY_UNTIL_REBOOT};

    let executable = std::env::current_exe().context("failed to locate agent executable")?;
    let Some(directory) = executable.parent() else {
        bail!("agent executable has no parent directory");
    };
    let tray = directory.join("probe-rs-tray.exe");
    if !tray.is_file() {
        return Ok(());
    }

    let unique = format!(
        "{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_millis()
    );
    let incoming = directory.join(format!(".probe-rs-tray-{unique}.update.exe"));
    let previous = directory.join(format!(".probe-rs-tray-{unique}.previous.exe"));
    std::fs::copy(new_executable, &incoming)
        .with_context(|| format!("failed to stage {}", incoming.display()))?;
    // 与主程序共用同一套 staged-replace（rename-aside / rename-in / 失败还原）
    swap_windows_executable(&tray, &incoming, &previous)?;

    let previous_wide: Vec<u16> = previous
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    if unsafe {
        MoveFileExW(
            previous_wide.as_ptr(),
            std::ptr::null(),
            MOVEFILE_DELAY_UNTIL_REBOOT,
        )
    } == 0
    {
        tracing::debug!(
            path = %previous.display(),
            error = %std::io::Error::last_os_error(),
            "old tray companion will remain until it can be removed manually"
        );
    }
    Ok(())
}

#[cfg(windows)]
pub fn maybe_finish_windows_update() -> Result<bool> {
    use std::os::windows::process::CommandExt;
    use std::process::Command;
    use windows_sys::Win32::Foundation::{CloseHandle, WAIT_OBJECT_0, WAIT_TIMEOUT};
    use windows_sys::Win32::System::Threading::{
        OpenProcess, WaitForSingleObject, CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW,
        PROCESS_SYNCHRONIZE,
    };

    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref() != Some(std::ffi::OsStr::new(WINDOWS_UPDATE_HELPER_ARG)) {
        return Ok(false);
    }
    let parent_id: u32 = args
        .next()
        .and_then(|value| value.into_string().ok())
        .context("Windows update helper is missing the parent process id")?
        .parse()
        .context("invalid Windows update parent process id")?;
    let previous_name = args
        .next()
        .context("Windows update helper is missing the previous executable name")?;
    let previous_executable = windows_previous_executable_path(&previous_name)?;
    let mut original_args: Vec<_> = args.collect();
    if original_args.first().is_some_and(|arg| arg == "--") {
        original_args.remove(0);
    }
    let user_mode = windows_user_mode(&original_args);

    let parent = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, parent_id) };
    if !parent.is_null() {
        let wait = unsafe { WaitForSingleObject(parent, 60_000) };
        unsafe { CloseHandle(parent) };
        if wait == WAIT_TIMEOUT {
            bail!("timed out waiting for the previous probe-rs process to stop");
        }
        if wait != WAIT_OBJECT_0 {
            bail!("failed while waiting for the previous probe-rs process");
        }
    } else {
        // The parent may have exited before OpenProcess, but an access failure
        // must not let this helper mistake that still-running parent for the
        // freshly started task instance.
        for attempt in 0..600 {
            if !windows_probe_process_id_running(parent_id)
                .context("failed to inspect the previous probe-rs process")?
            {
                break;
            }
            if attempt == 599 {
                bail!("timed out waiting for the previous probe-rs process to stop");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    if let Err(error) = std::fs::remove_file(&previous_executable) {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(
                path = %previous_executable.display(),
                %error,
                "failed to remove the previous Windows executable"
            );
        }
    }

    if !user_mode {
        'task_start: for _ in 0..15 {
            match windows_agent_process_running(parent_id) {
                Ok(true) => return Ok(true),
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(%error, "failed to inspect Windows processes while confirming restart");
                    break;
                }
            }
            let status = Command::new("schtasks.exe")
                .args(["/Run", "/TN", "probe-rs"])
                .creation_flags(CREATE_NO_WINDOW)
                .status();
            if status.is_ok_and(|status| status.success()) {
                for _ in 0..5 {
                    std::thread::sleep(Duration::from_millis(200));
                    match windows_agent_process_running(parent_id) {
                        Ok(true) => return Ok(true),
                        Ok(false) => {}
                        Err(error) => {
                            tracing::warn!(%error, "failed to inspect Windows processes while confirming restart");
                            break 'task_start;
                        }
                    }
                }
            } else {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }

    let executable = std::env::current_exe().context("failed to locate updated executable")?;
    let working_directory = executable.parent().map(std::path::PathBuf::from);
    let mut command = Command::new(executable);
    command
        .args(original_args)
        .creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    if let Some(directory) = working_directory {
        command.current_dir(directory);
    }
    command
        .spawn()
        .context("failed to restart updated probe-rs")?;
    Ok(true)
}

#[cfg(windows)]
fn windows_user_mode(args: &[std::ffi::OsString]) -> bool {
    args.iter()
        .any(|arg| arg == std::ffi::OsStr::new("--user-mode"))
}

#[cfg(windows)]
fn windows_previous_executable_path(file_name: &std::ffi::OsStr) -> Result<std::path::PathBuf> {
    let file_name = file_name
        .to_str()
        .context("previous executable name is not valid Unicode")?;
    if !file_name.starts_with(".probe-rs-update-")
        || !file_name.ends_with(".exe.previous.exe")
        || file_name.contains(['\\', '/'])
    {
        bail!("invalid previous executable name");
    }
    let executable = std::env::current_exe().context("failed to locate updated executable")?;
    let directory = executable
        .parent()
        .context("updated executable has no parent directory")?;
    Ok(directory.join(file_name))
}

#[cfg(windows)]
fn windows_agent_process_running(previous_process_id: u32) -> std::io::Result<bool> {
    let current_id = std::process::id();
    // 同名进程不足以证明"本安装已重启"——一台主机可能装有多套 probe-rs。
    // 只把可执行文件位于本安装目录内的进程视为本实例;取不到自身路径时
    // 宁可返回 false(继续走 schtasks 拉起,对已运行的任务无害)。
    let install_directory = std::env::current_exe()
        .ok()
        .and_then(|executable| executable.parent().map(std::path::Path::to_path_buf));
    windows_process_matches(|process_id, process_name| {
        if process_id == current_id
            || process_id == previous_process_id
            || !process_name.eq_ignore_ascii_case("probe-rs.exe")
        {
            return false;
        }
        install_directory
            .as_deref()
            .is_some_and(|directory| process_image_in_directory(process_id, directory))
    })
}

/// 进程可执行文件完整路径是否位于 `directory` 内。查询失败按"不是"处理。
#[cfg(windows)]
fn process_image_in_directory(process_id: u32, directory: &std::path::Path) -> bool {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        OpenProcess, QueryFullProcessImageNameW, PROCESS_NAME_WIN32,
        PROCESS_QUERY_LIMITED_INFORMATION,
    };

    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id);
        if handle.is_null() {
            return false;
        }
        let mut buffer = [0_u16; 1024];
        let mut size = buffer.len() as u32;
        let queried =
            QueryFullProcessImageNameW(handle, PROCESS_NAME_WIN32, buffer.as_mut_ptr(), &mut size);
        CloseHandle(handle);
        if queried == 0 {
            return false;
        }
        let image = String::from_utf16_lossy(&buffer[..size as usize]);
        std::path::Path::new(&image)
            .parent()
            .is_some_and(|parent| parent == directory)
    }
}

#[cfg(windows)]
fn windows_probe_process_id_running(process_id: u32) -> std::io::Result<bool> {
    windows_process_matches(|candidate_id, process_name| {
        candidate_id == process_id && process_name.eq_ignore_ascii_case("probe-rs.exe")
    })
}

#[cfg(windows)]
fn windows_process_matches(mut predicate: impl FnMut(u32, &str) -> bool) -> std::io::Result<bool> {
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Process32FirstW, Process32NextW, PROCESSENTRY32W,
        TH32CS_SNAPPROCESS,
    };

    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(std::io::Error::last_os_error());
    }

    let mut entry = PROCESSENTRY32W {
        dwSize: std::mem::size_of::<PROCESSENTRY32W>() as u32,
        ..Default::default()
    };
    let mut found = false;
    let mut has_entry = unsafe { Process32FirstW(snapshot, &mut entry) } != 0;
    while has_entry {
        let name_len = entry
            .szExeFile
            .iter()
            .position(|character| *character == 0)
            .unwrap_or(entry.szExeFile.len());
        let process_name = String::from_utf16_lossy(&entry.szExeFile[..name_len]);
        if predicate(entry.th32ProcessID, &process_name) {
            found = true;
            break;
        }
        has_entry = unsafe { Process32NextW(snapshot, &mut entry) } != 0;
    }
    unsafe { CloseHandle(snapshot) };
    Ok(found)
}

#[cfg(not(windows))]
#[allow(dead_code)]
pub fn maybe_finish_windows_update() -> Result<bool> {
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_REPOSITORY: &str = "fork-owner/probe-rs";

    fn release(version: &str, prerelease: bool, complete: bool) -> GithubRelease {
        let tag = format!("v{version}");
        let mut assets = vec![GithubAsset {
            name: "probe-rs-linux-x86_64".into(),
            browser_download_url: format!(
                "https://github.com/{TEST_REPOSITORY}/releases/download/{tag}/probe-rs-linux-x86_64"
            ),
        }];
        if complete {
            assets.push(GithubAsset {
                name: CHECKSUM_ASSET.into(),
                browser_download_url: format!(
                    "https://github.com/{TEST_REPOSITORY}/releases/download/{tag}/{CHECKSUM_ASSET}"
                ),
            });
        }
        GithubRelease {
            tag_name: tag,
            draft: false,
            prerelease,
            assets,
        }
    }

    #[test]
    fn stable_channel_ignores_prereleases() {
        let releases = vec![
            release("0.1.3-beta.3", true, true),
            release("0.1.2", false, true),
        ];
        assert!(select_candidate(
            &releases,
            &Version::parse("0.1.3-beta.2").unwrap(),
            UpdateChannel::Stable,
            "probe-rs-linux-x86_64"
        )
        .is_none());
    }

    #[test]
    fn prerelease_channel_selects_highest_strictly_newer_version() {
        let releases = vec![
            release("0.1.3-beta.3", true, true),
            release("0.1.3-beta.2", true, true),
            release("0.1.2", false, true),
        ];
        let selected = select_candidate(
            &releases,
            &Version::parse("0.1.3-beta.2").unwrap(),
            UpdateChannel::Prerelease,
            "probe-rs-linux-x86_64",
        )
        .unwrap();
        assert_eq!(selected.version, Version::parse("0.1.3-beta.3").unwrap());
    }

    #[test]
    fn prerelease_channel_promotes_to_newer_stable_release() {
        let releases = vec![
            release("0.1.3-beta.9", true, true),
            release("0.1.3", false, true),
        ];
        let selected = select_candidate(
            &releases,
            &Version::parse("0.1.3-beta.2").unwrap(),
            UpdateChannel::Prerelease,
            "probe-rs-linux-x86_64",
        )
        .unwrap();
        assert_eq!(selected.version, Version::parse("0.1.3").unwrap());
    }

    #[test]
    fn incomplete_release_is_not_selected() {
        let releases = vec![release("0.1.4", false, false)];
        assert!(select_candidate(
            &releases,
            &Version::parse("0.1.3").unwrap(),
            UpdateChannel::Stable,
            "probe-rs-linux-x86_64"
        )
        .is_none());
    }

    #[test]
    fn build_metadata_alone_does_not_trigger_an_update() {
        let releases = vec![release("0.1.3+build.2", false, true)];
        assert!(select_candidate(
            &releases,
            &Version::parse("0.1.3+build.1").unwrap(),
            UpdateChannel::Stable,
            "probe-rs-linux-x86_64"
        )
        .is_none());
    }

    #[test]
    fn parses_and_verifies_checksum() {
        let data = b"new probe binary";
        let hash = format!("{:x}", Sha256::digest(data));
        let sums = format!("{hash}  probe-rs-linux-x86_64\n");
        let parsed = checksum_for_asset(sums.as_bytes(), "probe-rs-linux-x86_64").unwrap();
        verify_sha256(data, &parsed, "probe-rs-linux-x86_64").unwrap();
        assert!(verify_sha256(b"tampered", &parsed, "probe-rs-linux-x86_64").is_err());
    }

    #[test]
    fn configured_repository_overrides_the_embedded_source() {
        let settings = AutoUpdateConfig {
            repository: Some(TEST_REPOSITORY.into()),
            ..AutoUpdateConfig::default()
        };
        assert_eq!(
            effective_update_repository(&settings).unwrap(),
            TEST_REPOSITORY
        );
        assert_eq!(
            releases_api(TEST_REPOSITORY),
            "https://api.github.com/repos/fork-owner/probe-rs/releases"
        );
    }

    #[test]
    fn missing_source_never_falls_back_to_the_official_repository() {
        let settings = AutoUpdateConfig::default();
        match EMBEDDED_UPDATE_REPOSITORY {
            Some(repository) => {
                assert_eq!(effective_update_repository(&settings).unwrap(), repository)
            }
            None => assert!(effective_update_repository(&settings).is_err()),
        }
    }

    #[test]
    fn release_asset_must_belong_to_the_selected_repository() {
        let tag = "v1.0.0";
        let expected = format!(
            "https://github.com/{TEST_REPOSITORY}/releases/download/{tag}/probe-rs-linux-x86_64"
        );
        validate_release_download_url(&expected, TEST_REPOSITORY, tag).unwrap();
        assert!(validate_release_download_url(
            "https://github.com/ukuq/probe-rs/releases/download/v1.0.0/probe-rs-linux-x86_64",
            TEST_REPOSITORY,
            tag,
        )
        .is_err());
    }

    #[test]
    fn release_downloads_try_direct_then_configured_proxys_in_order() {
        let direct = "https://github.com/ukuq/probe-rs/releases/download/v1.0.0/asset";
        assert_eq!(
            download_candidates(
                direct,
                &[
                    "https://proxy-a.example/".into(),
                    "https://proxy-b.example/prefix".into(),
                ],
            ),
            vec![
                direct.to_owned(),
                format!("https://proxy-a.example/{direct}"),
                format!("https://proxy-b.example/prefix/{direct}"),
            ]
        );
    }

    #[test]
    fn maps_only_published_platform_assets() {
        assert_eq!(
            platform_asset_name("linux", "aarch64"),
            Some("probe-rs-linux-aarch64")
        );
        assert_eq!(
            platform_asset_name("linux", "loongarch64"),
            Some("probe-rs-linux-loong64")
        );
        assert_eq!(
            platform_asset_name("windows", "x86_64"),
            Some("probe-rs-windows-x86_64.exe")
        );
        assert_eq!(platform_asset_name("macos", "aarch64"), None);
    }

    #[cfg(windows)]
    #[test]
    fn windows_swap_restores_the_previous_executable_when_install_fails() {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("probe-rs.exe");
        let missing_incoming = directory.path().join("missing-update.exe");
        let previous = directory.path().join("previous.exe");
        std::fs::write(&executable, b"previous binary").unwrap();

        assert!(swap_windows_executable(&executable, &missing_incoming, &previous).is_err());
        assert_eq!(std::fs::read(&executable).unwrap(), b"previous binary");
        assert!(!previous.exists());
    }

    #[cfg(windows)]
    #[test]
    fn windows_update_helper_accepts_only_its_own_backup_names() {
        let valid = std::ffi::OsStr::new(".probe-rs-update-abc.exe.previous.exe");
        assert_eq!(
            windows_previous_executable_path(valid).unwrap().file_name(),
            Some(valid)
        );
        assert!(windows_previous_executable_path(std::ffi::OsStr::new(
            ".probe-rs-update-abc.exe.previous.exe\\..\\victim"
        ))
        .is_err());
    }

    #[cfg(windows)]
    #[test]
    fn windows_update_detects_user_mode_restart() {
        assert!(windows_user_mode(&[
            "--user-mode".into(),
            "--config".into(),
            "user.toml".into(),
        ]));
        assert!(!windows_user_mode(&[
            "--config".into(),
            "machine.toml".into(),
        ]));
    }
}
