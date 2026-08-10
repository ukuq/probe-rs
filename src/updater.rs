use std::time::Duration;

use anyhow::{bail, Context, Result};
use semver::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::config::{AutoUpdateConfig, LocalConfig, UpdateChannel};

const RELEASES_API: &str = "https://api.github.com/repos/ukuq/probe-rs/releases";
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
    InstalledPendingRestart,
}

pub fn spawn(
    mut config_rx: watch::Receiver<LocalConfig>,
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
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() { return; }
                }
            }
        }
    })
}

async fn check_and_apply(settings: &AutoUpdateConfig) -> Result<CheckOutcome> {
    let asset_name = platform_asset_name(std::env::consts::OS, std::env::consts::ARCH)
        .context("automatic updates are not published for this platform")?;
    let current = Version::parse(env!("CARGO_PKG_VERSION"))
        .context("the compiled package version is not valid SemVer")?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent(concat!("probe-rs/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("failed to create update HTTP client")?;
    let releases = fetch_releases(&client, settings.channel).await?;
    let Some(candidate) = select_candidate(&releases, &current, settings.channel, asset_name)
    else {
        return Ok(CheckOutcome::Unchanged);
    };

    validate_release_download_url(&candidate.binary_url, &candidate.tag)?;
    validate_release_download_url(&candidate.checksum_url, &candidate.tag)?;
    tracing::info!(
        current = %current,
        available = %candidate.version,
        channel = %settings.channel,
        asset = asset_name,
        "new release available"
    );

    let checksum_data = download_limited(&client, &candidate.checksum_url, MAX_CHECKSUM_BYTES)
        .await
        .context("failed to download SHA256SUMS")?;
    let expected = checksum_for_asset(&checksum_data, asset_name)?;
    let binary = download_limited(&client, &candidate.binary_url, MAX_BINARY_BYTES)
        .await
        .with_context(|| format!("failed to download {asset_name}"))?;
    verify_sha256(&binary, &expected, asset_name)?;

    let suffix = asset_name.ends_with(".exe").then_some(".exe").unwrap_or("");
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

    let replace_source = staging.path().to_owned();
    let replaced = tokio::task::spawn_blocking(move || self_replace::self_replace(replace_source))
        .await
        .context("update replacement task panicked")?;
    if let Err(error) = replaced {
        return Err(error).context("failed to replace the running executable");
    }

    #[cfg(windows)]
    if let Err(error) = refresh_windows_tray_companion(staging.path()) {
        tracing::warn!(%error, "failed to refresh Windows tray companion");
    }

    tracing::info!(
        previous = %current,
        installed = %candidate.version,
        "automatic update installed"
    );

    #[cfg(windows)]
    {
        if let Err(error) = spawn_windows_restart_helper() {
            tracing::error!(%error, "failed to start Windows update restart helper");
            return Ok(CheckOutcome::InstalledPendingRestart);
        }
    }

    Ok(CheckOutcome::Restart)
}

async fn fetch_releases(
    client: &reqwest::Client,
    channel: UpdateChannel,
) -> Result<Vec<GithubRelease>> {
    let request = match channel {
        UpdateChannel::Stable => client.get(format!("{RELEASES_API}/latest")),
        UpdateChannel::Prerelease => client.get(format!("{RELEASES_API}?per_page=30")),
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

fn validate_release_download_url(url: &str, tag: &str) -> Result<()> {
    let url = reqwest::Url::parse(url).context("invalid release asset URL")?;
    let expected_prefix = format!("/ukuq/probe-rs/releases/download/{tag}/");
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
        ("windows", "x86_64") => Some("probe-rs-windows-x86_64.exe"),
        _ => None,
    }
}

#[cfg(windows)]
const WINDOWS_UPDATE_HELPER_ARG: &str = "--probe-rs-finish-update";

#[cfg(windows)]
fn spawn_windows_restart_helper() -> Result<()> {
    use std::os::windows::process::CommandExt;
    use std::process::Command;
    use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW};

    let executable = std::env::current_exe().context("failed to locate updated executable")?;
    let mut command = Command::new(executable);
    command
        .arg(WINDOWS_UPDATE_HELPER_ARG)
        .arg(std::process::id().to_string())
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
    if let Err(error) = std::fs::rename(&tray, &previous) {
        let _ = std::fs::remove_file(&incoming);
        return Err(error).context("failed to move the running tray companion aside");
    }
    if let Err(error) = std::fs::rename(&incoming, &tray) {
        let _ = std::fs::rename(&previous, &tray);
        let _ = std::fs::remove_file(&incoming);
        return Err(error).context("failed to install the new tray companion");
    }

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
    let mut original_args: Vec<_> = args.collect();
    if original_args.first().is_some_and(|arg| arg == "--") {
        original_args.remove(0);
    }

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
    }

    for _ in 0..15 {
        let status = Command::new("schtasks.exe")
            .args(["/Run", "/TN", "probe-rs"])
            .creation_flags(CREATE_NO_WINDOW)
            .status();
        if status.is_ok_and(|status| status.success()) {
            return Ok(true);
        }
        std::thread::sleep(Duration::from_secs(1));
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

#[cfg(not(windows))]
#[allow(dead_code)]
pub fn maybe_finish_windows_update() -> Result<bool> {
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn release(version: &str, prerelease: bool, complete: bool) -> GithubRelease {
        let tag = format!("v{version}");
        let mut assets = vec![GithubAsset {
            name: "probe-rs-linux-x86_64".into(),
            browser_download_url: format!(
                "https://github.com/ukuq/probe-rs/releases/download/{tag}/probe-rs-linux-x86_64"
            ),
        }];
        if complete {
            assets.push(GithubAsset {
                name: CHECKSUM_ASSET.into(),
                browser_download_url: format!(
                    "https://github.com/ukuq/probe-rs/releases/download/{tag}/{CHECKSUM_ASSET}"
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
    fn maps_only_published_platform_assets() {
        assert_eq!(
            platform_asset_name("linux", "aarch64"),
            Some("probe-rs-linux-aarch64")
        );
        assert_eq!(
            platform_asset_name("windows", "x86_64"),
            Some("probe-rs-windows-x86_64.exe")
        );
        assert_eq!(platform_asset_name("macos", "aarch64"), None);
    }
}
