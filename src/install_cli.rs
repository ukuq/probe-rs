//! Internal, non-service commands used by the platform installers.
//!
//! Keeping TOML-aware upsert and traffic-ledger correction here avoids having
//! two subtly different shell/PowerShell parsers. These commands are not a
//! remote execution surface: they only operate on explicit local paths.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{Local, TimeZone};

use crate::collector::net::IfaceFilter;
use crate::config::{self, AutoUpdateConfig, LocalConfig, UpdateChannel, CONFIG_SCHEMA};
use crate::model::{CfSection, ReporterConfig, ReporterProtocol};
use crate::netstatic::{self, NetStatic};

const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

pub async fn run_if_requested() -> Result<bool> {
    let mut args = std::env::args().skip(1);
    let Some(command) = args.next() else {
        return Ok(false);
    };
    match command.as_str() {
        "configure-cf" => {
            let selected = configure_cf(parse_configure_args(args)?)?;
            println!("{selected}");
            Ok(true)
        }
        "set-traffic-correction" => {
            set_traffic_correction(parse_correction_args(args)?).await?;
            Ok(true)
        }
        // 未知子命令必须报错:静默回退为启动 agent 会把拼错的管理命令变成
        // 一次意外的前台探针启动。
        other if !other.starts_with('-') => {
            bail!("unknown command: {other} (supported: configure-cf, set-traffic-correction)")
        }
        _ => Ok(false),
    }
}

#[derive(Debug)]
struct ConfigureCfOptions {
    config_path: PathBuf,
    default_net_static_path: String,
    server_id: String,
    secret: String,
    worker_url: String,
    reporter_id: Option<String>,
    collect: Option<u64>,
    report_interval: Option<u64>,
    /// 已废弃:连接固定为 auto。仅为兼容旧安装脚本保留解析。
    connection_mode: Option<String>,
    reset_day: Option<u8>,
    interfaces: Option<Vec<String>>,
    pings: Vec<(String, String)>,
    auto_update: Option<bool>,
    update_channel: Option<UpdateChannel>,
    replace_cf: bool,
}

fn parse_configure_args(args: impl Iterator<Item = String>) -> Result<ConfigureCfOptions> {
    let mut config_path = None;
    let mut default_net_static_path = None;
    let mut server_id = None;
    let mut secret = None;
    let mut worker_url = None;
    let mut reporter_id = None;
    let mut collect = None;
    let mut report_interval = None;
    let mut connection_mode = None;
    let mut reset_day = None;
    let mut interfaces = None;
    let mut pings = Vec::new();
    let mut auto_update = None;
    let mut update_channel = None;
    let mut replace_cf = false;
    let mut args = args;
    while let Some(arg) = args.next() {
        let value = |args: &mut dyn Iterator<Item = String>| {
            args.next()
                .with_context(|| format!("{arg} requires a value"))
        };
        match arg.as_str() {
            "--config" => config_path = Some(PathBuf::from(value(&mut args)?)),
            "--net-static-path" => default_net_static_path = Some(value(&mut args)?),
            "--server-id" => server_id = Some(value(&mut args)?),
            "--secret" => secret = Some(value(&mut args)?),
            "--url" => worker_url = Some(value(&mut args)?),
            "--reporter-id" => reporter_id = Some(value(&mut args)?),
            "--collect" => collect = Some(parse_positive_u64(&arg, &value(&mut args)?)?),
            "--report-interval" => {
                report_interval = Some(parse_positive_u64(&arg, &value(&mut args)?)?)
            }
            "--connection-mode" => {
                connection_mode = Some(value(&mut args)?.to_ascii_lowercase());
            }
            "--reset-day" => {
                let parsed = value(&mut args)?
                    .parse::<u8>()
                    .context("invalid --reset-day")?;
                if parsed > 31 {
                    bail!("--reset-day must be between 0 and 31");
                }
                reset_day = Some(parsed);
            }
            "--interfaces" => {
                interfaces = Some(
                    value(&mut args)?
                        .split(',')
                        .map(str::trim)
                        .filter(|item| !item.is_empty())
                        .map(ToOwned::to_owned)
                        .collect(),
                )
            }
            "--ct" | "--cu" | "--cm" | "--bd" => {
                pings.push((arg.trim_start_matches("--").to_owned(), value(&mut args)?));
            }
            "--auto-update" => auto_update = Some(parse_bool(&arg, &value(&mut args)?)?),
            "--update-channel" => {
                // 大小写归一,与 PowerShell ValidateSet 的大小写不敏感一致。
                update_channel = Some(match value(&mut args)?.to_ascii_lowercase().as_str() {
                    "stable" => UpdateChannel::Stable,
                    "prerelease" => UpdateChannel::Prerelease,
                    _ => bail!("--update-channel must be stable or prerelease"),
                });
            }
            "--replace-cf" => replace_cf = true,
            _ => bail!("unknown configure-cf option: {arg}"),
        }
    }
    let reporter_id = reporter_id
        .map(|id| validate_reporter_id(&id).map(|_| id))
        .transpose()?;
    Ok(ConfigureCfOptions {
        config_path: config_path.context("configure-cf requires --config")?,
        default_net_static_path: default_net_static_path
            .context("configure-cf requires --net-static-path")?,
        server_id: required_nonempty(server_id, "--server-id")?,
        secret: required_nonempty(secret, "--secret")?,
        worker_url: required_nonempty(worker_url, "--url")?,
        reporter_id,
        collect,
        report_interval,
        connection_mode,
        reset_day,
        interfaces,
        pings,
        auto_update,
        update_channel,
        replace_cf,
    })
}

fn configure_cf(options: ConfigureCfOptions) -> Result<String> {
    if let Some(mode) = &options.connection_mode {
        if mode != "auto" {
            tracing::warn!(
                mode,
                "--connection-mode 已废弃,连接固定为 auto(WSS+POST 回退)"
            );
        }
    }
    let mut config = load_install_config(
        &options.config_path,
        options.default_net_static_path.clone(),
    )?;
    config
        .reporters
        .retain(|reporter| !is_seeded_sample(reporter));

    let cf_ids: Vec<_> = config
        .reporters
        .iter()
        .filter(|reporter| reporter.cf.is_some())
        .map(|reporter| reporter.id.clone())
        .collect();
    let selected_id = match options.reporter_id {
        Some(id) => id,
        None if options.replace_cf || cf_ids.is_empty() => "cf".to_owned(),
        None if cf_ids.len() == 1 => cf_ids[0].clone(),
        None => bail!(
            "multiple CF Reporters exist ({}); pass -reporter_id=<id>",
            cf_ids.join(", ")
        ),
    };

    if config
        .reporters
        .iter()
        .any(|reporter| reporter.id == selected_id && reporter.cf.is_none())
    {
        bail!("Reporter id '{selected_id}' already belongs to a non-CF Reporter");
    }
    if options.replace_cf {
        config
            .reporters
            .retain(|reporter| reporter.cf.is_none() || reporter.id == selected_id);
    }

    let index = config
        .reporters
        .iter()
        .position(|reporter| reporter.id == selected_id);
    if index.is_none() {
        config.reporters.push(new_cf_reporter(selected_id.clone()));
    }
    let reporter = config
        .reporters
        .iter_mut()
        .find(|reporter| reporter.id == selected_id)
        .expect("CF Reporter was inserted");
    let cf = reporter.cf.as_mut().expect("CF section was inserted");
    cf.server_id = options.server_id;
    cf.secret = options.secret;
    cf.url = options.worker_url;
    cf.ext.config_version.clear();
    if let Some(value) = options.collect {
        cf.collect_interval = value;
    }
    if let Some(value) = options.report_interval {
        cf.interval = value;
    }
    if let Some(value) = options.reset_day {
        cf.reset_day = value;
    }
    if let Some(value) = options.interfaces {
        cf.interface = value.join(",");
    }
    for (name, target) in options.pings {
        let slot = match name.as_str() {
            "ct" => &mut cf.ct,
            "cu" => &mut cf.cu,
            "cm" => &mut cf.cm,
            "bd" => &mut cf.bd,
            _ => bail!("unknown CF ping slot: {name}"),
        };
        *slot = (!target.trim().is_empty()).then_some(target);
    }
    if let Some(value) = options.auto_update {
        config.auto_update.enabled = value;
    }
    if let Some(value) = options.update_channel {
        config.auto_update.channel = value;
    }

    config
        .validate()
        .context("generated CF config is invalid")?;
    config::persist(&options.config_path, &config).context("failed to persist CF config")?;
    Ok(selected_id)
}

fn new_cf_reporter(id: String) -> ReporterConfig {
    ReporterConfig {
        id,
        cf: Some(CfSection {
            server_id: String::new(),
            secret: String::new(),
            url: String::new(),
            interval: 60,
            collect_interval: 1,
            reset_day: 1,
            interface: String::new(),
            ct: None,
            cu: None,
            cm: None,
            bd: None,
            ext: Default::default(),
        }),
        komari: None,
        probe: None,
    }
}

fn load_install_config(path: &Path, net_static_path: String) -> Result<LocalConfig> {
    let default_data_dir = || {
        Path::new(&net_static_path)
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(|parent| parent.to_string_lossy().into_owned())
            .unwrap_or_else(config::default_data_dir)
    };
    if !path.is_file() {
        return Ok(LocalConfig {
            schema: CONFIG_SCHEMA,
            data_dir: default_data_dir(),
            auto_update: AutoUpdateConfig::default(),
            reporters: Vec::new(),
        });
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read existing config: {}", path.display()))?;
    let document: toml::Value = toml::from_str(&raw).context("existing config is invalid TOML")?;
    if document.get("reporters").is_none() {
        // 只补空 reporters,保留文件里可保留的字段(data_dir/auto_update;
        // 兼容旧版 net_static_path),不能整体丢弃用户已有配置。
        let legacy_data_dir = document
            .get("net_static_path")
            .and_then(|value| value.as_str())
            .and_then(|value| {
                Path::new(value)
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .map(|parent| parent.to_string_lossy().into_owned())
            });
        let preserved = LocalConfig {
            schema: CONFIG_SCHEMA,
            data_dir: document
                .get("data_dir")
                .and_then(|value| value.as_str())
                .map(str::to_string)
                .or(legacy_data_dir)
                .unwrap_or_else(default_data_dir),
            auto_update: document
                .get("auto_update")
                .cloned()
                .and_then(|value| value.try_into().ok())
                .unwrap_or_default(),
            reporters: Vec::new(),
        };
        tracing::warn!(
            path = %path.display(),
            "existing config has no [[reporters]]; reporters will be added by configure-cf, other fields preserved"
        );
        return Ok(preserved);
    }
    // 旧版配置走 schema 迁移(同时回写),新版直接解析。
    config::load(path).context("existing canonical config is invalid")
}

/// 只删除"完整示例指纹"的 Reporter:示例连接参数的精确配对命中才算,
/// 避免用户真实使用某示例 URL(如本地 demo)时被误删。
fn is_seeded_sample(reporter: &ReporterConfig) -> bool {
    if let Some(cf) = &reporter.cf {
        return matches!(
            (cf.server_id.as_str(), cf.url.as_str()),
            ("cf-server-uuid", "https://monitor.example.com/update")
                | ("srv-01", "https://monitor.example.com/report")
        );
    }
    if let Some(komari) = &reporter.komari {
        return komari.endpoint == "https://komari.example.com";
    }
    if let Some(probe) = &reporter.probe {
        return matches!(
            (probe.server_id.as_str(), probe.worker_url.as_str()),
            ("my-host", "http://127.0.0.1:8080/report")
        );
    }
    false
}

#[derive(Debug)]
struct CorrectionOptions {
    config_path: PathBuf,
    reporter_id: String,
    rx_gib: Option<f64>,
    tx_gib: Option<f64>,
}

fn parse_correction_args(args: impl Iterator<Item = String>) -> Result<CorrectionOptions> {
    let mut config_path = None;
    let mut reporter_id = None;
    let mut rx_gib = None;
    let mut tx_gib = None;
    let mut args = args;
    while let Some(arg) = args.next() {
        let value = args
            .next()
            .with_context(|| format!("{arg} requires a value"))?;
        match arg.as_str() {
            "--config" => config_path = Some(PathBuf::from(value)),
            "--reporter-id" => reporter_id = Some(value),
            "--rx-gib" => rx_gib = Some(parse_gib(&arg, &value)?),
            "--tx-gib" => tx_gib = Some(parse_gib(&arg, &value)?),
            _ => bail!("unknown set-traffic-correction option: {arg}"),
        }
    }
    if rx_gib.is_none() && tx_gib.is_none() {
        bail!("set-traffic-correction requires --rx-gib and/or --tx-gib");
    }
    let reporter_id = required_nonempty(reporter_id, "--reporter-id")?;
    validate_reporter_id(&reporter_id)?;
    Ok(CorrectionOptions {
        config_path: config_path.context("set-traffic-correction requires --config")?,
        reporter_id,
        rx_gib,
        tx_gib,
    })
}

async fn set_traffic_correction(options: CorrectionOptions) -> Result<()> {
    let clock = crate::reporter::AgentClock::default();
    clock.refresh_ntp().await;
    set_traffic_correction_with_clock(options, clock.report_time().accurate_ts)
}

fn set_traffic_correction_with_clock(
    options: CorrectionOptions,
    calibrated_now: Option<i64>,
) -> Result<()> {
    let config = config::load(&options.config_path)?;
    let reporter = config
        .reporter(&options.reporter_id)
        .with_context(|| format!("Reporter '{}' does not exist", options.reporter_id))?;
    if reporter.protocol != ReporterProtocol::Cf {
        bail!("Reporter '{}' is not a CF Reporter", options.reporter_id);
    }
    let ledger_path = config.net_static_path();
    let ledger = NetStatic::load_with_legacy_reporter(&ledger_path, Some(&options.reporter_id));
    let filter = IfaceFilter::new(&reporter.interfaces);
    let now = calibrated_now.or_else(|| ledger.calibrated_time()).with_context(|| {
        "cannot determine calibrated time for traffic correction; retry with NTP available or after the agent has persisted a calibrated sample"
    })?;
    let at = Local
        .timestamp_millis_opt(now)
        .single()
        .context("calibrated traffic correction time is outside the local date range")?;
    let period_start = netstatic::period_start_ms(reporter.reset_day, at);
    let raw = ledger.query(&filter, period_start, now);
    let current = ledger
        .query_batch(&options.reporter_id, &filter, &[(period_start, now)])
        .pop()
        .expect("one correction query window");
    let rx_gib = options.rx_gib.unwrap_or(current.total.rx as f64 / GIB);
    let tx_gib = options.tx_gib.unwrap_or(current.total.tx as f64 / GIB);
    ledger.apply_local_correction(&options.reporter_id, period_start, raw, rx_gib, tx_gib);
    Ok(())
}

fn parse_positive_u64(flag: &str, raw: &str) -> Result<u64> {
    let value = raw
        .parse::<u64>()
        .with_context(|| format!("invalid {flag}"))?;
    if value == 0 {
        bail!("{flag} must be at least 1");
    }
    Ok(value)
}

fn parse_gib(flag: &str, raw: &str) -> Result<f64> {
    let value = raw
        .parse::<f64>()
        .with_context(|| format!("invalid {flag}"))?;
    if !value.is_finite() || value < 0.0 || value * GIB > i64::MAX as f64 {
        bail!("{flag} must be a finite non-negative GiB value");
    }
    Ok(value)
}

fn parse_bool(flag: &str, raw: &str) -> Result<bool> {
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => bail!("{flag} must be 0 or 1"),
    }
}

fn required_nonempty(value: Option<String>, flag: &str) -> Result<String> {
    let value = value.with_context(|| format!("configure-cf requires {flag}"))?;
    if value.trim().is_empty() {
        bail!("{flag} must not be empty");
    }
    Ok(value)
}

fn validate_reporter_id(id: &str) -> Result<()> {
    if id.is_empty()
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
    {
        bail!("reporter id must use A-Z, a-z, 0-9, _, . or -");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(path: &Path) -> ConfigureCfOptions {
        ConfigureCfOptions {
            config_path: path.to_path_buf(),
            default_net_static_path: path.with_extension("json").display().to_string(),
            server_id: "server".into(),
            secret: "secret".into(),
            worker_url: "https://example.com/update".into(),
            reporter_id: None,
            collect: Some(2),
            report_interval: Some(60),
            connection_mode: Some("auto".into()),
            reset_day: Some(20),
            interfaces: None,
            pings: vec![("ct".into(), "ct.example.com:80".into())],
            auto_update: Some(true),
            update_channel: Some(UpdateChannel::Prerelease),
            replace_cf: false,
        }
    }

    #[test]
    fn fresh_config_uses_cf_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        assert_eq!(configure_cf(options(&path)).unwrap(), "cf");
        let config = config::load(&path).unwrap();
        let reporter = config.reporter("cf").unwrap();
        assert_eq!(reporter.intervals.collect, 2);
        assert_eq!(reporter.reset_day, 20);
        assert_eq!(reporter.pings[0].target.target, "ct.example.com:80");
        let cf = config.reporters[0].cf.as_ref().unwrap();
        assert_eq!(cf.interval, 60);
        assert_eq!(cf.ct.as_deref(), Some("ct.example.com:80"));
        assert!(config.auto_update.enabled);
        assert_eq!(config.auto_update.channel, UpdateChannel::Prerelease);
    }

    #[test]
    fn installer_defaults_match_the_compiled_package_version() {
        let expected = format!("v{}", env!("CARGO_PKG_VERSION"));
        assert!(
            include_str!("../deploy/cf-install.sh").contains(&format!("SCRIPT_VERSION={expected}"))
        );
        assert!(include_str!("../deploy/cf-install.ps1")
            .contains(&format!("[string]$InstallVersion = \"{expected}\"")));
    }

    #[test]
    fn windows_installer_registers_boot_logon_and_resume_triggers() {
        let script = include_str!("../deploy/install.ps1");
        for expected in [
            "New-ScheduledTaskTrigger -AtStartup",
            "New-ScheduledTaskTrigger -AtLogOn",
            "Microsoft-Windows-Power-Troubleshooter",
            "EventID=1",
            "Microsoft-Windows-Kernel-Power",
            "EventID=107",
            "$eventTrigger.Delay = \"PT10S\"",
            "-MultipleInstances IgnoreNew",
        ] {
            assert!(script.contains(expected), "missing {expected}");
        }
    }

    #[test]
    fn seeded_reporters_are_removed_before_cf_is_added() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, include_str!("../config.example.toml")).unwrap();
        assert_eq!(configure_cf(options(&path)).unwrap(), "cf");
        let config = config::load(&path).unwrap();
        assert_eq!(config.reporters.len(), 1);
        assert_eq!(config.reporters[0].id, "cf");
    }

    #[test]
    fn implicit_id_updates_the_only_existing_cf_and_preserves_unspecified_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let mut first = options(&path);
        first.reporter_id = Some("primary".into());
        first.interfaces = Some(vec!["eth0".into()]);
        configure_cf(first).unwrap();

        let mut second = options(&path);
        second.collect = None;
        second.reset_day = None;
        second.interfaces = None;
        second.pings.clear();
        assert_eq!(configure_cf(second).unwrap(), "primary");
        let reporter = config::load(&path).unwrap().reporter("primary").unwrap();
        assert_eq!(reporter.intervals.collect, 2);
        assert_eq!(reporter.reset_day, 20);
        assert_eq!(reporter.interfaces, vec!["eth0"]);
        assert_eq!(reporter.pings.len(), 1);
    }

    #[test]
    fn multiple_cf_reporters_require_an_explicit_id() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        for id in ["cf-a", "cf-b"] {
            let mut item = options(&path);
            item.reporter_id = Some(id.into());
            configure_cf(item).unwrap();
        }
        let error = configure_cf(options(&path)).unwrap_err().to_string();
        assert!(error.contains("multiple CF Reporters"));
    }

    #[test]
    fn replace_cf_does_not_remove_other_protocols() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let mut config = LocalConfig {
            schema: CONFIG_SCHEMA,
            data_dir: dir.path().display().to_string(),
            auto_update: AutoUpdateConfig::default(),
            reporters: vec![
                new_cf_reporter("old-a".into()),
                new_cf_reporter("old-b".into()),
            ],
        };
        for reporter in &mut config.reporters {
            let cf = reporter.cf.as_mut().unwrap();
            cf.server_id = "server".into();
            cf.secret = "secret".into();
            cf.url = "https://example.com/update".into();
        }
        config.reporters.push(ReporterConfig {
            id: "probe".into(),
            cf: None,
            komari: None,
            probe: Some(crate::model::ProbeSection {
                server_id: "probe-server".into(),
                secret: "probe-secret".into(),
                worker_url: "https://example.com/report".into(),
                report_interval: 60,
                reset_day: 1,
                report_errors: true,
                report_self: false,
                interfaces: Vec::new(),
                disks: Vec::new(),
                report_gpu: false,
                intervals: Default::default(),
                pings: Vec::new(),
                ext: Default::default(),
            }),
        });
        config::persist(&path, &config).unwrap();

        let mut replacement = options(&path);
        replacement.replace_cf = true;
        assert_eq!(configure_cf(replacement).unwrap(), "cf");
        let config = config::load(&path).unwrap();
        assert!(config.reporter("probe").is_some());
        assert!(config.reporter("cf").is_some());
        assert_eq!(
            config
                .reporter_specs()
                .iter()
                .filter(|reporter| reporter.protocol == ReporterProtocol::Cf)
                .count(),
            1
        );
    }

    #[test]
    fn local_correction_does_not_create_a_server_confirmation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        configure_cf(options(&path)).unwrap();
        let now = crate::model::now_millis();
        set_traffic_correction_with_clock(
            CorrectionOptions {
                config_path: path.clone(),
                reporter_id: "cf".into(),
                rx_gib: Some(3.0),
                tx_gib: Some(4.0),
            },
            Some(now),
        )
        .unwrap();
        let config = config::load(&path).unwrap();
        let ledger = NetStatic::load(&config.net_static_path());
        assert_eq!(ledger.confirm_pending("cf"), None);
        let reporter = config.reporter("cf").unwrap();
        let at = Local.timestamp_millis_opt(now).single().unwrap();
        let period = netstatic::period_start_ms(reporter.reset_day, at);
        assert_eq!(
            ledger.query_monthly("cf", &IfaceFilter::new(&[]), period, now),
            (3 * 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024)
        );

        set_traffic_correction_with_clock(
            CorrectionOptions {
                config_path: path,
                reporter_id: "cf".into(),
                rx_gib: Some(5.0),
                tx_gib: None,
            },
            Some(now),
        )
        .unwrap();
        let reloaded = NetStatic::load(&config.net_static_path());
        assert_eq!(reloaded.confirm_pending("cf"), None);
        assert_eq!(
            reloaded.query_monthly("cf", &IfaceFilter::new(&[]), period, now),
            (5 * 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn local_correction_uses_the_calibrated_billing_period() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        configure_cf(options(&path)).unwrap();
        let calibrated = Local
            .with_ymd_and_hms(2026, 7, 19, 23, 59, 59)
            .single()
            .unwrap();
        set_traffic_correction_with_clock(
            CorrectionOptions {
                config_path: path.clone(),
                reporter_id: "cf".into(),
                rx_gib: Some(3.0),
                tx_gib: Some(4.0),
            },
            Some(calibrated.timestamp_millis()),
        )
        .unwrap();

        let config = config::load(&path).unwrap();
        let reporter = config.reporter("cf").unwrap();
        let ledger = NetStatic::load(&config.net_static_path());
        let period = netstatic::period_start_ms(reporter.reset_day, calibrated);
        assert_eq!(
            ledger.query_monthly(
                "cf",
                &IfaceFilter::new(&[]),
                period,
                calibrated.timestamp_millis(),
            ),
            (3 * 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024)
        );

        let other_period = netstatic::period_start_ms(
            reporter.reset_day,
            Local
                .with_ymd_and_hms(2026, 7, 20, 0, 0, 1)
                .single()
                .unwrap(),
        );
        assert_ne!(period, other_period);
        assert_eq!(
            ledger.query_monthly(
                "cf",
                &IfaceFilter::new(&[]),
                other_period,
                calibrated.timestamp_millis(),
            ),
            (0, 0)
        );
    }
}
