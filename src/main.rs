mod buffer;
mod collector;
mod config;
mod install_cli;
mod model;
mod netstatic;
mod reporter;
mod reporter_cf;
mod reporter_komari;
mod scheduler;
#[cfg(windows)]
mod tray;
mod updater;
mod worker;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::watch;

use crate::config::{LocalConfig, SharedConfig};

const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(windows)]
    if updater::maybe_finish_windows_update()? {
        return Ok(());
    }

    if install_cli::run_if_requested().await? {
        return Ok(());
    }

    #[cfg(windows)]
    if std::env::args_os().any(|arg| arg == "--tray") {
        return tray::run();
    }

    let default_filter = if std::env::args_os().any(|arg| arg == "--debug") {
        "debug"
    } else {
        "info"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter)),
        )
        .init();

    let config_path = parse_config_arg();
    let local = config::load(&config_path).context("failed to load config")?;
    let initial_specs = local.reporter_specs();
    let agent_clock = Arc::new(reporter::AgentClock::default());
    tracing::info!(
        path = %config_path.display(),
        reporters = initial_specs.len(),
        "config loaded"
    );
    for spec in &initial_specs {
        tracing::info!(
            reporter_id = %spec.id,
            protocol = %spec.protocol,
            "reporter configured"
        );
        if spec.protocol == "cf" {
            for ping in &spec.pings {
                if !["ct", "cu", "cm", "bd", "bgp"].contains(&ping.target.name.as_str()) {
                    tracing::warn!(
                        reporter_id = %spec.id,
                        name = %ping.target.name,
                        "CF has no field for this ping group"
                    );
                }
            }
        }
    }

    let net_static_path = PathBuf::from(&local.net_static_path);
    let (shared, intervals_rx, config_rx) = SharedConfig::new(local.clone(), config_path.clone());
    let buffers = Arc::new(buffer::Buffers::new());
    for spec in &initial_specs {
        buffers.register(spec.id.clone());
    }
    let legacy_cf_reporter = initial_specs
        .iter()
        .find(|spec| spec.protocol == "cf")
        .map(|spec| spec.id.as_str());
    let net = netstatic::NetStatic::load_with_legacy_reporter(&net_static_path, legacy_cf_reporter);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (update_check_tx, update_check_rx) = watch::channel(0_u64);
    let _ntp_refresh_handle = agent_clock.spawn_ntp_refresh(shutdown_rx.clone());

    updater::spawn(
        shared.subscribe_config(),
        update_check_rx,
        shutdown_tx.clone(),
        shutdown_rx.clone(),
    );

    // The persistent traffic ledger captures all interfaces. Reporter-specific
    // filters and corrections are applied only when a payload is built.
    let net_sampler_handle = {
        let net = net.clone();
        let clock = Arc::clone(&agent_clock);
        let mut shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut ticker = crate::worker::ticker(net.sample_interval());
            let all = collector::net::IfaceFilter::all();
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let report_time = clock.report_time();
                        let sample_time = report_time.accurate_ts.unwrap_or(report_time.local_ts);
                        net.sample(&all, sample_time, report_time.accurate_ts.is_some());
                        net.flush_if_due();
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() { break; }
                    }
                }
            }
        })
    };

    let (ping_tx, ping_rx) = watch::channel(worker::ping::PingSnapshot::new());
    let (gpu_name_tx, gpu_name_rx) = watch::channel::<Option<String>>(None);
    let (gpu_tx, gpu_rx) = watch::channel::<Vec<model::GpuRecord>>(Vec::new());
    let (ip_handle, ip_rx) = worker::public_ip::spawn(Arc::clone(&buffers), intervals_rx.clone());
    watch_task("public-ip", ip_handle);
    let (slow_handle, slow_rx, self_rx) =
        worker::slow::spawn(intervals_rx.clone(), Arc::clone(&buffers));
    watch_task("slow", slow_handle);
    let (diskio_handle, diskio_rx) =
        worker::diskio::spawn(intervals_rx.clone(), Arc::clone(&buffers));
    watch_task("diskio", diskio_handle);

    let init_cfg = shared.get();
    let init_pings = init_cfg.effective_pings();
    let mut ping_worker = (!init_pings.is_empty()).then(|| {
        worker::ping::PingWorker::start(
            init_pings,
            ping_tx.clone(),
            Arc::clone(&buffers),
            intervals_rx.clone(),
        )
    });
    let mut gpu_handle = init_cfg.effective_gpu().then(|| {
        worker::gpu::start(
            gpu_name_tx.clone(),
            gpu_tx.clone(),
            Arc::clone(&buffers),
            intervals_rx.clone(),
        )
    });

    // Local file reload and shared collector reconciliation. Endpoint identity,
    // credentials, protocol and Reporter count are restart-only; scoped
    // collection settings remain hot-reloadable.
    {
        let watch_path = config_path.clone();
        let shared = Arc::clone(&shared);
        let ping_tx = ping_tx.clone();
        let gpu_name_tx = gpu_name_tx.clone();
        let gpu_tx = gpu_tx.clone();
        let buffers = Arc::clone(&buffers);
        let intervals_rx2 = intervals_rx.clone();
        let initial_connections = connection_signature(&local);
        let mut config_rx = config_rx;
        tokio::spawn(async move {
            let mtime =
                |path: &std::path::Path| std::fs::metadata(path).and_then(|m| m.modified()).ok();
            let mut last_mtime = mtime(&watch_path);
            let mut ticker = tokio::time::interval(Duration::from_secs(3));
            ticker.tick().await;
            let mut applied_pings = shared.get().effective_pings();
            let mut applied_gpu = shared.get().effective_gpu();
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let current_mtime = mtime(&watch_path);
                        if current_mtime == last_mtime { continue; }
                        last_mtime = current_mtime;
                        let cfg = match config::load(&watch_path) {
                            Ok(cfg) => cfg,
                            Err(error) => {
                                tracing::warn!(%error, "config hot reload rejected");
                                continue;
                            }
                        };
                        if connection_signature(&cfg) != initial_connections {
                            tracing::warn!(
                                "Reporter endpoints/count changed; restart required, hot reload skipped"
                            );
                            continue;
                        }
                        if cfg != shared.get() {
                            // 原子应用:写锁内复核 mtime,避免旧快照覆盖远端应用/
                            // Komari 学习刚刚落盘的新配置(C1)。
                            match shared.update_local_from_disk(&watch_path) {
                                Ok(true) => tracing::info!("config hot-reloaded"),
                                Ok(false) => tracing::debug!(
                                    "config changed during reload; retrying next tick"
                                ),
                                Err(error) => {
                                    tracing::warn!(%error, "config hot reload rejected")
                                }
                            }
                        }
                    }
                    changed = config_rx.changed() => {
                        if changed.is_err() { return; }
                        let cfg = config_rx.borrow().clone();
                        let desired_pings = cfg.effective_pings();
                        if desired_pings != applied_pings {
                            if let Some(worker) = ping_worker.take() {
                                worker.stop();
                            }
                            ping_worker = (!desired_pings.is_empty()).then(|| {
                                worker::ping::PingWorker::start(
                                    desired_pings.clone(),
                                    ping_tx.clone(),
                                    Arc::clone(&buffers),
                                    intervals_rx2.clone(),
                                )
                            });
                            applied_pings = desired_pings;
                            tracing::info!(groups = applied_pings.len(), "ping workers reconciled");
                        }
                        let desired_gpu = cfg.effective_gpu();
                        if desired_gpu != applied_gpu {
                            if desired_gpu {
                                gpu_handle = Some(worker::gpu::start(
                                    gpu_name_tx.clone(),
                                    gpu_tx.clone(),
                                    Arc::clone(&buffers),
                                    intervals_rx2.clone(),
                                ));
                            } else if let Some(handle) = gpu_handle.take() {
                                handle.abort();
                            }
                            applied_gpu = desired_gpu;
                            tracing::info!(enable = applied_gpu, "GPU worker reconciled");
                        }
                    }
                }
            }
        });
    }

    let collector = scheduler::Scheduler::new(
        Arc::clone(&buffers),
        Arc::clone(&agent_clock),
        intervals_rx,
        ping_rx.clone(),
        gpu_rx.clone(),
        slow_rx.clone(),
        self_rx,
        diskio_rx.clone(),
        shutdown_rx.clone(),
    );
    let collector_handle = tokio::spawn(collector.run());
    tokio::task::yield_now().await;

    // Every configured Reporter gets its own runner task and, for Komari,
    // an additional WebSocket worker task, including same-protocol peers.
    let mut reporter_handles: Vec<(String, tokio::task::JoinHandle<()>)> = Vec::new();
    for spec in initial_specs {
        let reporter = Arc::new(
            reporter::Reporter::new(
                &spec.worker_url,
                &spec.secret,
                AGENT_VERSION,
                &spec.id,
                &spec.protocol,
                Arc::clone(&agent_clock),
            )
            .with_context(|| format!("failed to initialize Reporter {}", spec.id))?,
        );
        let komari_tx = if spec.protocol == "komari" {
            let (tx, rx) = watch::channel(worker::komari::KomariOut::default());
            let komari_handle = worker::komari::spawn(
                spec.id.clone(),
                spec.worker_url.clone(),
                spec.secret.clone(),
                rx,
                Arc::clone(&buffers),
                Arc::clone(&shared),
                ping_rx.clone(),
            );
            watch_task("komari", komari_handle);
            Some(tx)
        } else {
            None
        };
        let runner = scheduler::ReporterRunner::new(
            spec.id.clone(),
            Arc::clone(&shared),
            Arc::clone(&buffers),
            reporter,
            net.clone(),
            shared.subscribe_config(),
            ip_rx.clone(),
            gpu_name_rx.clone(),
            ping_rx.clone(),
            gpu_rx.clone(),
            slow_rx.clone(),
            diskio_rx.clone(),
            shutdown_rx.clone(),
            update_check_tx.clone(),
            AGENT_VERSION.to_string(),
            komari_tx,
        );
        reporter_handles.push((spec.id.clone(), tokio::spawn(runner.run())));
    }

    tokio::spawn(async move {
        wait_for_signal().await;
        shutdown_tx.send_replace(true);
    });

    collector_handle
        .await
        .context("collection scheduler failed")?;
    // 优雅退出:先等各路 Reporter 停止(完成最后一次上报),再等采样任务
    // 收尾,最后 flush 流量账本,缩小丢失窗口。
    for (reporter_id, handle) in reporter_handles {
        if let Err(error) = handle.await {
            tracing::error!(reporter_id = %reporter_id, error = %error, "Reporter 任务异常退出(panic)");
        }
    }
    if let Err(error) = net_sampler_handle.await {
        tracing::error!(error = %error, "netstatic 采样任务异常退出(panic)");
    }
    net.flush();
    tracing::info!("probe-rs stopped");
    Ok(())
}

/// 后台任务监督:panic(或未预期的提前返回)必须产生可观察错误,而不是静默
/// 永久停报。被显式 abort 的任务(配置重建/进程退出)不计为异常。
fn watch_task(name: &'static str, handle: tokio::task::JoinHandle<()>) {
    tokio::spawn(async move {
        match handle.await {
            Ok(()) => tracing::debug!(task = name, "后台任务正常退出"),
            Err(error) if error.is_cancelled() => {}
            Err(error) => tracing::error!(task = name, error = %error, "后台任务异常退出(panic)"),
        }
    });
}

use std::time::Duration;

fn connection_signature(cfg: &LocalConfig) -> Vec<(String, String, String, String, String)> {
    cfg.reporter_specs()
        .into_iter()
        .map(|spec| {
            let (id, protocol, server_id, secret, worker_url) = spec.connection_key();
            (
                id.to_string(),
                protocol.to_string(),
                server_id.to_string(),
                secret.to_string(),
                worker_url.to_string(),
            )
        })
        .collect()
}

fn parse_config_arg() -> PathBuf {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-c" | "--config" => {
                if let Some(path) = args.next() {
                    return PathBuf::from(path);
                }
            }
            _ if arg.starts_with("--config=") => {
                return PathBuf::from(arg.trim_start_matches("--config="));
            }
            _ => {}
        }
    }
    config::default_config_path()
}

async fn wait_for_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("failed to register SIGTERM");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
