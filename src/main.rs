mod buffer;
mod collector;
mod config;
mod model;
mod netstatic;
mod reporter;
mod reporter_cf;
mod reporter_komari;
mod scheduler;
#[cfg(windows)]
mod tray;
mod worker;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::watch;

use crate::config::SharedConfig;

const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(windows)]
    if std::env::args_os().any(|arg| arg == "--tray") {
        return tray::run();
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config_path = parse_config_arg();
    let local = config::load(&config_path).context("配置加载失败")?;
    tracing::info!(path = %config_path.display(), server_id = %local.server_id, "配置已加载");
    if local.protocol == "cf" {
        tracing::info!(url = %local.worker_url, "CF 协议模式（POST /update）");
        for p in &local.pings {
            if !["ct", "cu", "cm", "bd", "bgp"].contains(&p.name.as_str()) {
                tracing::warn!(name = %p.name, "CF 模式下该 ping 组无落点（仅 ct/cu/cm/bd 会被上报）");
            }
        }
    }

    let net_static_path = PathBuf::from(&local.net_static_path);
    let reporter = Arc::new(
        reporter::Reporter::new(&local.worker_url, &local.secret, AGENT_VERSION)
            .context("reporter 初始化失败")?,
    );
    let (shared, intervals_rx, config_rx) = SharedConfig::new(local.clone(), config_path.clone());
    let buffers = Arc::new(buffer::Buffers::new());
    let net = netstatic::NetStatic::load(&net_static_path);
    // watch 保留最新退出状态；即使任务当时正在采集/上报，返回 select 后也不会漏信号。
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // netstatic 采样 task：每 2s 采样，每 10min 落盘
    {
        let net = net.clone();
        let shared = Arc::clone(&shared);
        let mut shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(net.sample_interval());
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let filter = collector::net::IfaceFilter::new(&shared.get().interfaces);
                        net.sample(&filter);
                        net.flush_if_due();
                    }
                    r = shutdown_rx.changed() => {
                        if r.is_err() || *shutdown_rx.borrow() { break; }
                    },
                }
            }
        });
    }

    let (ping_tx, ping_rx) = tokio::sync::watch::channel(worker::ping::PingSnapshot::new());
    let (gpu_name_tx, gpu_name_rx) = tokio::sync::watch::channel::<Option<String>>(None);
    let (gpu_tx, gpu_rx) = tokio::sync::watch::channel::<Vec<model::GpuRecord>>(Vec::new());
    let (_ip_handle, ip_rx) = worker::public_ip::spawn(Arc::clone(&buffers), intervals_rx.clone());
    let (_slow_handle, slow_rx, self_rx) = worker::slow::spawn(
        Arc::clone(&shared),
        intervals_rx.clone(),
        Arc::clone(&buffers),
    );
    let (_diskio_handle, diskio_rx) =
        worker::diskio::spawn(intervals_rx.clone(), Arc::clone(&buffers));

    // komari 模式：WS worker（v2 JSON-RPC）；其余协议不建通道
    let (komari_tx, _komari_handle) = if local.protocol == "komari" {
        let (tx, rx) = tokio::sync::watch::channel(worker::komari::KomariOut::default());
        let h = worker::komari::spawn(
            local.worker_url.clone(),
            local.secret.clone(),
            rx,
            Arc::clone(&buffers),
        );
        (Some(tx), Some(h))
    } else {
        (None, None)
    };

    let init_cfg = shared.get();
    let mut ping_worker = (!init_cfg.pings.is_empty()).then(|| {
        worker::ping::PingWorker::start(
            init_cfg.pings.clone(),
            ping_tx.clone(),
            Arc::clone(&buffers),
            intervals_rx.clone(),
        )
    });
    let mut gpu_handle = init_cfg.enable_gpu.then(|| {
        worker::gpu::start(
            gpu_name_tx.clone(),
            gpu_tx.clone(),
            Arc::clone(&buffers),
            intervals_rx.clone(),
        )
    });

    // 配置 supervisor：3s 轮询配置文件（本地热加载）+ 监听配置变更（远端下发），
    // 统一 reconcile pings / enable_gpu worker；interfaces/intervals 经 SharedConfig 即时生效
    {
        let watch_path = config_path.clone();
        let shared = Arc::clone(&shared);
        let ping_tx = ping_tx.clone();
        let gpu_name_tx = gpu_name_tx.clone();
        let gpu_tx = gpu_tx.clone();
        let buffers = Arc::clone(&buffers);
        let intervals_rx2 = intervals_rx.clone();
        let mut config_rx = config_rx;
        tokio::spawn(async move {
            let mtime = |p: &std::path::Path| std::fs::metadata(p).and_then(|m| m.modified()).ok();
            let mut last_mtime = mtime(&watch_path);
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(3));
            ticker.tick().await; // 跳过立即触发的第一拍
            let mut applied_pings = shared.get().pings;
            let mut applied_gpu = shared.get().enable_gpu;
            let applied_protocol = shared.get().protocol;
            loop {
                tokio::select! {
                    // 本地文件变更 → 重载进 SharedConfig（会触发 config_tx → 走下方 reconcile）
                    _ = ticker.tick() => {
                        let m = mtime(&watch_path);
                        if m == last_mtime {
                            continue;
                        }
                        last_mtime = m;
                        match config::load(&watch_path) {
                            Ok(cfg) => {
                                if cfg.protocol != applied_protocol {
                                    tracing::warn!(protocol = %cfg.protocol, "protocol 变更需重启 agent 才生效，已忽略");
                                    continue;
                                }
                                if cfg != shared.get() {
                                    shared.update_local(cfg);
                                    tracing::info!("配置已热加载");
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "热加载配置失败，保持原配置");
                            }
                        }
                    }
                    // 配置变更（本地热加载或远端下发）→ 重建差异 worker
                    r = config_rx.changed() => {
                        if r.is_err() { return; }
                        let cfg = config_rx.borrow().clone();
                        if cfg.pings != applied_pings {
                            if let Some(w) = ping_worker.take() {
                                w.stop();
                            }
                            ping_worker = (!cfg.pings.is_empty()).then(|| {
                                worker::ping::PingWorker::start(cfg.pings.clone(), ping_tx.clone(), Arc::clone(&buffers), intervals_rx2.clone())
                            });
                            applied_pings = cfg.pings.clone();
                            tracing::info!(groups = applied_pings.len(), "探测目标已重建");
                        }
                        if cfg.enable_gpu != applied_gpu {
                            if cfg.enable_gpu {
                                gpu_handle = Some(worker::gpu::start(gpu_name_tx.clone(), gpu_tx.clone(), Arc::clone(&buffers), intervals_rx2.clone()));
                            } else if let Some(h) = gpu_handle.take() {
                                h.abort();
                            }
                            applied_gpu = cfg.enable_gpu;
                            tracing::info!(enable = applied_gpu, "GPU 采集状态已切换");
                        }
                    }
                }
            }
        });
    }

    let sched = scheduler::Scheduler::new(
        Arc::clone(&shared),
        buffers,
        reporter,
        net.clone(),
        intervals_rx,
        ip_rx,
        gpu_name_rx,
        ping_rx,
        gpu_rx,
        slow_rx,
        self_rx,
        diskio_rx,
        shutdown_rx,
        AGENT_VERSION.to_string(),
        komari_tx,
    );

    tokio::spawn(async move {
        wait_for_signal().await;
        shutdown_tx.send_replace(true);
    });

    sched.run().await;

    // 退出前落盘，缩小丢失窗口
    net.flush();
    tracing::info!("probe-rs 已退出");
    Ok(())
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
        let mut term = signal(SignalKind::terminate()).expect("注册 SIGTERM 失败");
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
