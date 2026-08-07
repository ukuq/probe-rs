//! 调度器：collect/report 双层 ticker（相互独立，无频率约束）
//!
//! - dynamic 为空也必报（心跳）；static 首报必带，之后每 10 分钟或 IP/GPU 变化时携带
//! - 上报失败：数据 restore 回缓冲（有界，满 1000 条丢最旧）

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{watch, Notify};
use tokio::time::{interval, interval_at, Interval};

use crate::buffer::Buffers;
use crate::collector::{self, net, CpuMonitor};
use crate::config::SharedConfig;
use crate::model::{
    AsyncRecord, DynamicRecord, GpuRecord, Intervals, PingRecord, Report, SelfRecord, SlowBlock,
};
use crate::netstatic::{period_start_ms, NetStatic};
use crate::reporter::Reporter;
use crate::worker::ping::PingSnapshot;
use crate::worker::public_ip::IpSnapshot;

const STATIC_REFRESH: Duration = Duration::from_secs(600);

pub struct Scheduler {
    cfg: Arc<SharedConfig>,
    buffers: Arc<Buffers>,
    reporter: Arc<Reporter>,
    netstatic: NetStatic,
    intervals_rx: watch::Receiver<Intervals>,
    ip_rx: watch::Receiver<IpSnapshot>,
    gpu_name_rx: watch::Receiver<Option<String>>,
    ping_rx: watch::Receiver<PingSnapshot>,
    gpu_rx: watch::Receiver<Vec<GpuRecord>>,
    slow_rx: watch::Receiver<Option<SlowBlock>>,
    self_rx: watch::Receiver<Option<SelfRecord>>,
    shutdown: Arc<Notify>,
    agent_version: String,

    cpu: CpuMonitor,
    prev_net: Option<(net::NetBytes, Instant)>,
    last_static: Option<Instant>,
    /// CF 模式：static 缓存（CF metrics 每次上报都带 static 字段）
    static_cache: Option<crate::model::StaticInfo>,
    // 各异步源上次摘取的 ts：仅当快照 ts 更新才带入 dynamic 记录（新鲜度去重）
    last_ping_ts: std::collections::HashMap<String, i64>,
    last_gpu_ts: i64,
    last_slow_ts: i64,
    last_self_ts: i64,
}

impl Scheduler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: Arc<SharedConfig>,
        buffers: Arc<Buffers>,
        reporter: Arc<Reporter>,
        netstatic: NetStatic,
        intervals_rx: watch::Receiver<Intervals>,
        ip_rx: watch::Receiver<IpSnapshot>,
        gpu_name_rx: watch::Receiver<Option<String>>,
        ping_rx: watch::Receiver<PingSnapshot>,
        gpu_rx: watch::Receiver<Vec<GpuRecord>>,
        slow_rx: watch::Receiver<Option<SlowBlock>>,
        self_rx: watch::Receiver<Option<SelfRecord>>,
        shutdown: Arc<Notify>,
        agent_version: String,
    ) -> Self {
        Self {
            cfg,
            buffers,
            reporter,
            netstatic,
            intervals_rx,
            ip_rx,
            gpu_name_rx,
            ping_rx,
            gpu_rx,
            slow_rx,
            self_rx,
            shutdown,
            agent_version,
            cpu: CpuMonitor::new(),
            prev_net: None,
            last_static: None,
            static_cache: None,
            last_ping_ts: std::collections::HashMap::new(),
            last_gpu_ts: 0,
            last_slow_ts: 0,
            last_self_ts: 0,
        }
    }

    pub async fn run(mut self) {
        let initial = *self.intervals_rx.borrow();
        let mut collect_ticker = ticker(initial.collect);
        let mut report_ticker = ticker(initial.report);
        tracing::info!(
            collect = initial.collect,
            report = initial.report,
            "scheduler 启动"
        );
        loop {
            tokio::select! {
                _ = collect_ticker.tick() => self.on_collect(),
                _ = report_ticker.tick() => self.on_report().await,
                r = self.intervals_rx.changed() => {
                    if r.is_err() { return; }
                    let itv = *self.intervals_rx.borrow();
                    collect_ticker = ticker_from_next(itv.collect);
                    report_ticker = ticker_from_next(itv.report);
                    tracing::info!(collect = itv.collect, report = itv.report, "ticker 已按远端配置重建");
                }
                r = self.ip_rx.changed() => {
                    if r.is_err() { return; }
                    // 公网 IP 变化（含启动后首次查到）→ 下个 report 重报 static
                    self.last_static = None;
                }
                r = self.gpu_name_rx.changed() => {
                    if r.is_err() { return; }
                    self.last_static = None;
                }
                _ = self.shutdown.notified() => {
                    tracing::info!("收到退出信号，scheduler 停止");
                    return;
                }
            }
        }
    }

    fn on_collect(&mut self) {
        let cfg = self.cfg.get();
        let filter = net::IfaceFilter::new(&cfg.interfaces);
        let now_ms = crate::model::now_millis();

        let cpu_usage = self.cpu.sample().map(|u| (u * 100.0).round() / 100.0);

        let (_, mem_used, _, swap_used) = collector::memory();

        let net_now = collector::net_bytes(&filter);
        let (net_rx_speed, net_tx_speed) = match self.prev_net {
            Some((prev, at)) => {
                let dt = at.elapsed().as_secs_f64();
                if dt > 0.0 {
                    (
                        Some((net::counter_delta(net_now.rx, prev.rx) as f64 / dt) as u64),
                        Some((net::counter_delta(net_now.tx, prev.tx) as f64 / dt) as u64),
                    )
                } else {
                    (None, None)
                }
            }
            None => (None, None),
        };
        self.prev_net = Some((net_now, Instant::now()));

        let period_start = period_start_ms(cfg.reset_day, chrono::Local::now());
        let (net_rx_monthly, net_tx_monthly) =
            self.netstatic.query_monthly(&filter, period_start, now_ms);

        // fast 记录：只含快变字段，ts 即 tick 测量时刻
        self.buffers.push_dynamic(DynamicRecord {
            ts: now_ms,
            cpu_usage,
            mem_used: Some(mem_used),
            swap_used: Some(swap_used),
            load: collector::load(),
            net_rx: Some(net_now.rx),
            net_tx: Some(net_now.tx),
            net_rx_speed,
            net_tx_speed,
            net_rx_monthly: Some(net_rx_monthly),
            net_tx_monthly: Some(net_tx_monthly),
        });

        // 异步记录：快照 ts 更新才摘取（新鲜度去重），每条 ts 为各自真实测量时刻
        {
            let snap = self.ping_rx.borrow();
            let fresh: Vec<PingRecord> = snap
                .values()
                .filter(|r| r.ts > *self.last_ping_ts.get(&r.name).unwrap_or(&0))
                .cloned()
                .collect();
            for r in fresh {
                self.last_ping_ts.insert(r.name.clone(), r.ts);
                self.buffers.push_async(AsyncRecord::Ping(r));
            }
        }
        {
            let snap = self.gpu_rx.borrow();
            let ts = snap.first().map_or(0, |r| r.ts);
            if ts > self.last_gpu_ts {
                self.last_gpu_ts = ts;
                for r in snap.iter() {
                    self.buffers.push_async(AsyncRecord::Gpu(r.clone()));
                }
            }
        }
        {
            let snap = self.slow_rx.borrow().clone();
            if let Some(b) = snap {
                if b.ts > self.last_slow_ts {
                    self.last_slow_ts = b.ts;
                    self.buffers.push_async(AsyncRecord::Slow(b));
                }
            }
        }
        {
            let snap = self.self_rx.borrow().clone();
            if let Some(r) = snap {
                if r.ts > self.last_self_ts {
                    self.last_self_ts = r.ts;
                    self.buffers.push_async(AsyncRecord::Self_(r));
                }
            }
        }
    }

    async fn on_report(&mut self) {
        let cfg = self.cfg.get();
        if cfg.protocol == "cf" {
            self.on_report_cf(cfg).await;
            return;
        }
        let (dynamic, async_records, errors) = self.buffers.drain();
        // report_errors=false 时不上报错误事件（缓冲照常 drain，防积压）
        let errors = if cfg.report_errors {
            errors
        } else {
            Vec::new()
        };

        let due = self
            .last_static
            .is_none_or(|t| t.elapsed() >= STATIC_REFRESH);
        let static_info = if due {
            self.last_static = Some(Instant::now());
            let (ipv4, ipv6) = self.ip_rx.borrow().clone();
            let gpu_name = self.gpu_name_rx.borrow().clone();
            Some(collector::static_info(
                ipv4,
                ipv6,
                gpu_name,
                &self.agent_version,
                &cfg,
            ))
        } else {
            None
        };

        let report = Report {
            server_id: cfg.server_id.clone(),
            config_version: cfg.config_version,
            static_info,
            dynamic,
            async_records,
            errors,
        };

        match self.reporter.send(&report).await {
            Ok(action) => {
                // next.static：服务端要求下次上报强制带 static
                if action.next_static {
                    self.last_static = None;
                    tracing::info!("服务端要求刷新 static，下次上报携带");
                }
                if let Some(remote) = action.config {
                    match self.cfg.apply_remote(remote) {
                        Ok(()) => {
                            // 配置变了 → 下次上报带 static（intervals/reset_day 在其中）
                            self.last_static = None;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "远端配置被拒绝");
                        }
                    }
                }
            }
            Err(e) => {
                // 数据保留待重发（有界，满 1000 条丢最旧）；上报失败本身也记为错误事件
                let crate::model::Report {
                    dynamic,
                    async_records,
                    errors,
                    ..
                } = report;
                self.buffers.restore(dynamic, async_records, errors);
                self.buffers.push_error("reporter", e.to_string());
                tracing::warn!(error = %e, "上报失败，数据已保留待重发");
            }
        }
    }

    /// CF 协议上报：metrics 每次全量（static 走缓存），dynamic[] → samples[]。
    /// errors/self 无落点直接丢弃；ping/slow/gpu 从快照直读最新值
    async fn on_report_cf(&mut self, cfg: crate::config::LocalConfig) {
        let (dynamic, _async, _errors) = self.buffers.drain();

        if self
            .last_static
            .is_none_or(|t| t.elapsed() >= STATIC_REFRESH)
            || self.static_cache.is_none()
        {
            self.last_static = Some(Instant::now());
            let (ipv4, ipv6) = self.ip_rx.borrow().clone();
            let gpu_name = self.gpu_name_rx.borrow().clone();
            self.static_cache = Some(collector::static_info(
                ipv4,
                ipv6,
                gpu_name,
                &self.agent_version,
                &cfg,
            ));
        }
        let st = self.static_cache.clone().expect("static_cache 上面刚填充");

        let metrics = {
            let slow = self.slow_rx.borrow();
            let gpus = self.gpu_rx.borrow();
            let pings = self.ping_rx.borrow();
            crate::reporter_cf::build_metrics(&st, dynamic.last(), slow.as_ref(), &gpus, &pings)
        };
        let samples = if cfg.ext.cf.batch {
            dynamic
                .iter()
                .map(crate::reporter_cf::build_sample)
                .collect()
        } else {
            Vec::new()
        };

        // 校正确认是独立请求（CF 服务端见到 correction 字段会把整个请求
        // 当确认、丢弃 metrics）——先于主上报发出，成功后停止重传
        if let Some((rx_gb, tx_gb)) = self.netstatic.confirm_pending() {
            if cfg.ext.cf.correction {
                let confirm = crate::reporter_cf::CfConfirm {
                    id: cfg.server_id.clone(),
                    secret: cfg.secret.clone(),
                    rx_correction: rx_gb,
                    tx_correction: tx_gb,
                };
                match self.reporter.send_cf_confirm(&confirm).await {
                    Ok(()) => {
                        self.netstatic.clear_confirm();
                        tracing::info!(rx_gb, tx_gb, "流量校正确认已回传");
                    }
                    Err(e) => tracing::warn!(error = %e, "校正确认失败，下个周期重试"),
                }
            } else {
                // 回路被关闭：丢弃待确认，不发请求
                self.netstatic.clear_confirm();
            }
        }

        let update = crate::reporter_cf::CfUpdate {
            id: cfg.server_id.clone(),
            secret: cfg.secret.clone(),
            metrics,
            samples,
        };

        match self.reporter.send_cf(&update, &cfg.config_version).await {
            Ok(resp) => {
                if let Some(push) = resp.push {
                    let remote = crate::reporter_cf::synthesize_remote(&push, &cfg.intervals);
                    match self.cfg.apply_remote(remote) {
                        // 配置变了 → 刷新 static 缓存（下个 report 重建）
                        Ok(()) => self.last_static = None,
                        Err(e) => tracing::warn!(error = %e, "CF 配置被拒绝"),
                    }
                }
                // 校正回路用最新配置（push 可能刚改了 reset_day/interfaces）
                let cur = self.cfg.get();
                match resp.correction {
                    Some((rx_gb, tx_gb)) if cur.ext.cf.correction => {
                        let filter = net::IfaceFilter::new(&cur.interfaces);
                        let period_start = period_start_ms(cur.reset_day, chrono::Local::now());
                        let raw =
                            self.netstatic
                                .query(&filter, period_start, crate::model::now_millis());
                        self.netstatic
                            .apply_correction(period_start, raw, rx_gb, tx_gb);
                    }
                    // 响应不再带校正字段 = 服务端已确认清空，停止回传
                    _ => self.netstatic.clear_confirm(),
                }
            }
            Err(e) => {
                self.buffers.restore(dynamic, vec![], vec![]);
                tracing::warn!(error = %e, "上报失败，数据已保留待重发");
            }
        }
    }
}

/// 首次立即触发（启动即采集/上报）
fn ticker(secs: u64) -> Interval {
    interval(Duration::from_secs(secs.max(1)))
}

/// 从下一个周期开始触发（配置变更重建时不立即补一发）
fn ticker_from_next(secs: u64) -> Interval {
    let d = Duration::from_secs(secs.max(1));
    interval_at(tokio::time::Instant::now() + d, d)
}
