//! Independent collection and reporting schedulers.
//!
//! Collection writes a bounded shared event journal. Every Reporter owns a
//! cursor, report ticker, static cache and retry state, so one slow endpoint
//! cannot drain or block data needed by another endpoint.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::TimeZone;
use tokio::sync::watch;
use tokio::time::{interval, interval_at, Interval, MissedTickBehavior};

use crate::buffer::{BufferBatch, Buffers};
use crate::collector::{self, net, CpuMonitor};
use crate::config::{ReporterSpec, SharedConfig};
use crate::model::{
    AsyncRecord, DynamicRecord, ErrorRecord, GpuRecord, Intervals, NetInterfaceSample, PingRecord,
    Report, SelfRecord, SlowBlock, StaticInfo,
};
use crate::netstatic::{period_start_ms, NetStatic};
use crate::reporter::Reporter;
use crate::worker::komari::{KomariOut, TimedKomariReport};
use crate::worker::ping::PingSnapshot;
use crate::worker::public_ip::IpSnapshot;

const STATIC_REFRESH: Duration = Duration::from_secs(600);

/// Pure collection scheduler. Reporting is handled by `ReporterRunner`.
pub struct Scheduler {
    buffers: Arc<Buffers>,
    intervals_rx: watch::Receiver<Intervals>,
    ping_rx: watch::Receiver<PingSnapshot>,
    gpu_rx: watch::Receiver<Vec<GpuRecord>>,
    slow_rx: watch::Receiver<Option<SlowBlock>>,
    self_rx: watch::Receiver<Option<SelfRecord>>,
    diskio_rx: watch::Receiver<Option<crate::model::DiskIoRecord>>,
    shutdown_rx: watch::Receiver<bool>,
    cpu: CpuMonitor,
    prev_net: Option<(BTreeMap<String, net::NetBytes>, Instant)>,
    last_ping_ts: HashMap<String, i64>,
    last_gpu_ts: i64,
    last_slow_ts: i64,
    last_self_ts: i64,
    last_diskio_ts: i64,
}

impl Scheduler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        buffers: Arc<Buffers>,
        intervals_rx: watch::Receiver<Intervals>,
        ping_rx: watch::Receiver<PingSnapshot>,
        gpu_rx: watch::Receiver<Vec<GpuRecord>>,
        slow_rx: watch::Receiver<Option<SlowBlock>>,
        self_rx: watch::Receiver<Option<SelfRecord>>,
        diskio_rx: watch::Receiver<Option<crate::model::DiskIoRecord>>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            buffers,
            intervals_rx,
            ping_rx,
            gpu_rx,
            slow_rx,
            self_rx,
            diskio_rx,
            shutdown_rx,
            cpu: CpuMonitor::new(),
            prev_net: None,
            last_ping_ts: HashMap::new(),
            last_gpu_ts: 0,
            last_slow_ts: 0,
            last_self_ts: 0,
            last_diskio_ts: 0,
        }
    }

    pub async fn run(mut self) {
        let initial = *self.intervals_rx.borrow();
        self.on_collect();
        let mut collect_ticker = ticker_from_next(initial.collect);
        tracing::info!(collect = initial.collect, "collection scheduler started");
        loop {
            tokio::select! {
                _ = collect_ticker.tick() => self.on_collect(),
                changed = self.intervals_rx.changed() => {
                    if changed.is_err() { return; }
                    let collect = self.intervals_rx.borrow().collect;
                    collect_ticker = ticker_from_next(collect);
                    tracing::info!(collect, "collection ticker updated");
                }
                changed = self.shutdown_rx.changed() => {
                    if changed.is_err() || *self.shutdown_rx.borrow() {
                        tracing::info!("collection scheduler stopped");
                        return;
                    }
                }
            }
        }
    }

    fn on_collect(&mut self) {
        let now_ms = crate::model::now_millis();
        let cpu_usage = self.cpu.sample().map(|u| (u * 100.0).round() / 100.0);
        let (_, mem_used, _, swap_used) = collector::memory();

        // Capture every interface. Filtering is performed independently at
        // each Reporter outlet.
        let mut current = BTreeMap::new();
        collector::scan_net_dev(|name, rx, tx| {
            current.insert(name.to_string(), net::NetBytes { rx, tx });
        });
        let elapsed = self
            .prev_net
            .as_ref()
            .map(|(_, at)| at.elapsed().as_secs_f64());
        let mut net_interfaces = BTreeMap::new();
        for (name, counters) in &current {
            let (rx_speed, tx_speed) = match (&self.prev_net, elapsed) {
                (Some((previous, _)), Some(dt)) if dt > 0.0 => previous
                    .get(name)
                    .map(|old| {
                        (
                            (net::counter_delta(counters.rx, old.rx) as f64 / dt) as u64,
                            (net::counter_delta(counters.tx, old.tx) as f64 / dt) as u64,
                        )
                    })
                    .unwrap_or((0, 0)),
                _ => (0, 0),
            };
            net_interfaces.insert(
                name.clone(),
                NetInterfaceSample {
                    rx: counters.rx,
                    tx: counters.tx,
                    rx_speed,
                    tx_speed,
                    rx_monthly: None,
                    tx_monthly: None,
                },
            );
        }
        let net_rx = net_interfaces.values().map(|n| n.rx).sum();
        let net_tx = net_interfaces.values().map(|n| n.tx).sum();
        let net_rx_speed = elapsed.map(|_| net_interfaces.values().map(|n| n.rx_speed).sum());
        let net_tx_speed = elapsed.map(|_| net_interfaces.values().map(|n| n.tx_speed).sum());
        self.prev_net = Some((current, Instant::now()));

        self.buffers.push_dynamic(DynamicRecord {
            ts: now_ms,
            cpu_usage,
            mem_used: Some(mem_used),
            swap_used: Some(swap_used),
            load: collector::load(),
            net_rx: Some(net_rx),
            net_tx: Some(net_tx),
            net_rx_speed,
            net_tx_speed,
            net_rx_monthly: None,
            net_tx_monthly: None,
            net_interfaces,
        });

        let fresh_pings: Vec<PingRecord> = self
            .ping_rx
            .borrow()
            .values()
            .filter(|record| record.ts > *self.last_ping_ts.get(&record.name).unwrap_or(&0))
            .cloned()
            .collect();
        for record in fresh_pings {
            self.last_ping_ts.insert(record.name.clone(), record.ts);
            self.buffers.push_async(AsyncRecord::Ping(record));
        }

        let gpus = self.gpu_rx.borrow();
        let gpu_ts = gpus.first().map_or(0, |record| record.ts);
        if gpu_ts > self.last_gpu_ts {
            self.last_gpu_ts = gpu_ts;
            for record in gpus.iter().cloned() {
                self.buffers.push_async(AsyncRecord::Gpu(record));
            }
        }
        drop(gpus);

        if let Some(record) = self.slow_rx.borrow().clone() {
            if record.ts > self.last_slow_ts {
                self.last_slow_ts = record.ts;
                self.buffers.push_async(AsyncRecord::Slow(record));
            }
        }
        if let Some(record) = self.self_rx.borrow().clone() {
            if record.ts > self.last_self_ts {
                self.last_self_ts = record.ts;
                self.buffers.push_async(AsyncRecord::Self_(record));
            }
        }
        if let Some(record) = self.diskio_rx.borrow().clone() {
            if record.ts > self.last_diskio_ts {
                self.last_diskio_ts = record.ts;
                self.buffers.push_async(AsyncRecord::DiskIo(record));
            }
        }
    }
}

/// One independently scheduled reporting endpoint.
pub struct ReporterRunner {
    id: String,
    cfg: Arc<SharedConfig>,
    buffers: Arc<Buffers>,
    reporter: Arc<Reporter>,
    netstatic: NetStatic,
    config_rx: watch::Receiver<crate::config::LocalConfig>,
    ip_rx: watch::Receiver<IpSnapshot>,
    gpu_name_rx: watch::Receiver<Option<String>>,
    ping_rx: watch::Receiver<PingSnapshot>,
    gpu_rx: watch::Receiver<Vec<GpuRecord>>,
    slow_rx: watch::Receiver<Option<SlowBlock>>,
    diskio_rx: watch::Receiver<Option<crate::model::DiskIoRecord>>,
    shutdown_rx: watch::Receiver<bool>,
    agent_version: String,
    komari_tx: Option<watch::Sender<KomariOut>>,
    last_dynamic: Option<DynamicRecord>,
    last_static: Option<Instant>,
    static_cache: Option<StaticInfo>,
}

impl ReporterRunner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        cfg: Arc<SharedConfig>,
        buffers: Arc<Buffers>,
        reporter: Arc<Reporter>,
        netstatic: NetStatic,
        config_rx: watch::Receiver<crate::config::LocalConfig>,
        ip_rx: watch::Receiver<IpSnapshot>,
        gpu_name_rx: watch::Receiver<Option<String>>,
        ping_rx: watch::Receiver<PingSnapshot>,
        gpu_rx: watch::Receiver<Vec<GpuRecord>>,
        slow_rx: watch::Receiver<Option<SlowBlock>>,
        diskio_rx: watch::Receiver<Option<crate::model::DiskIoRecord>>,
        shutdown_rx: watch::Receiver<bool>,
        agent_version: String,
        komari_tx: Option<watch::Sender<KomariOut>>,
    ) -> Self {
        Self {
            id,
            cfg,
            buffers,
            reporter,
            netstatic,
            config_rx,
            ip_rx,
            gpu_name_rx,
            ping_rx,
            gpu_rx,
            slow_rx,
            diskio_rx,
            shutdown_rx,
            agent_version,
            komari_tx,
            last_dynamic: None,
            last_static: None,
            static_cache: None,
        }
    }

    pub async fn run(mut self) {
        let Some(initial) = self.cfg.get().reporter(&self.id) else {
            tracing::error!(reporter_id = %self.id, "reporter config missing at startup");
            return;
        };
        let mut report_interval = initial.intervals.report;
        let mut report_ticker = ticker(report_interval);
        tracing::info!(
            reporter_id = %self.id,
            protocol = %initial.protocol,
            report = initial.intervals.report,
            "reporter started"
        );
        loop {
            tokio::select! {
                _ = report_ticker.tick() => self.on_report().await,
                changed = self.config_rx.changed() => {
                    if changed.is_err() { return; }
                    let cfg = self.config_rx.borrow().clone();
                    let Some(spec) = cfg.reporter(&self.id) else {
                        tracing::warn!(reporter_id = %self.id, "reporter removal requires restart");
                        continue;
                    };
                    if report_interval_changed(&mut report_interval, spec.intervals.report) {
                        report_ticker = ticker_from_next(report_interval);
                    }
                    self.last_static = None;
                }
                changed = self.ip_rx.changed() => {
                    if changed.is_err() { return; }
                    // 每次成功测量都会刷新 IP 时间戳；是否需要重报 static
                    // 由下一个 report tick 比较过滤后的实际地址决定。
                }
                changed = self.gpu_name_rx.changed() => {
                    if changed.is_err() { return; }
                    self.last_static = None;
                }
                changed = self.shutdown_rx.changed() => {
                    if changed.is_err() || *self.shutdown_rx.borrow() {
                        tracing::info!(reporter_id = %self.id, "reporter stopped");
                        return;
                    }
                }
            }
        }
    }

    async fn on_report(&mut self) {
        let Some(spec) = self.cfg.get().reporter(&self.id) else {
            return;
        };
        let batch = self.buffers.read(&self.id);
        let dynamic = self.scope_dynamic_batch(&batch.dynamic, &spec);
        if let Some(latest) = dynamic.last() {
            self.last_dynamic = Some(latest.clone());
        }

        let success = match spec.protocol.as_str() {
            "cf" => self.report_cf(&spec, &dynamic).await,
            "komari" => self.report_komari(&spec, &dynamic, &batch).await,
            _ => self.report_probe(&spec, dynamic, &batch).await,
        };
        if success {
            self.buffers.ack(&self.id, batch.through);
        }
    }

    async fn report_probe(
        &mut self,
        spec: &ReporterSpec,
        dynamic: Vec<DynamicRecord>,
        batch: &BufferBatch,
    ) -> bool {
        let async_records = batch
            .async_records
            .iter()
            .flat_map(|record| match record {
                AsyncRecord::Ping(ping) => scope_ping_aliases(ping, spec)
                    .into_iter()
                    .map(AsyncRecord::Ping)
                    .collect(),
                AsyncRecord::Gpu(_) if spec.report_gpu => vec![record.clone()],
                AsyncRecord::Self_(_) if spec.report_self => vec![record.clone()],
                AsyncRecord::Slow(slow) => {
                    vec![AsyncRecord::Slow(self.scope_slow(slow, spec))]
                }
                AsyncRecord::DiskIo(diskio) => {
                    vec![AsyncRecord::DiskIo(self.scope_diskio(diskio, spec))]
                }
                AsyncRecord::Gpu(_) | AsyncRecord::Self_(_) => Vec::new(),
            })
            .collect();
        let errors = self.scope_errors(&batch.errors, spec);
        let include_static = self.static_due(spec);
        let static_info = include_static.then(|| self.refresh_static(spec));
        let report = Report {
            server_id: spec.server_id.clone(),
            config_version: spec.config_version.clone(),
            static_info,
            dynamic,
            async_records,
            errors,
        };
        match self.reporter.send(&report).await {
            Ok(action) => {
                if include_static {
                    self.last_static = Some(Instant::now());
                }
                if action.next_static {
                    self.last_static = None;
                }
                if let Some(remote) = action.config {
                    if let Err(error) = self.cfg.apply_remote_for(&self.id, remote) {
                        tracing::warn!(reporter_id = %self.id, %error, "remote config rejected");
                    } else {
                        self.last_static = None;
                    }
                }
                true
            }
            Err(error) => {
                self.last_static = None;
                self.report_error(error);
                false
            }
        }
    }

    async fn report_komari(
        &mut self,
        spec: &ReporterSpec,
        dynamic: &[DynamicRecord],
        batch: &BufferBatch,
    ) -> bool {
        let Some(tx) = self.komari_tx.clone() else {
            self.buffers.push_error(
                format!("reporter:{}", self.id),
                "komari output channel is unavailable",
            );
            return false;
        };
        let refreshed = self.static_due(spec);
        if refreshed {
            self.refresh_static(spec);
            self.last_static = Some(Instant::now());
        }
        let static_info = self
            .static_cache
            .clone()
            .unwrap_or_else(|| self.refresh_static(spec));
        let errors = self.scope_errors(&batch.errors, spec);
        let basic_info = refreshed
            .then(|| crate::reporter_komari::build_basic_info(&static_info, &self.agent_version));

        let now_ms = crate::model::now_millis();
        let Some(dyn_latest) = dynamic.last().or(self.last_dynamic.as_ref()) else {
            tracing::warn!(reporter_id = %self.id, "Komari report 缺少 dynamic 快照，已清空待发报告");
            publish_komari(&tx, None, basic_info);
            return true;
        };
        let Some(dynamic_valid_until) = snapshot_valid_until(
            dyn_latest.ts,
            spec.intervals.collect,
            spec.intervals.report,
            now_ms,
        ) else {
            tracing::warn!(
                reporter_id = %self.id,
                measured_at = dyn_latest.ts,
                "Komari dynamic 快照已过期，已清空待发报告"
            );
            publish_komari(&tx, None, basic_info);
            return true;
        };

        let slow = self.slow_rx.borrow();
        let Some(slow_record) = slow.as_ref() else {
            tracing::warn!(reporter_id = %self.id, "Komari report 缺少 slow 快照，已清空待发报告");
            publish_komari(&tx, None, basic_info);
            return true;
        };
        let Some(slow_valid_until) = snapshot_valid_until(
            slow_record.ts,
            spec.intervals.slow,
            spec.intervals.report,
            now_ms,
        ) else {
            tracing::warn!(
                reporter_id = %self.id,
                measured_at = slow_record.ts,
                "Komari slow 快照已过期，已清空待发报告"
            );
            publish_komari(&tx, None, basic_info);
            return true;
        };
        let scoped_slow = self.scope_slow(slow_record, spec);
        drop(slow);

        let gpus = self.gpu_rx.borrow();
        let mut report_valid_until = dynamic_valid_until.min(slow_valid_until);
        let fresh_gpus: Vec<GpuRecord> = if spec.report_gpu {
            gpus.iter()
                .filter_map(|gpu| {
                    snapshot_valid_until(gpu.ts, spec.intervals.gpu, spec.intervals.report, now_ms)
                        .map(|valid_until| {
                            report_valid_until = report_valid_until.min(valid_until);
                            gpu.clone()
                        })
                })
                .collect()
        } else {
            Vec::new()
        };
        if spec.report_gpu && !gpus.is_empty() && fresh_gpus.is_empty() {
            tracing::warn!(reporter_id = %self.id, "Komari GPU 快照已过期，本轮不携带 GPU 数据");
        }
        let report = crate::reporter_komari::build_report(
            &static_info,
            Some(dyn_latest),
            Some(&scoped_slow),
            &fresh_gpus,
            &errors,
            now_ms,
        );
        drop(gpus);
        publish_komari(
            &tx,
            Some(TimedKomariReport {
                payload: report,
                measured_at_ms: dyn_latest.ts,
                valid_until_ms: report_valid_until,
            }),
            basic_info,
        );
        true
    }

    async fn report_cf(&mut self, spec: &ReporterSpec, dynamic: &[DynamicRecord]) -> bool {
        if self.static_due(spec) {
            self.refresh_static(spec);
            self.last_static = Some(Instant::now());
        }
        let static_info = self
            .static_cache
            .clone()
            .unwrap_or_else(|| self.refresh_static(spec));
        let now_ms = crate::model::now_millis();
        let dyn_latest = dynamic
            .last()
            .or(self.last_dynamic.as_ref())
            .filter(|record| {
                snapshot_valid_until(
                    record.ts,
                    spec.intervals.collect,
                    spec.intervals.report,
                    now_ms,
                )
                .is_some()
            });
        let metrics = {
            let slow = self.slow_rx.borrow();
            let gpus = self.gpu_rx.borrow();
            let pings = self.ping_rx.borrow();
            let diskio = self.diskio_rx.borrow();
            let diskio = diskio
                .as_ref()
                .filter(|record| {
                    snapshot_valid_until(
                        record.ts,
                        spec.intervals.diskio,
                        spec.intervals.report,
                        now_ms,
                    )
                    .is_some()
                })
                .map(|record| self.scope_diskio(record, spec));
            let slow = slow
                .as_ref()
                .filter(|record| {
                    snapshot_valid_until(
                        record.ts,
                        spec.intervals.slow,
                        spec.intervals.report,
                        now_ms,
                    )
                    .is_some()
                })
                .map(|record| self.scope_slow(record, spec));
            let fresh_gpus: Vec<GpuRecord> = if spec.report_gpu {
                gpus.iter()
                    .filter(|record| {
                        snapshot_valid_until(
                            record.ts,
                            spec.intervals.gpu,
                            spec.intervals.report,
                            now_ms,
                        )
                        .is_some()
                    })
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            };
            let scoped_pings = scope_fresh_cf_pings(&pings, spec, now_ms);
            let ping_targets: Vec<_> = spec.pings.iter().map(|ping| ping.target.clone()).collect();
            crate::reporter_cf::build_metrics(
                &static_info,
                dyn_latest,
                slow.as_ref(),
                &fresh_gpus,
                &scoped_pings,
                diskio.as_ref(),
                &ping_targets,
            )
        };
        let samples = crate::reporter_cf::build_samples(dynamic, spec.ext.cf.batch);

        if let Some((rx_gb, tx_gb)) = self.netstatic.confirm_pending(&self.id) {
            if spec.ext.cf.correction {
                let confirm = crate::reporter_cf::CfConfirm {
                    id: spec.server_id.clone(),
                    secret: spec.secret.clone(),
                    rx_correction: rx_gb,
                    tx_correction: tx_gb,
                };
                match self.reporter.send_cf_confirm(&confirm).await {
                    Ok(()) => self.netstatic.clear_confirm(&self.id),
                    Err(error) => tracing::warn!(
                        reporter_id = %self.id,
                        %error,
                        "CF correction confirmation failed"
                    ),
                }
            } else {
                self.netstatic.clear_confirm(&self.id);
            }
        }

        let update = crate::reporter_cf::CfUpdate {
            id: spec.server_id.clone(),
            secret: spec.secret.clone(),
            metrics,
            samples,
        };
        match self.reporter.send_cf(&update, &spec.config_version).await {
            Ok(response) => {
                if let Some(push) = response.push {
                    let current_pings: Vec<_> =
                        spec.pings.iter().map(|ping| ping.target.clone()).collect();
                    let remote = crate::reporter_cf::synthesize_remote(
                        &push,
                        &spec.intervals,
                        &current_pings,
                    );
                    if let Err(error) = self.cfg.apply_remote_for(&self.id, remote) {
                        tracing::warn!(reporter_id = %self.id, %error, "CF config rejected");
                    } else {
                        self.last_static = None;
                    }
                }
                let current = self
                    .cfg
                    .get()
                    .reporter(&self.id)
                    .unwrap_or_else(|| spec.clone());
                match response.correction {
                    Some((rx_gb, tx_gb)) if current.ext.cf.correction => {
                        let filter = net::IfaceFilter::new(&current.interfaces);
                        let period_start = period_start_ms(current.reset_day, chrono::Local::now());
                        let raw =
                            self.netstatic
                                .query(&filter, period_start, crate::model::now_millis());
                        self.netstatic
                            .apply_correction(&self.id, period_start, raw, rx_gb, tx_gb);
                    }
                    _ => self.netstatic.clear_confirm(&self.id),
                }
                true
            }
            Err(error) => {
                self.report_error(error);
                false
            }
        }
    }

    fn scope_dynamic_batch(
        &self,
        records: &[DynamicRecord],
        spec: &ReporterSpec,
    ) -> Vec<DynamicRecord> {
        let filter = net::IfaceFilter::new(&spec.interfaces);
        let windows: Vec<_> = records
            .iter()
            .map(|record| {
                let at = chrono::Local
                    .timestamp_millis_opt(record.ts)
                    .single()
                    .unwrap_or_else(chrono::Local::now);
                (period_start_ms(spec.reset_day, at), record.ts)
            })
            .collect();
        let traffic = self.netstatic.query_batch(&self.id, &filter, &windows);

        records
            .iter()
            .zip(traffic)
            .map(|(record, traffic)| {
                let mut scoped = record.clone();
                if !record.net_interfaces.is_empty() {
                    let selected: BTreeMap<String, NetInterfaceSample> = record
                        .net_interfaces
                        .iter()
                        .filter(|(name, _)| filter.includes(name))
                        .map(|(name, sample)| {
                            let monthly = traffic.interfaces.get(name).copied().unwrap_or_default();
                            let mut sample = *sample;
                            sample.rx_monthly = Some(monthly.rx);
                            sample.tx_monthly = Some(monthly.tx);
                            (name.clone(), sample)
                        })
                        .collect();
                    scoped.net_rx = Some(selected.values().map(|sample| sample.rx).sum());
                    scoped.net_tx = Some(selected.values().map(|sample| sample.tx).sum());
                    if record.net_rx_speed.is_some() {
                        scoped.net_rx_speed =
                            Some(selected.values().map(|sample| sample.rx_speed).sum());
                        scoped.net_tx_speed =
                            Some(selected.values().map(|sample| sample.tx_speed).sum());
                    }
                    scoped.net_interfaces = selected;
                }
                scoped.net_rx_monthly = Some(traffic.total.rx);
                scoped.net_tx_monthly = Some(traffic.total.tx);
                scoped
            })
            .collect()
    }

    fn scope_slow(&self, record: &SlowBlock, spec: &ReporterSpec) -> SlowBlock {
        let mut scoped = record.clone();
        scoped.disks.retain(|disk| {
            disk_selected(
                &spec.disks,
                [
                    disk.id.as_str(),
                    disk.name.as_str(),
                    disk.mount_point.as_str(),
                ],
            )
        });
        if record.disk_used.is_some() {
            scoped.disk_used = Some(scoped.disks.iter().map(|disk| disk.used).sum());
        }
        scoped
    }

    fn scope_diskio(
        &self,
        record: &crate::model::DiskIoRecord,
        spec: &ReporterSpec,
    ) -> crate::model::DiskIoRecord {
        scope_diskio_record(record, &spec.disks)
    }

    fn scope_errors(
        &self,
        records: &[crate::model::ErrorRecord],
        spec: &ReporterSpec,
    ) -> Vec<crate::model::ErrorRecord> {
        if !spec.report_errors {
            return Vec::new();
        }
        records
            .iter()
            .flat_map(|record| {
                if let Some(task_id) = record.source.strip_prefix("ping:") {
                    return scope_ping_error_aliases(record, task_id, spec);
                }
                if let Some(reporter_id) = record.source.strip_prefix("reporter:") {
                    return if reporter_id == spec.id {
                        let mut scoped = record.clone();
                        scoped.source = "reporter".to_string();
                        vec![scoped]
                    } else {
                        Vec::new()
                    };
                }
                vec![record.clone()]
            })
            .collect()
    }

    fn static_due(&self, spec: &ReporterSpec) -> bool {
        if self
            .last_static
            .is_none_or(|last| last.elapsed() >= STATIC_REFRESH)
            || self.static_cache.is_none()
        {
            return true;
        }
        let current = self.ip_rx.borrow();
        let (ipv4, ipv6) = fresh_public_ips(
            &current,
            spec.intervals.ip,
            spec.intervals.report,
            crate::model::now_millis(),
        );
        self.static_cache
            .as_ref()
            .is_some_and(|cached| cached.ipv4 != ipv4 || cached.ipv6 != ipv6)
    }

    fn refresh_static(&mut self, spec: &ReporterSpec) -> StaticInfo {
        let current = self.ip_rx.borrow();
        let (ipv4, ipv6) = fresh_public_ips(
            &current,
            spec.intervals.ip,
            spec.intervals.report,
            crate::model::now_millis(),
        );
        drop(current);
        let gpu_name = if spec.report_gpu {
            self.gpu_name_rx.borrow().clone()
        } else {
            None
        };
        let config = self.config_rx.borrow().clone();
        let static_config =
            spec.static_config(config.global_summary(), config.reporter_summaries());
        let mut info =
            collector::static_info(ipv4, ipv6, gpu_name, &self.agent_version, &static_config);
        info.disks.retain(|disk| {
            disk_selected(
                &spec.disks,
                [
                    disk.id.as_str(),
                    disk.name.as_str(),
                    disk.mount_point.as_str(),
                ],
            )
        });
        info.disk_total = info.disks.iter().map(|disk| disk.total).sum();
        self.static_cache = Some(info.clone());
        info
    }

    fn report_error(&self, error: anyhow::Error) {
        self.buffers
            .push_error(format!("reporter:{}", self.id), error.to_string());
        tracing::warn!(reporter_id = %self.id, %error, "report failed; cursor retained");
    }
}

fn scope_diskio_record(
    record: &crate::model::DiskIoRecord,
    disk_patterns: &[String],
) -> crate::model::DiskIoRecord {
    let mut scoped = record.clone();
    if record.disks.is_empty() && disk_patterns.is_empty() {
        // macOS exposes aggregate counters without per-device records. With
        // no Reporter filter there is nothing to re-scope, so retain them.
        return scoped;
    }
    scoped
        .disks
        .retain(|disk| disk_selected(disk_patterns, [disk.name.as_str(), "", ""]));
    let sum = |pick: fn(&crate::model::DiskIoDeviceRecord) -> Option<f64>| {
        let values: Vec<_> = scoped.disks.iter().filter_map(pick).collect();
        (!values.is_empty()).then(|| values.into_iter().sum())
    };
    scoped.read_bps = sum(|disk| disk.read_bps);
    scoped.write_bps = sum(|disk| disk.write_bps);
    scoped.read_iops = sum(|disk| disk.read_iops);
    scoped.write_iops = sum(|disk| disk.write_iops);
    let (wait_total, ops_total) = scoped.disks.iter().fold((0.0, 0.0), |(wait, ops), disk| {
        let disk_ops = disk.read_iops.unwrap_or(0.0) + disk.write_iops.unwrap_or(0.0);
        (
            wait + disk.await_ms.unwrap_or(0.0) * disk_ops,
            ops + disk_ops,
        )
    });
    scoped.await_ms = (ops_total > 0.0).then_some(wait_total / ops_total);
    scoped.usage = scoped
        .disks
        .iter()
        .filter_map(|disk| disk.usage)
        .reduce(f64::max);
    scoped
}

fn fresh_public_ips(
    snapshot: &IpSnapshot,
    source_interval: u64,
    report_interval: u64,
    now_ms: i64,
) -> (Option<String>, Option<String>) {
    let fresh = |measurement: &crate::worker::public_ip::IpMeasurement| {
        snapshot_valid_until(
            measurement.measured_at_ms,
            source_interval,
            report_interval,
            now_ms,
        )
        .map(|_| measurement.address.clone())
    };
    (
        snapshot.ipv4.as_ref().and_then(fresh),
        snapshot.ipv6.as_ref().and_then(fresh),
    )
}

fn scope_ping_aliases(ping: &PingRecord, spec: &ReporterSpec) -> Vec<PingRecord> {
    spec.pings
        .iter()
        .filter(|target| target.task_id == ping.name)
        .map(|target| {
            let mut scoped = ping.clone();
            scoped.name.clone_from(&target.target.name);
            scoped
        })
        .collect()
}

fn scope_fresh_cf_pings(
    pings: &PingSnapshot,
    spec: &ReporterSpec,
    now_ms: i64,
) -> HashMap<String, PingRecord> {
    spec.pings
        .iter()
        .filter_map(|target| {
            let interval = target.target.interval.unwrap_or(spec.intervals.ping);
            pings
                .get(&target.task_id)
                .filter(|record| {
                    snapshot_valid_until(record.ts, interval, spec.intervals.report, now_ms)
                        .is_some()
                })
                .map(|ping| {
                    let mut scoped = ping.clone();
                    scoped.name.clone_from(&target.target.name);
                    (scoped.name.clone(), scoped)
                })
        })
        .collect()
}

fn scope_ping_error_aliases(
    record: &ErrorRecord,
    task_id: &str,
    spec: &ReporterSpec,
) -> Vec<ErrorRecord> {
    spec.pings
        .iter()
        .filter(|target| target.task_id == task_id)
        .map(|target| {
            let mut scoped = record.clone();
            scoped.source = format!("ping:{}", target.target.name);
            scoped
        })
        .collect()
}

fn disk_selected<'a>(patterns: &[String], values: impl IntoIterator<Item = &'a str>) -> bool {
    if patterns.is_empty() {
        return true;
    }
    let values: Vec<_> = values.into_iter().collect();
    patterns.iter().any(|pattern| {
        globset::Glob::new(pattern).is_ok_and(|glob| {
            let matcher = glob.compile_matcher();
            values.iter().any(|value| {
                matcher.is_match(value)
                    || value
                        .rsplit(['/', '\\'])
                        .next()
                        .is_some_and(|basename| matcher.is_match(basename))
            })
        })
    })
}

fn report_interval_changed(current: &mut u64, next: u64) -> bool {
    if *current == next {
        false
    } else {
        *current = next;
        true
    }
}

/// 更新 Komari 待发报告，同时持久保留最近一次 basicInfo，供断线重连使用。
fn publish_komari(
    tx: &watch::Sender<KomariOut>,
    report: Option<TimedKomariReport>,
    basic_info: Option<serde_json::Value>,
) {
    tx.send_modify(|out| {
        out.report = report;
        if let Some(info) = basic_info {
            out.basic_info = Some(info);
        }
    });
}

/// 计算快照有效截止时间。
///
/// 采集快于上报时不能因上报周期很长而放宽有效期；采集慢于上报时则允许
/// 在下一次采集前复用快照。额外两秒用于 ticker 抖动和任务切换。
fn snapshot_valid_until(
    measured_at_ms: i64,
    source_interval: u64,
    report_interval: u64,
    now_ms: i64,
) -> Option<i64> {
    const GRACE_SECS: u64 = 2;
    const GRACE_MS: i64 = (GRACE_SECS * 1_000) as i64;

    if measured_at_ms <= 0 || measured_at_ms > now_ms.saturating_add(GRACE_MS) {
        return None;
    }
    let source = source_interval.max(1);
    let report = report_interval.max(1);
    let max_age_secs = source
        .saturating_add(source.min(report))
        .saturating_add(GRACE_SECS);
    let max_age_ms = max_age_secs.saturating_mul(1_000).min(i64::MAX as u64) as i64;
    let valid_until = measured_at_ms.saturating_add(max_age_ms);
    (now_ms <= valid_until).then_some(valid_until)
}

/// First tick is immediate.
fn ticker(secs: u64) -> Interval {
    let mut ticker = interval(Duration::from_secs(secs.max(1)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker
}

/// Start at the next period (used when live config changes).
fn ticker_from_next(secs: u64) -> Interval {
    let duration = Duration::from_secs(secs.max(1));
    let mut ticker = interval_at(tokio::time::Instant::now() + duration, duration);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ScopedPingTarget;
    use crate::model::{PingKind, PingTarget};
    use crate::worker::public_ip::IpMeasurement;

    fn reporter_with_ping_aliases() -> ReporterSpec {
        let task_id = "tcp:example.com:80";
        let ping = |name: &str| ScopedPingTarget {
            task_id: task_id.into(),
            target: PingTarget {
                name: name.into(),
                kind: PingKind::Tcp,
                target: "example.com:80".into(),
                interval: Some(30),
            },
        };
        ReporterSpec {
            id: "primary".into(),
            protocol: "probe".into(),
            server_id: "server".into(),
            secret: "secret".into(),
            worker_url: "https://example.com/report".into(),
            config_version: String::new(),
            intervals: Intervals::default(),
            reset_day: 1,
            interfaces: vec![],
            disks: vec![],
            report_gpu: false,
            report_errors: true,
            report_self: false,
            pings: vec![ping("first"), ping("second")],
            ext: Default::default(),
        }
    }

    #[test]
    fn normalized_ping_results_and_errors_keep_all_logical_aliases() {
        let spec = reporter_with_ping_aliases();
        let ping = PingRecord {
            ts: 1,
            name: "tcp:example.com:80".into(),
            rtt: 12,
            loss: 0,
        };
        let names: Vec<_> = scope_ping_aliases(&ping, &spec)
            .into_iter()
            .map(|ping| ping.name)
            .collect();
        assert_eq!(names, ["first", "second"]);

        let error = ErrorRecord {
            ts: 1,
            source: "ping:tcp:example.com:80".into(),
            msg: "timeout".into(),
        };
        let sources: Vec<_> = scope_ping_error_aliases(&error, "tcp:example.com:80", &spec)
            .into_iter()
            .map(|error| error.source)
            .collect();
        assert_eq!(sources, ["ping:first", "ping:second"]);
    }

    #[test]
    fn cf_ping_scoping_drops_expired_cache_but_keeps_configured_aliases_fresh() {
        let spec = reporter_with_ping_aliases();
        let task_id = spec.pings[0].task_id.clone();
        let pings = HashMap::from([(
            task_id.clone(),
            PingRecord {
                ts: 10_000,
                name: task_id,
                rtt: 12,
                loss: 0,
            },
        )]);

        let fresh = scope_fresh_cf_pings(&pings, &spec, 72_000);
        assert_eq!(fresh.len(), 2);
        assert_eq!(fresh["first"].rtt, 12);
        assert_eq!(fresh["second"].rtt, 12);

        let expired = scope_fresh_cf_pings(&pings, &spec, 72_001);
        assert!(expired.is_empty());
    }

    #[test]
    fn report_interval_changes_only_when_value_differs() {
        let mut current = 30;
        assert!(!report_interval_changed(&mut current, 30));
        assert_eq!(current, 30);
        assert!(report_interval_changed(&mut current, 60));
        assert_eq!(current, 60);
    }

    #[test]
    fn komari_publication_keeps_basic_info_for_reconnects() {
        let (tx, rx) = watch::channel(KomariOut::default());
        publish_komari(&tx, None, Some(serde_json::json!({ "cpu_name": "test" })));
        publish_komari(
            &tx,
            Some(TimedKomariReport {
                payload: serde_json::json!({ "cpu": { "usage": 1 } }),
                measured_at_ms: 10_000,
                valid_until_ms: 14_000,
            }),
            None,
        );

        let current = rx.borrow();
        assert_eq!(current.basic_info.as_ref().unwrap()["cpu_name"], "test");
        assert!(current.report.is_some());
    }

    #[test]
    fn snapshot_freshness_does_not_expand_to_slow_report_interval() {
        let measured = 100_000;
        assert_eq!(
            snapshot_valid_until(measured, 1, 60, 104_000),
            Some(104_000)
        );
        assert_eq!(snapshot_valid_until(measured, 1, 60, 104_001), None);
    }

    #[test]
    fn snapshot_freshness_allows_slow_source_reuse_between_reports() {
        let measured = 100_000;
        assert_eq!(
            snapshot_valid_until(measured, 60, 3, 165_000),
            Some(165_000)
        );
        assert_eq!(snapshot_valid_until(measured, 60, 3, 165_001), None);
    }

    #[test]
    fn snapshot_freshness_rejects_invalid_or_future_timestamp() {
        assert_eq!(snapshot_valid_until(0, 1, 60, 100_000), None);
        assert_eq!(snapshot_valid_until(102_001, 1, 60, 100_000), None);
    }

    #[test]
    fn public_ip_freshness_expires_each_address_independently() {
        let snapshot = IpSnapshot {
            ipv4: Some(IpMeasurement {
                address: "192.0.2.1".into(),
                measured_at_ms: 100_000,
            }),
            ipv6: Some(IpMeasurement {
                address: "2001:db8::1".into(),
                measured_at_ms: 110_000,
            }),
        };
        let (ipv4, ipv6) = fresh_public_ips(&snapshot, 60, 3, 170_000);
        assert_eq!(ipv4, None);
        assert_eq!(ipv6.as_deref(), Some("2001:db8::1"));

        let (ipv4, ipv6) = fresh_public_ips(&snapshot, 60, 3, 175_001);
        assert_eq!((ipv4, ipv6), (None, None));
    }

    #[test]
    fn disk_filter_matches_linux_device_basename() {
        assert!(disk_selected(
            &["nvme*".into()],
            ["/dev/nvme0n1p1", "/mnt/data"]
        ));
        assert!(!disk_selected(
            &["sda*".into()],
            ["/dev/nvme0n1p1", "/mnt/data"]
        ));
    }

    #[test]
    fn aggregate_only_diskio_survives_without_a_filter() {
        let record = crate::model::DiskIoRecord {
            ts: 1,
            read_bps: Some(10.0),
            write_bps: Some(20.0),
            read_iops: Some(1.0),
            write_iops: Some(2.0),
            await_ms: Some(3.0),
            usage: None,
            disks: vec![],
        };
        let scoped = scope_diskio_record(&record, &[]);
        assert_eq!(scoped.read_bps, Some(10.0));
        assert_eq!(scoped.write_bps, Some(20.0));

        let filtered = scope_diskio_record(&record, &["disk0".into()]);
        assert_eq!(filtered.read_bps, None);
        assert_eq!(filtered.write_bps, None);
    }
}
