//! netstatic：网卡流量 delta 时序（移植 komari netstatic 设计）
//!
//! 月流量完全由客户端计算：上报时按 Reporter 批量查询各采样时间点。
//! 增量正确性纪律见 DESIGN.md §5.3。

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Datelike, Local, TimeZone};
use serde::{Deserialize, Serialize};

use crate::collector::net::{self, IfaceFilter, NetBytes};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(2);
const SAVE_INTERVAL: Duration = Duration::from_secs(600);
const RETAIN: chrono::Duration = chrono::Duration::days(31);

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Entry {
    /// 毫秒时间戳
    ts: i64,
    rx: u64,
    tx: u64,
}

/// 已移出 31 天明细窗口的永久累计。`through_ms` 用于避免历史查询把
/// 查询结束时间之后才归档的总量提前计入。
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct ArchivedTotal {
    through_ms: i64,
    rx: u64,
    tx: u64,
}

/// Most recent sampler clock observation. Persisting the domain lets
/// standalone maintenance commands use the same billing clock as the agent.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct LedgerTime {
    ts: i64,
    calibrated: bool,
}

/// 流量校正（CF 协议）：覆盖语义——当月累计 = 原始累计 + offset。
/// 账期翻页（period_start 不匹配）自动失效；confirm_pending 控制确认回传
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Correction {
    /// 所属账期起点（毫秒）
    pub period_start: i64,
    pub rx_offset: i64,
    pub tx_offset: i64,
    /// 收到的原始 GB 值（回传确认用，不换算）
    pub rx_gb: f64,
    pub tx_gb: f64,
    /// 是否还需向服务端回传确认（服务端清空待修正后置 false）
    pub confirm_pending: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StoreFile {
    interfaces: BTreeMap<String, VecDeque<Entry>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ledger_time: Option<LedgerTime>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    archived_totals: BTreeMap<String, ArchivedTotal>,
    #[serde(default)]
    corrections: BTreeMap<String, Correction>,
    /// Compatibility with the pre-multi-reporter on-disk format.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    correction: Option<Correction>,
}

fn sort_entries(entries: &mut VecDeque<Entry>) -> bool {
    if entries
        .iter()
        .zip(entries.iter().skip(1))
        .all(|(left, right)| left.ts <= right.ts)
    {
        return false;
    }
    entries.make_contiguous().sort_by_key(|entry| entry.ts);
    true
}

fn insert_entry(entries: &mut VecDeque<Entry>, entry: Entry) {
    if entries.back().is_none_or(|last| last.ts <= entry.ts) {
        entries.push_back(entry);
        return;
    }
    let index = entries.partition_point(|existing| existing.ts <= entry.ts);
    entries.insert(index, entry);
}

fn archive_before(store: &mut StoreFile, interface: &str, cutoff: i64) -> bool {
    let Some(entries) = store.interfaces.get_mut(interface) else {
        return false;
    };
    let mut archived = ArchivedTotal::default();
    let mut removed = false;
    while entries.front().is_some_and(|entry| entry.ts < cutoff) {
        let entry = entries.pop_front().expect("checked ledger entry");
        archived.through_ms = archived.through_ms.max(entry.ts);
        archived.rx = archived.rx.saturating_add(entry.rx);
        archived.tx = archived.tx.saturating_add(entry.tx);
        removed = true;
    }
    if removed {
        let total = store
            .archived_totals
            .entry(interface.to_string())
            .or_default();
        total.through_ms = total.through_ms.max(archived.through_ms);
        total.rx = total.rx.saturating_add(archived.rx);
        total.tx = total.tx.saturating_add(archived.tx);
    }
    removed
}

struct Inner {
    store: StoreFile,
    last_counters: HashMap<String, NetBytes>,
    last_save: Instant,
    dirty: bool,
}

#[derive(Clone)]
pub struct NetStatic {
    inner: Arc<Mutex<Inner>>,
    path: Arc<PathBuf>,
}

#[derive(Debug, Clone, Default)]
pub struct TrafficSnapshot {
    pub interfaces: BTreeMap<String, NetBytes>,
    pub total: NetBytes,
}

impl NetStatic {
    #[cfg(test)]
    pub fn load(path: &Path) -> Self {
        Self::load_with_legacy_reporter(path, None)
    }

    pub fn load_with_legacy_reporter(path: &Path, legacy_reporter_id: Option<&str>) -> Self {
        let store: StoreFile = std::fs::read_to_string(path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default();
        let mut store = store;
        let mut migrated = if let Some(reporter_id) = legacy_reporter_id {
            if let Some(legacy) = store.correction.take() {
                store
                    .corrections
                    .entry(reporter_id.to_string())
                    .or_insert(legacy);
                true
            } else {
                false
            }
        } else {
            false
        };
        // A previous process may have switched from a skewed local wall clock
        // to calibrated time. Normalize old ledgers before query_batch relies
        // on timestamp ordering. Retention is intentionally deferred until a
        // calibrated timestamp is supplied by the running sampler.
        for entries in store.interfaces.values_mut() {
            migrated |= sort_entries(entries);
        }
        Self {
            inner: Arc::new(Mutex::new(Inner {
                store,
                last_counters: HashMap::new(),
                last_save: Instant::now(),
                dirty: migrated,
            })),
            path: Arc::new(path.to_path_buf()),
        }
    }

    /// 读一次 /proc/net/dev，按网卡算 delta 并追加（由 sampler 每 2s 调用）
    pub fn sample(&self, filter: &IfaceFilter, now: i64, calibrated: bool) {
        let current = read_per_iface(filter);
        let mut inner = self.inner.lock().expect("netstatic lock poisoned");
        if calibrated
            && inner
                .store
                .ledger_time
                .is_none_or(|previous| previous.ts != now || !previous.calibrated)
        {
            inner.store.ledger_time = Some(LedgerTime {
                ts: now,
                calibrated: true,
            });
            inner.dirty = true;
        }
        for (name, counters) in current {
            let (rx_delta, tx_delta) = match inner.last_counters.insert(name.clone(), counters) {
                Some(prev) => (
                    net::counter_delta(counters.rx, prev.rx),
                    net::counter_delta(counters.tx, prev.tx),
                ),
                None => (0, 0),
            };
            if rx_delta == 0 && tx_delta == 0 {
                continue;
            }
            insert_entry(
                inner.store.interfaces.entry(name.clone()).or_default(),
                Entry {
                    ts: now,
                    rx: rx_delta,
                    tx: tx_delta,
                },
            );
            inner.dirty = true;
        }
        drop(inner);
        if calibrated {
            self.prune(now);
        }
    }

    fn prune(&self, now: i64) {
        let cutoff = now - RETAIN.num_milliseconds();
        let mut inner = self.inner.lock().expect("netstatic lock poisoned");
        let interfaces: Vec<_> = inner.store.interfaces.keys().cloned().collect();
        let mut archived = false;
        for interface in interfaces {
            archived |= archive_before(&mut inner.store, &interface, cutoff);
        }
        inner.dirty |= archived;
    }

    /// Latest persisted timestamp known to be in the calibrated ledger domain.
    pub fn calibrated_time(&self) -> Option<i64> {
        self.inner
            .lock()
            .expect("netstatic lock poisoned")
            .store
            .ledger_time
            .filter(|time| time.calibrated)
            .map(|time| time.ts)
    }

    /// 查询 [start_ms, now_ms] 窗口内白名单网卡的 (rx, tx) 合计（原始值，不含校正）
    pub fn query(&self, filter: &IfaceFilter, start_ms: i64, now_ms: i64) -> (u64, u64) {
        let inner = self.inner.lock().expect("netstatic lock poisoned");
        let mut rx = 0u64;
        let mut tx = 0u64;
        if start_ms <= 0 {
            for (name, total) in &inner.store.archived_totals {
                if filter.includes(name) && total.through_ms <= now_ms {
                    rx = rx.saturating_add(total.rx);
                    tx = tx.saturating_add(total.tx);
                }
            }
        }
        for (name, entries) in &inner.store.interfaces {
            if !filter.includes(name) {
                continue;
            }
            for e in entries {
                if e.ts >= start_ms && e.ts <= now_ms {
                    rx = rx.saturating_add(e.rx);
                    tx = tx.saturating_add(e.tx);
                }
            }
        }
        (rx, tx)
    }

    /// Query all monthly snapshots for one Reporter batch while holding the
    /// ledger lock once. Entries are scanned once per interface and distinct
    /// period start, rather than once per dynamic record and interface.
    pub fn query_batch(
        &self,
        reporter_id: &str,
        filter: &IfaceFilter,
        windows: &[(i64, i64)],
    ) -> Vec<TrafficSnapshot> {
        let inner = self.inner.lock().expect("netstatic lock poisoned");
        let mut snapshots = vec![TrafficSnapshot::default(); windows.len()];
        let mut groups: BTreeMap<i64, Vec<(i64, usize)>> = BTreeMap::new();
        for (index, &(start, end)) in windows.iter().enumerate() {
            groups.entry(start).or_default().push((end, index));
        }
        for points in groups.values_mut() {
            points.sort_unstable();
        }

        let interface_names: BTreeSet<_> = inner
            .store
            .interfaces
            .keys()
            .chain(inner.store.archived_totals.keys())
            .collect();
        for name in interface_names {
            if !filter.includes(name) {
                continue;
            }
            for (&start, points) in &groups {
                let mut entries = inner
                    .store
                    .interfaces
                    .get(name)
                    .into_iter()
                    .flatten()
                    .filter(|entry| entry.ts >= start)
                    .peekable();
                let archived = (start <= 0)
                    .then(|| inner.store.archived_totals.get(name))
                    .flatten();
                let mut archived_added = false;
                let mut total = NetBytes::default();
                for &(end, index) in points {
                    if !archived_added && archived.is_some_and(|value| value.through_ms <= end) {
                        let archived = archived.expect("checked archived total");
                        total.rx = total.rx.saturating_add(archived.rx);
                        total.tx = total.tx.saturating_add(archived.tx);
                        archived_added = true;
                    }
                    while entries.peek().is_some_and(|entry| entry.ts <= end) {
                        let entry = entries.next().expect("peeked ledger entry");
                        total.rx = total.rx.saturating_add(entry.rx);
                        total.tx = total.tx.saturating_add(entry.tx);
                    }
                    snapshots[index].interfaces.insert(name.clone(), total);
                    snapshots[index].total.rx = snapshots[index].total.rx.saturating_add(total.rx);
                    snapshots[index].total.tx = snapshots[index].total.tx.saturating_add(total.tx);
                }
            }
        }

        if let Some(correction) = inner.store.corrections.get(reporter_id).copied() {
            for (&(period_start, _), snapshot) in windows.iter().zip(&mut snapshots) {
                if correction.period_start == period_start {
                    snapshot.total.rx =
                        (snapshot.total.rx as i64 + correction.rx_offset).max(0) as u64;
                    snapshot.total.tx =
                        (snapshot.total.tx as i64 + correction.tx_offset).max(0) as u64;
                }
            }
        }
        snapshots
    }

    /// 月累计查询：原始值 + 校正偏移（偏移仅当属于当前账期时生效）
    #[cfg(test)]
    pub fn query_monthly(
        &self,
        reporter_id: &str,
        filter: &IfaceFilter,
        period_start: i64,
        now_ms: i64,
    ) -> (u64, u64) {
        let (raw_rx, raw_tx) = self.query(filter, period_start, now_ms);
        let inner = self.inner.lock().expect("netstatic lock poisoned");
        match inner.store.corrections.get(reporter_id).copied() {
            Some(c) if c.period_start == period_start => (
                (raw_rx as i64 + c.rx_offset).max(0) as u64,
                (raw_tx as i64 + c.tx_offset).max(0) as u64,
            ),
            _ => (raw_rx, raw_tx),
        }
    }

    /// 应用流量校正（覆盖语义）：offset = 校正字节数 − 当前原始月累计。
    /// GB 值原样保存供回传确认；立即落盘（重启不丢）
    pub fn apply_correction(
        &self,
        reporter_id: &str,
        period_start: i64,
        raw_monthly: (u64, u64),
        rx_gb: f64,
        tx_gb: f64,
    ) {
        self.store_correction(reporter_id, period_start, raw_monthly, rx_gb, tx_gb, true);
    }

    /// Apply an installer/operator supplied correction without scheduling a
    /// CF protocol acknowledgement. The server did not issue this correction,
    /// so presenting it as a remote-command confirmation would be incorrect.
    pub fn apply_local_correction(
        &self,
        reporter_id: &str,
        period_start: i64,
        raw_monthly: (u64, u64),
        rx_gb: f64,
        tx_gb: f64,
    ) {
        self.store_correction(reporter_id, period_start, raw_monthly, rx_gb, tx_gb, false);
    }

    fn store_correction(
        &self,
        reporter_id: &str,
        period_start: i64,
        raw_monthly: (u64, u64),
        rx_gb: f64,
        tx_gb: f64,
        confirm_pending: bool,
    ) {
        const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
        let rx_bytes = (rx_gb * GIB).round() as i64;
        let tx_bytes = (tx_gb * GIB).round() as i64;
        {
            let mut inner = self.inner.lock().expect("netstatic lock poisoned");
            inner.store.corrections.insert(
                reporter_id.to_string(),
                Correction {
                    period_start,
                    rx_offset: rx_bytes - raw_monthly.0 as i64,
                    tx_offset: tx_bytes - raw_monthly.1 as i64,
                    rx_gb,
                    tx_gb,
                    confirm_pending,
                },
            );
            inner.dirty = true;
        }
        self.flush();
        tracing::info!(reporter_id, rx_gb, tx_gb, confirm_pending, "流量校正已应用");
    }

    /// 待回传的校正确认值（GB 原值）
    pub fn confirm_pending(&self, reporter_id: &str) -> Option<(f64, f64)> {
        let inner = self.inner.lock().expect("netstatic lock poisoned");
        inner
            .store
            .corrections
            .get(reporter_id)
            .copied()
            .filter(|c| c.confirm_pending)
            .map(|c| (c.rx_gb, c.tx_gb))
    }

    /// 服务端已清空待修正（响应不再带校正字段）：停止回传；偏移保留到账期结束
    pub fn clear_confirm(&self, reporter_id: &str) {
        let need = {
            let mut inner = self.inner.lock().expect("netstatic lock poisoned");
            match inner.store.corrections.get_mut(reporter_id) {
                Some(c) if c.confirm_pending => {
                    c.confirm_pending = false;
                    inner.dirty = true;
                    true
                }
                _ => false,
            }
        };
        if need {
            self.flush();
        }
    }

    /// 到点（10min）或退出时落盘；tmp + rename 原子写
    pub fn flush_if_due(&self) {
        let should = {
            let inner = self.inner.lock().expect("netstatic lock poisoned");
            inner.dirty && inner.last_save.elapsed() >= SAVE_INTERVAL
        };
        if should {
            self.flush();
        }
    }

    pub fn flush(&self) {
        let data = {
            let mut inner = self.inner.lock().expect("netstatic lock poisoned");
            inner.last_save = Instant::now();
            inner.dirty = false;
            match serde_json::to_vec(&inner.store) {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(error = %e, "netstatic 序列化失败");
                    return;
                }
            }
        };
        let path = self.path.clone();
        let _ = std::fs::create_dir_all(path.parent().unwrap_or(Path::new(".")));
        let tmp = path.with_extension("json.tmp");
        if let Err(e) = std::fs::write(&tmp, data).and_then(|_| std::fs::rename(&tmp, &*path)) {
            tracing::warn!(error = %e, path = %path.display(), "netstatic 落盘失败");
        }
    }

    pub fn sample_interval(&self) -> Duration {
        SAMPLE_INTERVAL
    }
}

/// 读全部网卡计数器（经平台门面），白名单在采样时生效
fn read_per_iface(filter: &IfaceFilter) -> HashMap<String, NetBytes> {
    let mut out = HashMap::new();
    crate::collector::scan_net_dev(|name, rx, tx| {
        if filter.includes(name) {
            out.insert(name.to_string(), NetBytes { rx, tx });
        }
    });
    out
}

/// 账期起点（毫秒时间戳）；reset_day = 0 返回 0（永久累计）
pub fn period_start_ms(reset_day: u8, now: DateTime<Local>) -> i64 {
    if reset_day == 0 {
        return 0;
    }
    last_reset_date(now, reset_day).timestamp_millis()
}

fn last_reset_date(now: DateTime<Local>, reset_day: u8) -> DateTime<Local> {
    let this_month = actual_reset_date(now.year(), now.month(), reset_day, now.timezone());
    if now >= this_month {
        this_month
    } else {
        let (y, m) = if now.month() == 1 {
            (now.year() - 1, 12)
        } else {
            (now.year(), now.month() - 1)
        };
        actual_reset_date(y, m, reset_day, now.timezone())
    }
}

/// reset_day 超过当月天数时顺延到下月 1 号（移植 cfsm actualResetDate）
fn actual_reset_date<Tz: TimeZone>(year: i32, month: u32, reset_day: u8, tz: Tz) -> DateTime<Tz> {
    let last_day = last_day_of_month(year, month);
    if u32::from(reset_day) <= last_day {
        tz.with_ymd_and_hms(year, month, reset_day.into(), 0, 0, 0)
            .single()
            .unwrap_or_else(|| {
                tz.with_ymd_and_hms(year, month, reset_day.into(), 3, 0, 0)
                    .unwrap()
            })
    } else {
        let (y, m) = if month == 12 {
            (year + 1, 1)
        } else {
            (year, month + 1)
        };
        tz.with_ymd_and_hms(y, m, 1, 0, 0, 0).unwrap()
    }
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    let (y, m) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_next = chrono::NaiveDate::from_ymd_opt(y, m, 1).unwrap();
    first_next.pred_opt().unwrap().day()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(y: i32, m: u32, d: u32) -> DateTime<Local> {
        Local.with_ymd_and_hms(y, m, d, 12, 0, 0).unwrap()
    }

    #[test]
    fn period_normal() {
        let now = at(2026, 8, 5);
        let start = period_start_ms(1, now);
        assert_eq!(start, at(2026, 8, 1).timestamp_millis() - 12 * 3600 * 1000);
        let start15 = period_start_ms(15, now);
        // 8/5 还没到 8/15，上一个账期起点是 7/15
        let expect = Local.with_ymd_and_hms(2026, 7, 15, 0, 0, 0).unwrap();
        assert_eq!(start15, expect.timestamp_millis());
    }

    #[test]
    fn period_reset_day_31_in_30_day_month() {
        // 4 月只有 30 天：actual(2026-04, 31) 顺延到 5/1；now=4/20 < 5/1，取 actual(2026-03, 31)=3/31
        let now = at(2026, 4, 20);
        let start = period_start_ms(31, now);
        let expect = Local.with_ymd_and_hms(2026, 3, 31, 0, 0, 0).unwrap();
        assert_eq!(start, expect.timestamp_millis());
    }

    #[test]
    fn period_across_year() {
        let now = at(2026, 1, 10);
        let start = period_start_ms(15, now);
        let expect = Local.with_ymd_and_hms(2025, 12, 15, 0, 0, 0).unwrap();
        assert_eq!(start, expect.timestamp_millis());
    }

    #[test]
    fn period_zero_means_forever() {
        assert_eq!(period_start_ms(0, at(2026, 8, 5)), 0);
    }

    #[test]
    fn reset_zero_survives_detail_pruning_and_restart() {
        let (_, path) = tmp_ns("lifetime");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let now = crate::model::now_millis();
        let old = now - chrono::Duration::days(32).num_milliseconds();
        let recent = now - 1_000;
        let store = StoreFile {
            interfaces: BTreeMap::from([
                (
                    "eth0".into(),
                    [
                        Entry {
                            ts: old,
                            rx: 10,
                            tx: 20,
                        },
                        Entry {
                            ts: recent,
                            rx: 1,
                            tx: 2,
                        },
                    ]
                    .into(),
                ),
                (
                    "eth1".into(),
                    [Entry {
                        ts: old + 1,
                        rx: 100,
                        tx: 200,
                    }]
                    .into(),
                ),
            ]),
            ..Default::default()
        };
        std::fs::write(&path, serde_json::to_vec(&store).unwrap()).unwrap();

        let ns = NetStatic::load(&path);
        let all = IfaceFilter::new(&[]);
        let eth0 = IfaceFilter::new(&["eth0".into()]);
        let month_start = now - RETAIN.num_milliseconds();
        {
            let inner = ns.inner.lock().unwrap();
            assert_eq!(inner.store.interfaces["eth0"].len(), 2);
            assert!(inner.store.archived_totals.is_empty());
        }
        assert_eq!(ns.query(&all, 0, now), (111, 222));
        assert_eq!(ns.query(&eth0, 0, now), (11, 22));
        assert_eq!(ns.query(&all, month_start, now), (1, 2));

        // Loading cannot know whether the persisted ledger used local or
        // calibrated timestamps. Only an explicit calibrated observation may
        // advance retention.
        ns.prune(now);

        let snapshots = ns.query_batch("cf", &all, &[(0, old - 1), (0, now), (month_start, now)]);
        assert_eq!((snapshots[0].total.rx, snapshots[0].total.tx), (0, 0));
        assert_eq!((snapshots[1].total.rx, snapshots[1].total.tx), (111, 222));
        assert_eq!((snapshots[2].total.rx, snapshots[2].total.tx), (1, 2));
        assert_eq!(snapshots[1].interfaces["eth0"].rx, 11);
        assert_eq!(snapshots[1].interfaces["eth1"].rx, 100);

        ns.flush();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("archived_totals"));
        let reloaded = NetStatic::load(&path);
        assert_eq!(reloaded.query(&all, 0, now), (111, 222));
        assert_eq!(reloaded.query(&all, month_start, now), (1, 2));

        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    fn tmp_ns(tag: &str) -> (NetStatic, PathBuf) {
        let dir = std::env::temp_dir().join(format!("probe-rs-ns-{tag}-{}", std::process::id()));
        let path = dir.join("net.json");
        (NetStatic::load(&path), path)
    }

    #[test]
    fn batch_query_scans_a_series_without_losing_sample_time_semantics() {
        let (ns, path) = tmp_ns("batch");
        {
            let mut inner = ns.inner.lock().unwrap();
            inner.store.interfaces.insert(
                "eth0".into(),
                [
                    Entry {
                        ts: 10,
                        rx: 1,
                        tx: 10,
                    },
                    Entry {
                        ts: 20,
                        rx: 2,
                        tx: 20,
                    },
                    Entry {
                        ts: 30,
                        rx: 4,
                        tx: 40,
                    },
                ]
                .into(),
            );
            inner.store.interfaces.insert(
                "eth1".into(),
                [Entry {
                    ts: 15,
                    rx: 8,
                    tx: 80,
                }]
                .into(),
            );
        }
        let snapshots = ns.query_batch(
            "probe",
            &IfaceFilter::new(&[]),
            &[(0, 15), (0, 25), (20, 35)],
        );
        assert_eq!((snapshots[0].total.rx, snapshots[0].total.tx), (9, 90));
        assert_eq!((snapshots[1].total.rx, snapshots[1].total.tx), (11, 110));
        assert_eq!((snapshots[2].total.rx, snapshots[2].total.tx), (6, 60));
        assert_eq!(snapshots[1].interfaces["eth0"].rx, 3);
        assert_eq!(snapshots[1].interfaces["eth1"].rx, 8);

        {
            let mut inner = ns.inner.lock().unwrap();
            inner.store.corrections.insert(
                "cf".into(),
                Correction {
                    period_start: 0,
                    rx_offset: 100,
                    tx_offset: 200,
                    rx_gb: 0.0,
                    tx_gb: 0.0,
                    confirm_pending: false,
                },
            );
        }
        let corrected = ns.query_batch("cf", &IfaceFilter::new(&[]), &[(0, 25)]);
        assert_eq!((corrected[0].total.rx, corrected[0].total.tx), (111, 310));
        assert_eq!(corrected[0].interfaces["eth0"].rx, 3);
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn load_sorts_entries_after_a_clock_domain_reversal() {
        let (_, path) = tmp_ns("clock-reversal");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let store = StoreFile {
            interfaces: BTreeMap::from([(
                "eth0".into(),
                [
                    Entry {
                        ts: 300,
                        rx: 30,
                        tx: 300,
                    },
                    Entry {
                        ts: 100,
                        rx: 1,
                        tx: 10,
                    },
                    Entry {
                        ts: 200,
                        rx: 2,
                        tx: 20,
                    },
                ]
                .into(),
            )]),
            ..Default::default()
        };
        std::fs::write(&path, serde_json::to_vec(&store).unwrap()).unwrap();

        let ns = NetStatic::load(&path);
        let snapshots = ns.query_batch("cf", &IfaceFilter::new(&[]), &[(0, 150), (0, 250)]);
        assert_eq!((snapshots[0].total.rx, snapshots[0].total.tx), (1, 10));
        assert_eq!((snapshots[1].total.rx, snapshots[1].total.tx), (3, 30));
        let inner = ns.inner.lock().unwrap();
        assert_eq!(
            inner.store.interfaces["eth0"]
                .iter()
                .map(|entry| entry.ts)
                .collect::<Vec<_>>(),
            vec![100, 200, 300]
        );
        drop(inner);
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn insertion_keeps_calibrated_entries_ordered_after_future_local_entries() {
        let mut entries = VecDeque::from([Entry {
            ts: 300,
            rx: 30,
            tx: 300,
        }]);
        insert_entry(
            &mut entries,
            Entry {
                ts: 100,
                rx: 1,
                tx: 10,
            },
        );
        insert_entry(
            &mut entries,
            Entry {
                ts: 200,
                rx: 2,
                tx: 20,
            },
        );
        assert_eq!(
            entries.iter().map(|entry| entry.ts).collect::<Vec<_>>(),
            vec![100, 200, 300]
        );
    }

    #[test]
    fn legacy_correction_migrates_to_the_configured_cf_reporter() {
        let (_, path) = tmp_ns("legacy-correction");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let correction = Correction {
            period_start: 100,
            rx_offset: 1,
            tx_offset: 2,
            rx_gb: 3.0,
            tx_gb: 4.0,
            confirm_pending: true,
        };
        let store = StoreFile {
            correction: Some(correction),
            ..Default::default()
        };
        std::fs::write(&path, serde_json::to_vec(&store).unwrap()).unwrap();

        let ns = NetStatic::load_with_legacy_reporter(&path, Some("cf"));
        assert_eq!(ns.confirm_pending("cf"), Some((3.0, 4.0)));
        assert_eq!(ns.confirm_pending("primary"), None);
        let inner = ns.inner.lock().unwrap();
        assert!(inner.store.correction.is_none());
        assert!(inner.store.corrections.contains_key("cf"));
        drop(inner);
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn correction_overrides_monthly() {
        let (ns, path) = tmp_ns("corr");
        let filter = IfaceFilter::new(&[]);
        let period = period_start_ms(1, Local::now());
        let now = crate::model::now_millis();
        // 原始月累计 2 GB：直接注入时序条目
        {
            let mut inner = ns.inner.lock().unwrap();
            inner
                .store
                .interfaces
                .entry("eth0".into())
                .or_default()
                .push_back(Entry {
                    ts: now,
                    rx: 2 * 1024 * 1024 * 1024,
                    tx: 1024 * 1024 * 1024,
                });
        }
        // 校正为 rx=10GB tx=5GB（覆盖语义）
        ns.apply_correction(
            "primary",
            period,
            (2 * 1024 * 1024 * 1024, 1024 * 1024 * 1024),
            10.0,
            5.0,
        );
        let (rx, tx) = ns.query_monthly("primary", &filter, period, now);
        assert_eq!(rx, 10 * 1024 * 1024 * 1024);
        assert_eq!(tx, 5 * 1024 * 1024 * 1024);
        assert_eq!(ns.confirm_pending("primary"), Some((10.0, 5.0)));

        // A second CF Reporter owns a separate offset and confirmation cursor.
        ns.apply_correction(
            "cf-secondary",
            period,
            (2 * 1024 * 1024 * 1024, 1024 * 1024 * 1024),
            3.0,
            4.0,
        );
        assert_eq!(
            ns.query_monthly("cf-secondary", &filter, period, now),
            (3 * 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024)
        );
        assert_eq!(ns.confirm_pending("cf-secondary"), Some((3.0, 4.0)));
        ns.clear_confirm("primary");
        assert_eq!(ns.confirm_pending("primary"), None);
        assert_eq!(ns.confirm_pending("cf-secondary"), Some((3.0, 4.0)));
        ns.apply_correction(
            "primary",
            period,
            (2 * 1024 * 1024 * 1024, 1024 * 1024 * 1024),
            10.0,
            5.0,
        );

        // 落盘恢复：偏移与确认状态都在
        let ns2 = NetStatic::load(path.as_path());
        let (rx2, tx2) = ns2.query_monthly("primary", &filter, period, now);
        assert_eq!((rx2, tx2), (rx, tx));
        assert_eq!(ns2.confirm_pending("primary"), Some((10.0, 5.0)));

        // 服务端清空后停止回传，但偏移保留
        ns2.clear_confirm("primary");
        assert_eq!(ns2.confirm_pending("primary"), None);
        let (rx3, _) = ns2.query_monthly("primary", &filter, period, now);
        assert_eq!(rx3, rx);

        // 账期翻页：偏移失效，回到原始累计
        let next_period = period + 32i64 * 24 * 3600 * 1000;
        let (rx4, tx4) = ns2.query_monthly("primary", &filter, next_period, now);
        assert_eq!((rx4, tx4), (0, 0));

        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }
}
