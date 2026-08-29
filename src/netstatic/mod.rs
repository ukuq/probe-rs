//! netstatic：网卡流量 delta 时序（移植 komari netstatic 设计）
//!
//! 月流量完全由客户端计算：上报时按 Reporter 批量查询各采样时间点。
//! 增量正确性纪律见 DESIGN.md §5.3。

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Local, TimeZone};
use serde::{Deserialize, Serialize};

use crate::collector::net::{self, IfaceFilter, NetBytes};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(2);
const SAVE_INTERVAL: Duration = Duration::from_secs(600);
// 保留期必须严格大于最长账期(31 天),否则 reset_day 28-31 的账期首日
// 明细会在账期内被归档,而非永久累计的月查询不读归档基数,月流量永久少计。
const RETAIN: chrono::Duration = chrono::Duration::days(32);
/// 24h 内保留 2s 粒度明细；更老的条目在 prune 时合并为小时桶（桶 ts 取该
/// 小时首条）。桶是原子的：恰好跨账期起点的小时桶整体计入/排除，边界误差
/// 上限 1 小时流量且只影响 24h 以前的数据（CF 校正本身以 GB 取整）。
/// 没有它，繁忙网卡 32 天会累积 ~1.4M 条明细，内存与每次落盘都全量承受。
const FINE_RETAIN: chrono::Duration = chrono::Duration::hours(24);
const BUCKET_MS: i64 = 3_600_000;

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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StoreFile {
    interfaces: BTreeMap<String, VecDeque<Entry>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ledger_time: Option<LedgerTime>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    archived_totals: BTreeMap<String, ArchivedTotal>,
    /// 各网卡最近一次采样的绝对计数器。持久化后，agent 停机期间的流量
    /// 会在重启后的第一次采样计入 delta（机器重启导致计数器归零时
    /// saturating_sub 得 0，不会虚增）。
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    last_counters: BTreeMap<String, NetBytes>,
    /// Last observation time for each interface. This bounds persistent state
    /// from short-lived virtual interfaces without changing physical-interface
    /// accounting or eagerly deleting ledgers written by older versions.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    last_seen: BTreeMap<String, i64>,
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

/// 把 `cutoff` 之前的明细合并为小时桶（桶 ts = 该小时首条 ts）。条目按 ts
/// 有序，旧前缀逐小时累加；只在确实存在同小时多条时才重建。
fn coarsen_entries(entries: &mut VecDeque<Entry>, cutoff: i64) -> bool {
    let mut needs_merge = false;
    let mut previous_hour: Option<i64> = None;
    for entry in entries.iter() {
        if entry.ts >= cutoff {
            break;
        }
        let hour = entry.ts.div_euclid(BUCKET_MS);
        if previous_hour == Some(hour) {
            needs_merge = true;
            break;
        }
        previous_hour = Some(hour);
    }
    if !needs_merge {
        return false;
    }
    let mut buckets: VecDeque<Entry> = VecDeque::new();
    while entries.front().is_some_and(|entry| entry.ts < cutoff) {
        let entry = entries.pop_front().expect("checked front");
        match buckets.back_mut() {
            Some(bucket) if bucket.ts.div_euclid(BUCKET_MS) == entry.ts.div_euclid(BUCKET_MS) => {
                bucket.rx = bucket.rx.saturating_add(entry.rx);
                bucket.tx = bucket.tx.saturating_add(entry.tx);
            }
            _ => buckets.push_back(entry),
        }
    }
    while let Some(bucket) = buckets.pop_back() {
        entries.push_front(bucket);
    }
    true
}

/// 已按 ts 有序的队列中，首个 ts >= threshold 的下标（二分查找）。
fn lower_bound_ts(entries: &VecDeque<Entry>, threshold: i64) -> usize {
    let mut low = 0;
    let mut high = entries.len();
    while low < high {
        let mid = low + (high - low) / 2;
        if entries[mid].ts < threshold {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

struct Inner {
    store: StoreFile,
    last_save: Instant,
    dirty: bool,
    /// 每次标记 dirty 时递增;flush 成功且期间无新变更才允许清 dirty,
    /// 避免把"落盘期间产生的新数据"误标为已持久化。
    dirty_generation: u64,
}

fn mark_dirty(inner: &mut Inner) {
    inner.dirty = true;
    inner.dirty_generation = inner.dirty_generation.saturating_add(1);
}

#[derive(Clone)]
pub struct NetStatic {
    inner: Arc<Mutex<Inner>>,
    path: Arc<PathBuf>,
    /// 串行化 flush:采样 flush_if_due 与退出 flush 可能并发触发,
    /// 固定 tmp 名下的并发写会让旧快照覆盖新快照。
    flush_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Clone, Default)]
pub struct TrafficSnapshot {
    pub interfaces: BTreeMap<String, NetBytes>,
    pub total: NetBytes,
}

impl NetStatic {
    #[cfg(test)]
    pub fn load(path: &Path) -> Self {
        Self::load_with_legacy_reporter(path, None).expect("load test traffic ledger")
    }

    pub fn load_with_legacy_reporter(
        path: &Path,
        legacy_reporter_id: Option<&str>,
    ) -> Result<Self> {
        let store: StoreFile = match std::fs::read_to_string(path) {
            Ok(raw) => serde_json::from_str(&raw)
                .with_context(|| format!("parse traffic ledger {}", path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => StoreFile::default(),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read traffic ledger {}", path.display()));
            }
        };
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
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                store,
                last_save: Instant::now(),
                dirty: migrated,
                dirty_generation: 0,
            })),
            path: Arc::new(path.to_path_buf()),
            flush_lock: Arc::new(Mutex::new(())),
        })
    }

    /// 读一次 /proc/net/dev，按网卡算 delta 并追加（由 sampler 每 2s 调用）
    pub fn sample(&self, filter: &IfaceFilter, now: i64, calibrated: bool) {
        self.sample_counters(read_per_iface(filter), now, calibrated);
    }

    #[cfg(test)]
    fn sample_with<'a>(
        &self,
        _filter: &IfaceFilter,
        ifaces: impl IntoIterator<Item = (&'a str, u64, u64)>,
        now: i64,
        calibrated: bool,
    ) {
        let current = ifaces
            .into_iter()
            .map(|(name, rx, tx)| (name.to_string(), NetBytes { rx, tx }))
            .collect();
        self.sample_counters(current, now, calibrated);
    }

    fn sample_counters(&self, current: HashMap<String, NetBytes>, now: i64, calibrated: bool) {
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
            mark_dirty(&mut inner);
        }
        for (name, counters) in current {
            if inner.store.last_seen.insert(name.clone(), now) != Some(now) {
                mark_dirty(&mut inner);
            }
            let (rx_delta, tx_delta) =
                match inner.store.last_counters.insert(name.clone(), counters) {
                    Some(prev) if prev != counters => {
                        mark_dirty(&mut inner);
                        (
                            net::counter_delta(counters.rx, prev.rx),
                            net::counter_delta(counters.tx, prev.tx),
                        )
                    }
                    Some(_) => (0, 0),
                    // 新网卡/新账本的计数器基线同样需要持久化,否则停机流量
                    // 捕获的基线会在崩溃时丢失。
                    None => {
                        mark_dirty(&mut inner);
                        (0, 0)
                    }
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
            mark_dirty(&mut inner);
        }
        // 校准时钟与本地时钟混用会污染归档边界：只在采样时钟与账本时钟
        // 同域时清理——已校准，或账本从未见过校准时间（全部条目都是本地时钟）。
        let can_prune = calibrated || inner.store.ledger_time.is_none_or(|time| !time.calibrated);
        drop(inner);
        if can_prune {
            self.prune(now);
        }
    }

    fn prune(&self, now: i64) {
        let cutoff = now - RETAIN.num_milliseconds();
        let fine_cutoff = now - FINE_RETAIN.num_milliseconds();
        let mut inner = self.inner.lock().expect("netstatic lock poisoned");
        let interfaces: Vec<_> = inner.store.interfaces.keys().cloned().collect();
        let mut archived = false;
        let mut coarsened = false;
        for interface in interfaces {
            archived |= archive_before(&mut inner.store, &interface, cutoff);
            if let Some(entries) = inner.store.interfaces.get_mut(&interface) {
                coarsened |= coarsen_entries(entries, fine_cutoff);
            }
        }
        // Default-excluded virtual adapters are often ephemeral (containers,
        // VPNs and bridges). Once absent for a full retention window, their
        // per-interface history no longer contributes to normal reports and
        // must not grow the on-disk maps forever. Interfaces without a
        // last_seen value came from an older schema and are kept until they
        // have first been observed by this version.
        let stale_virtual: Vec<_> = inner
            .store
            .last_seen
            .iter()
            .filter(|(name, seen)| **seen < cutoff && net::is_default_excluded(name))
            .map(|(name, _)| name.clone())
            .collect();
        for interface in &stale_virtual {
            inner.store.interfaces.remove(interface);
            inner.store.archived_totals.remove(interface);
            inner.store.last_counters.remove(interface);
            inner.store.last_seen.remove(interface);
        }
        if archived || coarsened || !stale_virtual.is_empty() {
            mark_dirty(&mut inner);
        }
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
            // 条目按 ts 有序：二分定位窗口起点，避免整表线性扫描。
            for entry in entries.range(lower_bound_ts(entries, start_ms)..) {
                if entry.ts > now_ms {
                    break;
                }
                rx = rx.saturating_add(entry.rx);
                tx = tx.saturating_add(entry.tx);
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
                    .flat_map(|list| list.range(lower_bound_ts(list, start)..))
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
        // 幂等:相同 reporter + period_start + GB 原值视为同一命令。若按已经
        // 增长的 raw 累计重算 offset,校正后的显示值会被拉回服务端目标值,
        // 期间新增流量被暂时吞掉;重发只应恢复确认状态。
        let same_command = {
            let inner = self.inner.lock().expect("netstatic lock poisoned");
            inner.store.corrections.get(reporter_id).is_some_and(|c| {
                c.period_start == period_start && c.rx_gb == rx_gb && c.tx_gb == tx_gb
            })
        };
        if same_command {
            let changed = {
                let mut inner = self.inner.lock().expect("netstatic lock poisoned");
                let correction = inner
                    .store
                    .corrections
                    .get_mut(reporter_id)
                    .expect("checked correction exists");
                let changed = !correction.confirm_pending && confirm_pending;
                if confirm_pending {
                    correction.confirm_pending = true;
                    mark_dirty(&mut inner);
                }
                changed
            };
            if changed {
                self.flush();
            }
            tracing::info!(
                reporter_id,
                rx_gb,
                tx_gb,
                "相同校正命令,已恢复确认状态,offset 保持不变"
            );
            return;
        }

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
            mark_dirty(&mut inner);
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
                    mark_dirty(&mut inner);
                    true
                }
                _ => false,
            }
        };
        if need {
            self.flush();
        }
    }

    /// apply_correction 的 runtime 外版本：落盘含 fsync 的全量写，调用方在
    /// 异步上下文时应使用它，避免阻塞 runtime worker 线程。
    pub async fn apply_correction_off_runtime(
        &self,
        reporter_id: &str,
        period_start: i64,
        raw_monthly: (u64, u64),
        rx_gb: f64,
        tx_gb: f64,
    ) {
        let net = self.clone();
        let reporter_id = reporter_id.to_string();
        let result = tokio::task::spawn_blocking(move || {
            net.apply_correction(&reporter_id, period_start, raw_monthly, rx_gb, tx_gb)
        })
        .await;
        if let Err(error) = result {
            tracing::error!(%error, "netstatic apply_correction 后台任务 panic");
        }
    }

    /// clear_confirm 的 runtime 外版本，同 [`Self::apply_correction_off_runtime`]。
    pub async fn clear_confirm_off_runtime(&self, reporter_id: &str) {
        let net = self.clone();
        let reporter_id = reporter_id.to_string();
        let result = tokio::task::spawn_blocking(move || net.clear_confirm(&reporter_id)).await;
        if let Err(error) = result {
            tracing::error!(%error, "netstatic clear_confirm 后台任务 panic");
        }
    }

    /// 到点（10min）或退出时落盘；tmp + rename + fsync 原子写
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
        let _guard = self
            .flush_lock
            .lock()
            .expect("netstatic flush lock poisoned");
        // 锁内只做快照(clone 是连续内存拷贝,远快于序列化),序列化与写盘
        // 都在锁外——期间 reporter 的 query/query_batch 最多等一次拷贝。
        let (store, generation) = {
            let inner = self.inner.lock().expect("netstatic lock poisoned");
            (inner.store.clone(), inner.dirty_generation)
        };
        let data = match serde_json::to_vec(&store) {
            Ok(data) => data,
            Err(error) => {
                // 序列化失败不清 dirty、不推进 last_save,下次 flush 重试。
                tracing::warn!(error = %error, "netstatic 序列化失败");
                return;
            }
        };
        let path = self.path.clone();
        let write_result = std::fs::create_dir_all(path.parent().unwrap_or(Path::new(".")))
            .and_then(|_| {
                let tmp = path.with_extension(format!("json.{}.tmp", std::process::id()));
                write_durable(&tmp, &data).and_then(|_| std::fs::rename(&tmp, &*path))
            });
        if let Err(error) = write_result {
            // 保留 dirty 与旧 last_save,flush_if_due / 退出 flush 会重试。
            tracing::warn!(
                error = %error,
                path = %path.display(),
                "netstatic 落盘失败(保留 dirty 待重试)"
            );
            return;
        }
        sync_parent_dir(&path);
        let mut inner = self.inner.lock().expect("netstatic lock poisoned");
        inner.last_save = Instant::now();
        // 落盘期间新产生的变更已推进 generation;只有快照仍是最新时才可清 dirty,
        // 否则新数据会等到下一个 SAVE_INTERVAL 才被持久化。
        if inner.dirty_generation == generation {
            inner.dirty = false;
        }
    }

    pub fn sample_interval(&self) -> Duration {
        SAMPLE_INTERVAL
    }
}

/// 写入并 fsync：rename 只保证进程崩溃下的原子替换；没有 fsync，断电后
/// 可能留下 0 字节/半截文件，而 load 对损坏文件会静默清零整个账本。
fn write_durable(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    file.write_all(data)?;
    file.sync_all()
}

/// rename 的目录项也要落盘,否则断电时 rename 本身可能丢失。
#[cfg(unix)]
fn sync_parent_dir(path: &Path) {
    let Some(parent) = path.parent() else { return };
    if let Ok(dir) = std::fs::File::open(parent) {
        let _ = dir.sync_all();
    }
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) {}

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
        let old = now - chrono::Duration::days(33).num_milliseconds();
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

    #[test]
    fn prune_coarsens_old_entries_into_hourly_buckets_without_losing_totals() {
        let (ns, path) = tmp_ns("coarsen");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        // 小时对齐的起点，取 26h 前：两小时明细整体落在 24h 粗化窗口之外。
        let base = 472_223_i64 * 3_600_000;
        // i=0 只建立计数器基线；i=1..=3600 每条 delta 1000，明细共两小时。
        for i in 0..=3600u64 {
            let ts = base + (i as i64) * 2_000;
            ns.sample_with(&IfaceFilter::all(), [("eth0", i * 1000, 0)], ts, false);
        }
        let now = base + 26 * 3_600_000;
        ns.sample_with(
            &IfaceFilter::all(),
            [("eth0", 3_600_000 + 999_999, 7)],
            now,
            false,
        );

        {
            let inner = ns.inner.lock().unwrap();
            let entries = &inner.store.interfaces["eth0"];
            // 2 个小时桶 + 1 条恰好落在粗化边界上的明细 + 1 条新明细，
            // 而不是 3600+ 条。
            assert_eq!(entries.len(), 4, "entries not coarsened");
        }
        // 总量守恒：3600 条 ×1000 + 最后一条 delta。
        let all = IfaceFilter::new(&[]);
        assert_eq!(ns.query(&all, base, now), (3_600 * 1000 + 999_999, 7));
        // 从首个桶的 ts 起查，整个桶计入。
        assert_eq!(ns.query(&all, base + 2_000, now).0, 3_600 * 1000 + 999_999);

        ns.flush();
        let reloaded = NetStatic::load(&path);
        assert_eq!(reloaded.query(&all, base, now), (3_600 * 1000 + 999_999, 7));
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    fn tmp_ns(tag: &str) -> (NetStatic, PathBuf) {
        let dir = std::env::temp_dir().join(format!("probe-rs-ns-{tag}-{}", std::process::id()));
        let path = dir.join("net.json");
        (NetStatic::load(&path), path)
    }

    #[test]
    fn malformed_existing_ledger_is_reported_and_preserved() {
        let (_, path) = tmp_ns("malformed");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let malformed = b"{ definitely-not-json";
        std::fs::write(&path, malformed).unwrap();

        let result = NetStatic::load_with_legacy_reporter(&path, None);
        assert!(
            result.is_err(),
            "a corrupt ledger must not look like an empty one"
        );
        assert_eq!(std::fs::read(&path).unwrap(), malformed);
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn stale_default_excluded_interfaces_are_reaped_after_retention() {
        let (ns, path) = tmp_ns("stale-virtual");
        let start = 1_700_000_000_000_i64;
        let all = IfaceFilter::all();
        ns.sample_with(
            &all,
            [("veth-dead", 100, 200), ("eth0", 100, 200)],
            start,
            false,
        );
        ns.sample_with(
            &all,
            [("veth-dead", 110, 220), ("eth0", 110, 220)],
            start + 1_000,
            false,
        );

        ns.sample_with(
            &all,
            std::iter::empty::<(&str, u64, u64)>(),
            start + 1_001 + RETAIN.num_milliseconds(),
            false,
        );

        let inner = ns.inner.lock().unwrap();
        assert!(!inner.store.interfaces.contains_key("veth-dead"));
        assert!(!inner.store.archived_totals.contains_key("veth-dead"));
        assert!(!inner.store.last_counters.contains_key("veth-dead"));
        assert!(!inner.store.last_seen.contains_key("veth-dead"));
        assert!(inner.store.archived_totals.contains_key("eth0"));
        assert!(inner.store.last_counters.contains_key("eth0"));
        assert!(inner.store.last_seen.contains_key("eth0"));
        drop(inner);
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn persisted_counters_capture_downtime_traffic_after_restart() {
        let (_, path) = tmp_ns("downtime");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let store = StoreFile {
            last_counters: BTreeMap::from([("eth0".into(), NetBytes { rx: 1000, tx: 2000 })]),
            ..Default::default()
        };
        std::fs::write(&path, serde_json::to_vec(&store).unwrap()).unwrap();

        let ns = NetStatic::load(&path);
        // 重启后第一次采样：计数器从 1000/2000 涨到 1600/2300（停机期间的
        // 流量），delta 应计入账本而不是记 (0,0)。
        ns.sample_with(
            &IfaceFilter::new(&[]),
            [("eth0", 1600, 2300)],
            10_000,
            false,
        );
        assert_eq!(ns.query(&IfaceFilter::new(&[]), 0, 10_000), (600, 300));

        // 机器重启导致计数器归零：saturating_sub 得 0，不虚增。
        ns.sample_with(&IfaceFilter::new(&[]), [("eth0", 50, 60)], 20_000, false);
        assert_eq!(ns.query(&IfaceFilter::new(&[]), 0, 20_000), (600, 300));

        ns.flush();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("last_counters"));
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn uncalibrated_sampler_prunes_when_ledger_never_calibrated() {
        let (ns, path) = tmp_ns("uncalibrated-prune");
        let now = crate::model::now_millis();
        let old = now - RETAIN.num_milliseconds() - 1_000;
        {
            let mut inner = ns.inner.lock().unwrap();
            inner.store.interfaces.insert(
                "eth0".into(),
                [Entry {
                    ts: old,
                    rx: 7,
                    tx: 8,
                }]
                .into(),
            );
        }
        // 从未校准的账本：未校准采样也应推进 retention（同一时钟域）。
        ns.sample_with(&IfaceFilter::new(&[]), [("eth0", 1, 1)], now, false);
        let inner = ns.inner.lock().unwrap();
        assert!(inner.store.interfaces["eth0"].is_empty());
        assert_eq!(inner.store.archived_totals["eth0"].rx, 7);
        drop(inner);

        // 账本带校准标记后，未校准采样不再推进 retention（时钟域不明）。
        let (ns2, _path2) = (NetStatic::load(&path), &path);
        ns2.sample_with(&IfaceFilter::new(&[]), [("eth0", 2, 2)], now + 1_000, true);
        let old2 = now - RETAIN.num_milliseconds() - 500;
        {
            let mut inner = ns2.inner.lock().unwrap();
            inner.store.interfaces.insert(
                "eth1".into(),
                [Entry {
                    ts: old2,
                    rx: 9,
                    tx: 9,
                }]
                .into(),
            );
        }
        ns2.sample_with(&IfaceFilter::new(&[]), [("eth0", 3, 3)], now + 2_000, false);
        let inner = ns2.inner.lock().unwrap();
        assert_eq!(inner.store.interfaces["eth1"].len(), 1);
        drop(inner);
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn retention_window_keeps_the_first_day_of_a_31_day_period() {
        let (ns, path) = tmp_ns("retain-32d");
        let now = crate::model::now_millis();
        let first_day = now - chrono::Duration::days(31).num_milliseconds();
        {
            let mut inner = ns.inner.lock().unwrap();
            inner.store.interfaces.insert(
                "eth0".into(),
                [Entry {
                    ts: first_day,
                    rx: 100,
                    tx: 200,
                }]
                .into(),
            );
        }
        // 32 天保留期下,31 天前的账期首日仍在明细窗口内,不被归档。
        ns.sample_with(&IfaceFilter::new(&[]), [("eth0", 1, 1)], now, false);
        {
            let inner = ns.inner.lock().unwrap();
            assert_eq!(inner.store.interfaces["eth0"].len(), 1);
            assert!(
                !inner.store.archived_totals.contains_key("eth0"),
                "31 天账期的首日明细不得在 32 天保留期内被归档"
            );
        }
        // 非永久累计查询(账期起点 = 31 天前)必须能看到这一条。
        let (rx, tx) = ns.query(&IfaceFilter::new(&[]), first_day, now);
        assert_eq!((rx, tx), (100, 200));
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn flush_failure_keeps_dirty_and_recovers() {
        let dir =
            std::env::temp_dir().join(format!("probe-rs-ns-flushfail-{}", std::process::id()));
        let blocker = dir.join("blocker");
        // 用普通文件占住目录位置,使 create_dir_all 失败。
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&blocker, b"x").unwrap();
        let path = blocker.join("net.json");
        let ns = NetStatic::load(&path);
        ns.sample_with(&IfaceFilter::new(&[]), [("eth0", 100, 200)], 10_000, false);
        assert!(ns.inner.lock().unwrap().dirty);
        ns.flush();
        assert!(
            ns.inner.lock().unwrap().dirty,
            "落盘失败后 dirty 必须保留,等待重试"
        );
        assert!(!path.exists());
        // 解除阻塞后重试成功,dirty 清空。
        std::fs::remove_file(&blocker).unwrap();
        ns.flush();
        assert!(!ns.inner.lock().unwrap().dirty);
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("last_counters"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn repeated_identical_correction_does_not_recompute_offset() {
        let (ns, path) = tmp_ns("correction-idem");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        ns.apply_correction("cf", 1_000, (1_000_000, 2_000_000), 1.0, 2.0);
        let first = {
            let inner = ns.inner.lock().unwrap();
            let c = inner.store.corrections["cf"];
            (c.rx_offset, c.tx_offset, c.confirm_pending)
        };
        assert!(first.2);
        // 期间新增流量:raw 累计增长,服务端重发同一命令。
        ns.apply_correction("cf", 1_000, (5_000_000, 6_000_000), 1.0, 2.0);
        let second = {
            let inner = ns.inner.lock().unwrap();
            let c = inner.store.corrections["cf"];
            (c.rx_offset, c.tx_offset, c.confirm_pending)
        };
        assert_eq!(first, second, "相同命令不得按增长的 raw 累计重算 offset");
        // 本地同值校正(confirm_pending=false)不改变确认状态,也不重算。
        ns.apply_local_correction("cf", 1_000, (9_000_000, 9_000_000), 1.0, 2.0);
        let third = {
            let inner = ns.inner.lock().unwrap();
            let c = inner.store.corrections["cf"];
            (c.rx_offset, c.tx_offset, c.confirm_pending)
        };
        assert_eq!(first, third);
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
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

        let ns = NetStatic::load_with_legacy_reporter(&path, Some("cf")).unwrap();
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
