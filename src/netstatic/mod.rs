//! netstatic：网卡流量 delta 时序（移植 komari netstatic 设计）
//!
//! 月流量完全由客户端计算：上报时现查 `query(period_start, now)`。
//! 增量正确性纪律见 DESIGN.md §5.3。

use std::collections::{BTreeMap, HashMap, VecDeque};
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
    #[serde(default)]
    correction: Option<Correction>,
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

impl NetStatic {
    pub fn load(path: &Path) -> Self {
        let store: StoreFile = std::fs::read_to_string(path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default();
        let cutoff = crate::model::now_millis() - RETAIN.num_milliseconds();
        let mut store = store;
        for entries in store.interfaces.values_mut() {
            while entries.front().is_some_and(|e| e.ts < cutoff) {
                entries.pop_front();
            }
        }
        Self {
            inner: Arc::new(Mutex::new(Inner {
                store,
                last_counters: HashMap::new(),
                last_save: Instant::now(),
                dirty: false,
            })),
            path: Arc::new(path.to_path_buf()),
        }
    }

    /// 读一次 /proc/net/dev，按网卡算 delta 并追加（由 sampler 每 2s 调用）
    pub fn sample(&self, filter: &IfaceFilter) {
        let now = crate::model::now_millis();
        let current = read_per_iface(filter);
        let cutoff = now - RETAIN.num_milliseconds();
        let mut inner = self.inner.lock().expect("netstatic lock poisoned");
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
            let entries = inner.store.interfaces.entry(name).or_default();
            entries.push_back(Entry { ts: now, rx: rx_delta, tx: tx_delta });
            while entries.front().is_some_and(|e| e.ts < cutoff) {
                entries.pop_front();
            }
            inner.dirty = true;
        }
    }

    /// 查询 [start_ms, now_ms] 窗口内白名单网卡的 (rx, tx) 合计（原始值，不含校正）
    pub fn query(&self, filter: &IfaceFilter, start_ms: i64, now_ms: i64) -> (u64, u64) {
        let inner = self.inner.lock().expect("netstatic lock poisoned");
        let mut rx = 0u64;
        let mut tx = 0u64;
        for (name, entries) in &inner.store.interfaces {
            if !filter.includes(name) {
                continue;
            }
            for e in entries {
                if e.ts >= start_ms && e.ts <= now_ms {
                    rx += e.rx;
                    tx += e.tx;
                }
            }
        }
        (rx, tx)
    }

    /// 月累计查询：原始值 + 校正偏移（偏移仅当属于当前账期时生效）
    pub fn query_monthly(&self, filter: &IfaceFilter, period_start: i64, now_ms: i64) -> (u64, u64) {
        let (raw_rx, raw_tx) = self.query(filter, period_start, now_ms);
        let inner = self.inner.lock().expect("netstatic lock poisoned");
        match inner.store.correction {
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
        period_start: i64,
        raw_monthly: (u64, u64),
        rx_gb: f64,
        tx_gb: f64,
    ) {
        const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
        let rx_bytes = (rx_gb * GIB).round() as i64;
        let tx_bytes = (tx_gb * GIB).round() as i64;
        {
            let mut inner = self.inner.lock().expect("netstatic lock poisoned");
            inner.store.correction = Some(Correction {
                period_start,
                rx_offset: rx_bytes - raw_monthly.0 as i64,
                tx_offset: tx_bytes - raw_monthly.1 as i64,
                rx_gb,
                tx_gb,
                confirm_pending: true,
            });
            inner.dirty = true;
        }
        self.flush();
        tracing::info!(rx_gb, tx_gb, "流量校正已应用");
    }

    /// 待回传的校正确认值（GB 原值）
    pub fn confirm_pending(&self) -> Option<(f64, f64)> {
        let inner = self.inner.lock().expect("netstatic lock poisoned");
        inner
            .store
            .correction
            .filter(|c| c.confirm_pending)
            .map(|c| (c.rx_gb, c.tx_gb))
    }

    /// 服务端已清空待修正（响应不再带校正字段）：停止回传；偏移保留到账期结束
    pub fn clear_confirm(&self) {
        let need = {
            let mut inner = self.inner.lock().expect("netstatic lock poisoned");
            match &mut inner.store.correction {
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
            .unwrap_or_else(|| tz.with_ymd_and_hms(year, month, reset_day.into(), 3, 0, 0).unwrap())
    } else {
        let (y, m) = if month == 12 { (year + 1, 1) } else { (year, month + 1) };
        tz.with_ymd_and_hms(y, m, 1, 0, 0, 0).unwrap()
    }
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    let (y, m) = if month == 12 { (year + 1, 1) } else { (year, month + 1) };
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

    fn tmp_ns(tag: &str) -> (NetStatic, PathBuf) {
        let dir = std::env::temp_dir().join(format!("probe-rs-ns-{tag}-{}", std::process::id()));
        let path = dir.join("net.json");
        (NetStatic::load(&path), path)
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
            inner.store.interfaces.entry("eth0".into()).or_default().push_back(Entry {
                ts: now,
                rx: 2 * 1024 * 1024 * 1024,
                tx: 1024 * 1024 * 1024,
            });
        }
        // 校正为 rx=10GB tx=5GB（覆盖语义）
        ns.apply_correction(period, (2 * 1024 * 1024 * 1024, 1024 * 1024 * 1024), 10.0, 5.0);
        let (rx, tx) = ns.query_monthly(&filter, period, now);
        assert_eq!(rx, 10 * 1024 * 1024 * 1024);
        assert_eq!(tx, 5 * 1024 * 1024 * 1024);
        assert_eq!(ns.confirm_pending(), Some((10.0, 5.0)));

        // 落盘恢复：偏移与确认状态都在
        let ns2 = NetStatic::load(path.as_path());
        let (rx2, tx2) = ns2.query_monthly(&filter, period, now);
        assert_eq!((rx2, tx2), (rx, tx));
        assert_eq!(ns2.confirm_pending(), Some((10.0, 5.0)));

        // 服务端清空后停止回传，但偏移保留
        ns2.clear_confirm();
        assert_eq!(ns2.confirm_pending(), None);
        let (rx3, _) = ns2.query_monthly(&filter, period, now);
        assert_eq!(rx3, rx);

        // 账期翻页：偏移失效，回到原始累计
        let next_period = period + 32i64 * 24 * 3600 * 1000;
        let (rx4, tx4) = ns2.query_monthly(&filter, next_period, now);
        assert_eq!((rx4, tx4), (0, 0));

        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }
}
