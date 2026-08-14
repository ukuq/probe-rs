use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use crate::model::{AsyncRecord, DynamicRecord, ErrorRecord};

/// 所有 Reporter 共享的短期事件日志；慢端点不会阻塞采集，超限只丢最旧事件。
/// 丢弃未确认事件时会注入 source=buffer 的错误记录并打 warn 日志（节流）。
pub const MAX_JOURNAL_RECORDS: usize = 512;

/// 每个错误来源保留的近期消息条数。只比对最后一条会让 A/B/A/B 交替消息
/// 每次都穿透去重、把 journal 刷满并驱逐真实指标；保留一个小历史窗口后，
/// 交替消息被抑制，而真正的新错误照常入队。
const DEDUP_HISTORY: usize = 8;

/// 错误来源（类型化，journal 内部使用）。线上协议的 `ping:`/`reporter:`
/// 前缀字符串由 Reporter 出口处（scope_errors）从该枚举生成，内部路由
/// 不再解析自由文本前缀，磁盘/网卡名撞上前缀也不会被误路由。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ErrorOrigin {
    /// 采集器直报：gpu / ip / connections / diskio / buffer ...
    Collector(String),
    /// Ping 任务（task_id，出口处按 Reporter 别名展开）
    Ping(String),
    /// 某个 Reporter 自身的错误（只路由给该 Reporter）
    Reporter(String),
}

#[derive(Debug, Clone)]
pub struct LoggedError {
    pub ts: i64,
    pub origin: ErrorOrigin,
    pub msg: String,
}

impl LoggedError {
    /// 生成线上协议形态：source 字符串 + msg
    pub fn to_wire(&self, source: impl Into<String>) -> ErrorRecord {
        ErrorRecord {
            ts: self.ts,
            source: source.into(),
            msg: self.msg.clone(),
        }
    }
}

#[derive(Debug, Clone)]
enum Event {
    Dynamic(DynamicRecord),
    Async(AsyncRecord),
    Error(LoggedError),
}

#[derive(Default)]
struct State {
    next_seq: u64,
    events: VecDeque<(u64, Event)>,
    /// reporter_id -> 已确认的最后序号
    cursors: HashMap<String, u64>,
    /// 溢出丢弃未确认事件的累计数，用于节流告警
    dropped_unacked: u64,
    /// 上一次溢出告警时的 dropped_unacked，避免每个事件都刷日志
    last_drop_warn: u64,
}

#[derive(Debug, Clone, Default)]
pub struct BufferBatch {
    pub through: u64,
    pub dynamic: Vec<DynamicRecord>,
    pub async_records: Vec<AsyncRecord>,
    pub errors: Vec<LoggedError>,
}

pub struct Buffers {
    state: Mutex<State>,
    error_dedup: Mutex<HashMap<String, VecDeque<String>>>,
}

impl Buffers {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                next_seq: 1,
                ..Default::default()
            }),
            error_dedup: Mutex::new(HashMap::new()),
        }
    }

    /// Reporter 必须在采集启动前注册，初始游标指向当前日志尾部。
    pub fn register(&self, reporter_id: impl Into<String>) {
        let mut state = self.state.lock().expect("buffer lock poisoned");
        let tail = state.next_seq.saturating_sub(1);
        state.cursors.entry(reporter_id.into()).or_insert(tail);
    }

    /// 采集器错误：source 原样作为线上 source（gpu / ip / connections ...）。
    pub fn push_error(&self, source: impl Into<String>, msg: impl Into<String>) {
        self.push_typed_error(ErrorOrigin::Collector(source.into()), msg.into());
    }

    /// Ping 任务错误：task_id 在出口处按 Reporter 别名展开。
    pub fn push_ping_error(&self, task_id: impl Into<String>, msg: impl Into<String>) {
        self.push_typed_error(ErrorOrigin::Ping(task_id.into()), msg.into());
    }

    /// Reporter 自身错误：只路由给该 Reporter，线上 source 为 "reporter"。
    pub fn push_reporter_error(&self, reporter_id: impl Into<String>, msg: impl Into<String>) {
        self.push_typed_error(ErrorOrigin::Reporter(reporter_id.into()), msg.into());
    }

    fn push_typed_error(&self, origin: ErrorOrigin, msg: String) {
        {
            let mut dedup = self.error_dedup.lock().expect("buffer lock poisoned");
            let history = dedup.entry(format!("{origin:?}")).or_default();
            if history.iter().any(|recent| *recent == msg) {
                return;
            }
            history.push_back(msg.clone());
            while history.len() > DEDUP_HISTORY {
                history.pop_front();
            }
        }
        self.push(Event::Error(LoggedError {
            ts: crate::model::now_millis(),
            origin,
            msg,
        }));
    }

    pub fn push_dynamic(&self, record: DynamicRecord) {
        self.push(Event::Dynamic(record));
    }

    pub fn push_async(&self, record: AsyncRecord) {
        self.push(Event::Async(record));
    }

    fn push(&self, event: Event) {
        let mut state = self.state.lock().expect("buffer lock poisoned");
        push_locked(&mut state, event);
    }

    /// 非破坏性读取：同一批数据可被任意数量 Reporter 独立消费。
    pub fn read(&self, reporter_id: &str) -> BufferBatch {
        let mut state = self.state.lock().expect("buffer lock poisoned");
        let default_cursor = state.next_seq.saturating_sub(1);
        let cursor = *state
            .cursors
            .entry(reporter_id.to_string())
            .or_insert(default_cursor);
        let mut batch = BufferBatch {
            through: state.events.back().map_or(cursor, |(seq, _)| *seq),
            ..Default::default()
        };
        for (_, event) in state.events.iter().filter(|(seq, _)| *seq > cursor) {
            match event {
                Event::Dynamic(record) => batch.dynamic.push(record.clone()),
                Event::Async(record) => batch.async_records.push(record.clone()),
                Event::Error(record) => batch.errors.push(record.clone()),
            }
        }
        batch
    }

    /// 仅成功上报后确认；日志只清理到所有 Reporter 都确认的位置。
    pub fn ack(&self, reporter_id: &str, through: u64) {
        let mut state = self.state.lock().expect("buffer lock poisoned");
        if let Some(cursor) = state.cursors.get_mut(reporter_id) {
            *cursor = (*cursor).max(through);
        }
        let min_ack = state
            .cursors
            .values()
            .copied()
            .min()
            .unwrap_or_else(|| state.next_seq.saturating_sub(1));
        while state.events.front().is_some_and(|(seq, _)| *seq <= min_ack) {
            state.events.pop_front();
        }
    }
}

impl Default for Buffers {
    fn default() -> Self {
        Self::new()
    }
}

/// 追加事件并在溢出时丢弃最旧事件；丢弃**未确认**事件时留下信号——
/// 本地 tracing::warn + 注入一条 source=buffer 的 ErrorRecord 随上报带出，
/// 避免端点长时间中断导致的数据丢失完全无声。
fn push_locked(state: &mut State, event: Event) {
    let seq = state.next_seq;
    state.next_seq = state.next_seq.saturating_add(1);
    state.events.push_back((seq, event));
    let mut dropped = 0u64;
    while state.events.len() > MAX_JOURNAL_RECORDS {
        let (dropped_seq, _) = state.events.pop_front().expect("journal non-empty");
        // 与 ack() 一致：无游标时视为全部已确认
        let min_cursor = state
            .cursors
            .values()
            .copied()
            .min()
            .unwrap_or_else(|| seq.saturating_sub(1));
        if dropped_seq > min_cursor {
            dropped += 1;
        }
    }
    if dropped == 0 {
        return;
    }
    state.dropped_unacked = state.dropped_unacked.saturating_add(dropped);
    // 首次及此后每 64 条告警一次；递归注入不会再触发告警（差值 < 64）
    if state.last_drop_warn == 0 || state.dropped_unacked - state.last_drop_warn >= 64 {
        state.last_drop_warn = state.dropped_unacked;
        let total = state.dropped_unacked;
        tracing::warn!(
            dropped_total = total,
            "journal 溢出：未确认事件被丢弃（端点中断过久）"
        );
        push_locked(
            state,
            Event::Error(LoggedError {
                ts: crate::model::now_millis(),
                origin: ErrorOrigin::Collector("buffer".to_string()),
                msg: format!("上报缓冲溢出，已累计丢弃 {total} 条未确认事件"),
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(ts: i64) -> DynamicRecord {
        DynamicRecord {
            ts,
            ..Default::default()
        }
    }

    #[test]
    fn reporters_read_and_ack_independently() {
        let buffers = Buffers::new();
        buffers.register("a");
        buffers.register("b");
        buffers.push_dynamic(rec(1));
        buffers.push_dynamic(rec(2));

        let a = buffers.read("a");
        let b = buffers.read("b");
        assert_eq!(a.dynamic.len(), 2);
        assert_eq!(b.dynamic.len(), 2);
        buffers.ack("a", a.through);
        assert_eq!(buffers.read("b").dynamic.len(), 2);
        buffers.ack("b", b.through);
        assert!(buffers.read("a").dynamic.is_empty());
    }

    #[test]
    fn failed_reporter_keeps_its_cursor() {
        let buffers = Buffers::new();
        buffers.register("a");
        buffers.push_dynamic(rec(1));
        let first = buffers.read("a");
        let retry = buffers.read("a");
        assert_eq!(first.through, retry.through);
        assert_eq!(retry.dynamic[0].ts, 1);
    }

    #[test]
    fn errors_are_deduplicated() {
        let buffers = Buffers::new();
        buffers.register("a");
        buffers.push_error("gpu", "x");
        buffers.push_error("gpu", "x");
        buffers.push_error("gpu", "y");
        assert_eq!(buffers.read("a").errors.len(), 2);
    }

    #[test]
    fn alternating_error_messages_do_not_flood_the_journal() {
        let buffers = Buffers::new();
        buffers.register("a");
        for _ in 0..4 {
            buffers.push_error("gpu", "timeout");
            buffers.push_error("gpu", "exit 1");
        }
        // 交替消息在历史窗口内重复，被抑制；不会驱逐 dynamic 记录。
        let batch = buffers.read("a");
        assert_eq!(batch.errors.len(), 2);
        buffers.ack("a", batch.through);

        // 历史窗口之外的新消息照常入队。
        buffers.push_error("gpu", "timeout");
        buffers.push_error("gpu", "brand new failure");
        assert_eq!(buffers.read("a").errors.len(), 1);
    }

    #[test]
    fn overflow_of_unacked_events_emits_a_notice() {
        let buffers = Buffers::new();
        buffers.register("a");
        for i in 0..(MAX_JOURNAL_RECORDS + 10) {
            buffers.push_dynamic(rec(i as i64));
        }
        let batch = buffers.read("a");
        assert_eq!(batch.dynamic.len(), MAX_JOURNAL_RECORDS - 1);
        let notice = batch
            .errors
            .iter()
            .find(|e| e.origin == ErrorOrigin::Collector("buffer".to_string()))
            .expect("overflow should inject a buffer error record");
        assert!(notice.msg.contains("丢弃"));
    }

    #[test]
    fn overflow_of_acked_events_stays_quiet() {
        let buffers = Buffers::new();
        buffers.register("a");
        for i in 0..(MAX_JOURNAL_RECORDS + 10) {
            buffers.push_dynamic(rec(i as i64));
            let batch = buffers.read("a");
            buffers.ack("a", batch.through);
        }
        assert!(buffers.read("a").errors.is_empty());
    }
}
