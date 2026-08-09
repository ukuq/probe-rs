use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use crate::model::{AsyncRecord, DynamicRecord, ErrorRecord};

/// 所有 Reporter 共享的短期事件日志；慢端点不会阻塞采集，超限只丢最旧事件。
pub const MAX_JOURNAL_RECORDS: usize = 512;

#[derive(Debug, Clone)]
enum Event {
    Dynamic(DynamicRecord),
    Async(AsyncRecord),
    Error(ErrorRecord),
}

#[derive(Default)]
struct State {
    next_seq: u64,
    events: VecDeque<(u64, Event)>,
    /// reporter_id -> 已确认的最后序号
    cursors: HashMap<String, u64>,
}

#[derive(Debug, Clone, Default)]
pub struct BufferBatch {
    pub through: u64,
    pub dynamic: Vec<DynamicRecord>,
    pub async_records: Vec<AsyncRecord>,
    pub errors: Vec<ErrorRecord>,
}

pub struct Buffers {
    state: Mutex<State>,
    error_dedup: Mutex<HashMap<String, String>>,
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

    pub fn push_error(&self, source: impl Into<String>, msg: impl Into<String>) {
        let source = source.into();
        let msg = msg.into();
        {
            let mut dedup = self.error_dedup.lock().expect("buffer lock poisoned");
            if dedup.get(&source).is_some_and(|last| *last == msg) {
                return;
            }
            dedup.insert(source.clone(), msg.clone());
        }
        self.push(Event::Error(ErrorRecord {
            ts: crate::model::now_millis(),
            source,
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
        let seq = state.next_seq;
        state.next_seq = state.next_seq.saturating_add(1);
        state.events.push_back((seq, event));
        while state.events.len() > MAX_JOURNAL_RECORDS {
            state.events.pop_front();
        }
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
}
