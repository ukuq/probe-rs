use std::collections::HashMap;
use std::sync::Mutex;

use crate::model::{AsyncRecord, DynamicRecord, ErrorRecord};

/// 单个缓冲的最大保留条数：上报失败时数据保留待重发，超限丢最旧。
/// 只覆盖短暂抖动（长断网的历史不值得补发），10 条足够
pub const MAX_BUFFER_RECORDS: usize = 10;
/// 错误事件缓冲上限（比数据缓冲小，错误是辅助信息）
pub const MAX_ERROR_RECORDS: usize = 200;

/// dynamic / async / errors 三缓冲；report 时 drain，失败 restore（有界保留）
pub struct Buffers {
    dynamic: Mutex<Vec<DynamicRecord>>,
    async_records: Mutex<Vec<AsyncRecord>>,
    errors: Mutex<Vec<ErrorRecord>>,
    /// 同源同文去重：source -> 上次推送的 msg（持久状态，不随 drain 清空）
    error_dedup: Mutex<HashMap<String, String>>,
}

fn drain<T>(buf: &Mutex<Vec<T>>) -> Vec<T> {
    std::mem::take(&mut *buf.lock().expect("buffer lock poisoned"))
}

fn prepend_bounded<T>(buf: &Mutex<Vec<T>>, mut older: Vec<T>) {
    let mut guard = buf.lock().expect("buffer lock poisoned");
    older.append(&mut guard);
    let overflow = older.len().saturating_sub(MAX_BUFFER_RECORDS);
    if overflow > 0 {
        older.drain(..overflow);
    }
    *guard = older;
}

impl Buffers {
    pub fn new() -> Self {
        Self {
            dynamic: Mutex::new(Vec::new()),
            async_records: Mutex::new(Vec::new()),
            errors: Mutex::new(Vec::new()),
            error_dedup: Mutex::new(HashMap::new()),
        }
    }

    /// 推送错误事件：同源同文去重（上一条相同则跳过，防止周期性失败刷屏）
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
        let mut guard = self.errors.lock().expect("buffer lock poisoned");
        let overflow = guard.len().saturating_sub(MAX_ERROR_RECORDS - 1);
        if overflow > 0 {
            guard.drain(..overflow);
        }
        guard.push(ErrorRecord {
            ts: crate::model::now_millis(),
            source,
            msg,
        });
    }

    pub fn push_dynamic(&self, r: DynamicRecord) {
        self.dynamic.lock().expect("buffer lock poisoned").push(r);
    }

    pub fn push_async(&self, r: AsyncRecord) {
        self.async_records
            .lock()
            .expect("buffer lock poisoned")
            .push(r);
    }

    /// 换出全部缓冲内容；调用后缓冲为空
    pub fn drain(&self) -> (Vec<DynamicRecord>, Vec<AsyncRecord>, Vec<ErrorRecord>) {
        (
            drain(&self.dynamic),
            drain(&self.async_records),
            drain(&self.errors),
        )
    }

    /// 上报失败：数据放回缓冲头部（保持时间顺序），超上限丢最旧
    pub fn restore(
        &self,
        dynamic: Vec<DynamicRecord>,
        async_records: Vec<AsyncRecord>,
        errors: Vec<ErrorRecord>,
    ) {
        prepend_bounded(&self.dynamic, dynamic);
        prepend_bounded(&self.async_records, async_records);
        prepend_bounded(&self.errors, errors);
    }

    #[cfg(test)]
    fn dynamic_len(&self) -> usize {
        self.dynamic.lock().expect("buffer lock poisoned").len()
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
    fn restore_keeps_order() {
        let b = Buffers::new();
        b.push_dynamic(rec(3));
        b.push_dynamic(rec(4));
        let (mut drained, _, _) = b.drain();
        assert_eq!(drained.len(), 2);
        // 期间来了新数据
        b.push_dynamic(rec(5));
        drained.push(rec(6));
        b.restore(drained, vec![], vec![]);
        let (all, _, _) = b.drain();
        let ts: Vec<i64> = all.iter().map(|r| r.ts).collect();
        // 旧数据在前，新采集的 5 在后，顺序不乱
        assert_eq!(ts, vec![3, 4, 6, 5]);
    }

    #[test]
    fn error_dedup_same_source_same_msg() {
        let b = Buffers::new();
        b.push_error("gpu", "nvidia-smi exit 1");
        b.push_error("gpu", "nvidia-smi exit 1"); // 同文：跳过
        b.push_error("gpu", "timeout"); // 不同文：入队
        b.push_error("ip", "timeout"); // 不同源：入队
        let (_, _, errors) = b.drain();
        assert_eq!(errors.len(), 3);
        assert_eq!(errors[0].msg, "nvidia-smi exit 1");
        assert_eq!(errors[1].msg, "timeout");
        assert_eq!(errors[2].source, "ip");
    }

    #[test]
    fn restore_drops_oldest_when_full() {
        let b = Buffers::new();
        let failed: Vec<DynamicRecord> = (0..100).map(rec).collect();
        b.restore(failed, vec![], vec![]);
        assert_eq!(b.dynamic_len(), MAX_BUFFER_RECORDS);
        let (all, _, _) = b.drain();
        // 只留最新 10 条，最旧的被丢弃
        assert_eq!(all.first().unwrap().ts, 90);
    }
}
