//! Windows 磁盘 IO：通过 PDH 读取 PhysicalDisk 原始累计计数器。
//!
//! PDH 的 `*/sec` 原始值仍是开机以来的累计量；统一交给 collector::disk_io_diff
//! 做相邻采样差分，和 Linux `/proc/diskstats` 的语义保持一致。

use std::collections::HashMap;
use std::mem::size_of;
use std::ptr::{null, null_mut};
use std::sync::{Mutex, OnceLock};

use windows_sys::Win32::Foundation::ERROR_SUCCESS;
use windows_sys::Win32::System::Performance::{
    PdhAddEnglishCounterW, PdhCloseQuery, PdhCollectQueryData, PdhGetCounterTimeBase,
    PdhGetRawCounterArrayW, PdhGetRawCounterValue, PdhOpenQueryW, PDH_CSTATUS_NEW_DATA,
    PDH_CSTATUS_VALID_DATA, PDH_HCOUNTER, PDH_HQUERY, PDH_MORE_DATA, PDH_RAW_COUNTER,
    PDH_RAW_COUNTER_ITEM_W,
};

use super::DiskIoCounters;

const READ_BYTES: &str = r"\PhysicalDisk(_Total)\Disk Read Bytes/sec";
const WRITE_BYTES: &str = r"\PhysicalDisk(_Total)\Disk Write Bytes/sec";
const READ_OPS: &str = r"\PhysicalDisk(_Total)\Disk Reads/sec";
const WRITE_OPS: &str = r"\PhysicalDisk(_Total)\Disk Writes/sec";
const TOTAL_TIME: &str = r"\PhysicalDisk(_Total)\Avg. Disk sec/Transfer";
const DISK_TIME: &str = r"\PhysicalDisk(*)\% Disk Time";

static QUERY: OnceLock<Mutex<Option<PdhDiskQuery>>> = OnceLock::new();

pub fn read_counters() -> Option<DiskIoCounters> {
    let query = QUERY.get_or_init(|| Mutex::new(None));
    let mut slot = query.lock().ok()?;

    if slot.is_none() {
        match PdhDiskQuery::new() {
            Ok(created) => *slot = Some(created),
            Err(e) => {
                tracing::warn!(error = %e, "Windows PDH 磁盘 IO 初始化失败");
                return None;
            }
        }
    }

    match slot.as_ref().expect("PDH query initialized").collect() {
        Ok(counters) => Some(counters),
        Err(e) => {
            tracing::warn!(error = %e, "Windows PDH 磁盘 IO 读取失败，将重建查询");
            *slot = None;
            None
        }
    }
}

struct PdhDiskQuery {
    // windows-sys 的 PDH handle 是裸指针；保存为 usize 后由 Mutex 串行访问。
    query: usize,
    read_bytes: usize,
    write_bytes: usize,
    read_ops: usize,
    write_ops: usize,
    total_time: usize,
    disk_time: usize,
    total_time_base: i64,
    disk_time_base: i64,
}

impl PdhDiskQuery {
    fn new() -> Result<Self, String> {
        let mut query: PDH_HQUERY = null_mut();
        let status = unsafe { PdhOpenQueryW(null(), 0, &mut query) };
        check_status("PdhOpenQueryW", status)?;

        let mut out = Self {
            query: query as usize,
            read_bytes: 0,
            write_bytes: 0,
            read_ops: 0,
            write_ops: 0,
            total_time: 0,
            disk_time: 0,
            total_time_base: 0,
            disk_time_base: 0,
        };
        out.read_bytes = out.add_counter(READ_BYTES)?;
        out.write_bytes = out.add_counter(WRITE_BYTES)?;
        out.read_ops = out.add_counter(READ_OPS)?;
        out.write_ops = out.add_counter(WRITE_OPS)?;
        out.total_time = out.add_counter(TOTAL_TIME)?;
        out.disk_time = out.add_counter(DISK_TIME)?;
        out.total_time_base = counter_time_base(out.total_time)?;
        out.disk_time_base = counter_time_base(out.disk_time)?;
        Ok(out)
    }

    fn add_counter(&self, path: &str) -> Result<usize, String> {
        let path_wide: Vec<u16> = path.encode_utf16().chain(Some(0)).collect();
        let mut counter: PDH_HCOUNTER = null_mut();
        let status = unsafe {
            PdhAddEnglishCounterW(
                self.query as PDH_HQUERY,
                path_wide.as_ptr(),
                0,
                &mut counter,
            )
        };
        check_status(path, status)?;
        Ok(counter as usize)
    }

    fn collect(&self) -> Result<DiskIoCounters, String> {
        let status = unsafe { PdhCollectQueryData(self.query as PDH_HQUERY) };
        check_status("PdhCollectQueryData", status)?;

        let read_bytes = raw_value(self.read_bytes)?;
        let write_bytes = raw_value(self.write_bytes)?;
        let read_ops = raw_value(self.read_ops)?;
        let write_ops = raw_value(self.write_ops)?;
        let total_time = raw_value(self.total_time)?;
        let disk_times = raw_array(self.disk_time)?;

        let mut io_ms_per_dev = HashMap::new();
        let mut total_fallback = None;
        for (name, raw) in disk_times {
            let elapsed = ticks_to_ms(raw.FirstValue, self.disk_time_base);
            if name.eq_ignore_ascii_case("_Total") {
                total_fallback = Some(elapsed);
            } else {
                io_ms_per_dev.insert(name, elapsed);
            }
        }
        // 极少数系统只公开 _Total；保留利用率指标，仍限制到 100%。
        if io_ms_per_dev.is_empty() {
            if let Some(elapsed) = total_fallback {
                io_ms_per_dev.insert("_Total".to_string(), elapsed);
            }
        }

        Ok(DiskIoCounters {
            read_bytes: nonnegative(read_bytes.FirstValue),
            write_bytes: nonnegative(write_bytes.FirstValue),
            read_ops: nonnegative(read_ops.FirstValue),
            write_ops: nonnegative(write_ops.FirstValue),
            total_time_ms: ticks_to_ms(total_time.FirstValue, self.total_time_base),
            io_ms_per_dev,
        })
    }
}

impl Drop for PdhDiskQuery {
    fn drop(&mut self) {
        if self.query != 0 {
            unsafe {
                PdhCloseQuery(self.query as PDH_HQUERY);
            }
        }
    }
}

fn counter_time_base(counter: usize) -> Result<i64, String> {
    let mut time_base = 0i64;
    let status = unsafe { PdhGetCounterTimeBase(counter as PDH_HCOUNTER, &mut time_base) };
    check_status("PdhGetCounterTimeBase", status)?;
    if time_base <= 0 {
        return Err(format!("PDH 返回非法时间基数: {time_base}"));
    }
    Ok(time_base)
}

fn raw_value(counter: usize) -> Result<PDH_RAW_COUNTER, String> {
    let mut raw = PDH_RAW_COUNTER::default();
    let status = unsafe { PdhGetRawCounterValue(counter as PDH_HCOUNTER, null_mut(), &mut raw) };
    check_status("PdhGetRawCounterValue", status)?;
    check_counter_status(raw.CStatus)?;
    Ok(raw)
}

fn raw_array(counter: usize) -> Result<Vec<(String, PDH_RAW_COUNTER)>, String> {
    let mut buffer_size = 0u32;
    let mut item_count = 0u32;
    let status = unsafe {
        PdhGetRawCounterArrayW(
            counter as PDH_HCOUNTER,
            &mut buffer_size,
            &mut item_count,
            null_mut(),
        )
    };
    if status != PDH_MORE_DATA && status != ERROR_SUCCESS {
        return Err(status_error("PdhGetRawCounterArrayW(size)", status));
    }
    if buffer_size == 0 {
        return Ok(Vec::new());
    }

    // 设备热插拔可能令两次调用间所需缓冲区变大，最多重试三次。
    for _ in 0..3 {
        let slots = (buffer_size as usize).div_ceil(size_of::<PDH_RAW_COUNTER_ITEM_W>());
        let mut buffer = vec![PDH_RAW_COUNTER_ITEM_W::default(); slots.max(1)];
        let mut actual_size = buffer_size;
        let status = unsafe {
            PdhGetRawCounterArrayW(
                counter as PDH_HCOUNTER,
                &mut actual_size,
                &mut item_count,
                buffer.as_mut_ptr(),
            )
        };
        if status == PDH_MORE_DATA {
            buffer_size = actual_size;
            continue;
        }
        check_status("PdhGetRawCounterArrayW", status)?;

        let mut out = Vec::with_capacity(item_count as usize);
        for item in buffer.iter().take(item_count as usize) {
            if !is_valid_counter_status(item.RawValue.CStatus) {
                continue;
            }
            let name = unsafe { wide_ptr_to_string(item.szName) };
            if !name.is_empty() {
                out.push((name, item.RawValue));
            }
        }
        return Ok(out);
    }
    Err("PdhGetRawCounterArrayW 缓冲区连续变化".to_string())
}

fn nonnegative(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn ticks_to_ms(value: i64, time_base: i64) -> u64 {
    if value <= 0 || time_base <= 0 {
        return 0;
    }
    let millis = (value as u128).saturating_mul(1000) / time_base as u128;
    millis.min(u64::MAX as u128) as u64
}

fn check_status(operation: &str, status: u32) -> Result<(), String> {
    if status == ERROR_SUCCESS {
        Ok(())
    } else {
        Err(status_error(operation, status))
    }
}

fn status_error(operation: &str, status: u32) -> String {
    format!("{operation} 失败，PDH status=0x{status:08x}")
}

fn check_counter_status(status: u32) -> Result<(), String> {
    if is_valid_counter_status(status) {
        Ok(())
    } else {
        Err(status_error("PDH counter data", status))
    }
}

fn is_valid_counter_status(status: u32) -> bool {
    status == PDH_CSTATUS_VALID_DATA || status == PDH_CSTATUS_NEW_DATA
}

unsafe fn wide_ptr_to_string(ptr: *mut u16) -> String {
    if ptr.is_null() {
        return String::new();
    }
    let mut len = 0usize;
    // PDH 实例名上限远低于此值；上限也防止损坏指针导致无界扫描。
    while len < 4096 && unsafe { *ptr.add(len) } != 0 {
        len += 1;
    }
    String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(ptr, len) })
}

#[cfg(test)]
mod tests {
    #[test]
    fn converts_pdh_ticks_using_reported_time_base() {
        assert_eq!(super::ticks_to_ms(20_000_000, 10_000_000), 2_000);
        assert_eq!(super::ticks_to_ms(-1, 10_000_000), 0);
        assert_eq!(super::ticks_to_ms(10, 0), 0);
    }

    #[test]
    #[ignore = "requires Windows PhysicalDisk performance counters"]
    fn reads_live_windows_disk_counters() {
        let counters = super::read_counters().expect("Windows PDH disk counters unavailable");
        assert!(counters.read_bytes > 0 || counters.write_bytes > 0);
        assert!(!counters.io_ms_per_dev.is_empty());
    }
}
