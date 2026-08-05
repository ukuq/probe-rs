//! 负载：/proc/loadavg

pub fn collect() -> Option<[f64; 3]> {
    let data = std::fs::read_to_string("/proc/loadavg").ok()?;
    let mut fields = data.split_whitespace();
    let l1 = fields.next()?.parse().ok()?;
    let l5 = fields.next()?.parse().ok()?;
    let l15 = fields.next()?.parse().ok()?;
    Some([l1, l5, l15])
}
