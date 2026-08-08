//! 异步 worker：ping / slow / gpu / public-ip
//!
//! 两层分类（DESIGN.md §2.3）：机制同类，语义分流。
//! - 机制：所有 worker 相同——独立 ticker → 采集 → 发快照（只保留最近一次，
//!   ts 为真实测量时刻）；采集端按 ts 新鲜度摘取，失败 = 快照 ts 停滞。
//! - 语义：slow = 每台机器必有的系统慢指标（kind:"slow"）；
//!   gpu = 仅部分机器有的可选硬件指标（kind:"gpu"）；
//!   公网 IP = 身份信息，喂 static，不进 async[]。

pub mod diskio;
pub mod gpu;
pub mod ping;
pub mod public_ip;
pub mod slow;
