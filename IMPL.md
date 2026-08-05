# probe-rs Rust 实现方案

对应 [DESIGN.md](DESIGN.md) 的设计与 [REPORT.md](REPORT.md) 的协议。一期目标：Linux，单二进制，无 CGO 类依赖。

## 1. 技术选型

| 用途 | crate | 说明 |
|---|---|---|
| 异步运行时 | `tokio`（rt-multi-thread, macros, time, sync, signal） | ticker、worker、watch channel 全靠它 |
| HTTP 客户端 | `reqwest`（rustls-tls, json） | 上报 + 公网 IP 查询；rustls 避免 OpenSSL 依赖，方便静态编译 |
| 序列化 | `serde` + `serde_json` | 报文与配置 |
| 本地配置 | `toml` | 本地配置文件格式（远端下发是 JSON，两者分开） |
| 日志 | `tracing` + `tracing-subscriber` | |
| 错误 | `thiserror`（库内）/ `anyhow`（main 层） | |
| 时间计算 | `time` | reset_day 账期计算（移植 lastResetDate 逻辑） |
| glob 匹配 | `globset` | 网卡白名单 `eth*` 匹配 |
| ICMP 探测（可选） | `surge-ping` | 纯 Rust ICMP，需 CAP_NET_RAW/root；一期可只做 tcp/http |

**不引入 `sysinfo`**：/proc 解析全部手写（参照 cfsm-agent，单文件几百行，零依赖、行为可控）。

## 2. 项目结构

单 crate，模块与设计书 §6 一一对应：

```
probe-rs/
├── Cargo.toml
├── src/
│   ├── main.rs              # 入口：加载配置 → 校验 → 起 scheduler + workers → 等退出信号
│   ├── config.rs            # 本地 TOML 加载/校验；远端 JSON 原子应用 + 落盘
│   ├── model.rs             # Report/Static/DynamicRecord(SlowBlock/PingRecord/GpuRecord)/RemoteConfig（serde）
│   ├── scheduler.rs         # collect/report 双层 ticker；异步快照新鲜度摘取
│   ├── buffer.rs            # dynamic 单缓冲，drain/restore（有界）
│   ├── reporter.rs          # POST /report；X-Secret 头；解析响应里的远端配置
│   ├── collector/
│   │   ├── mod.rs           # 平台门面：Linux /proc 实现 + sysinfo 跨平台实现
│   │   ├── cpu.rs           # /proc/stat 差值（持有 prev 状态）
│   │   ├── mem.rs           # /proc/meminfo
│   │   ├── disk.rs          # /proc/mounts 遍历 + statfs + 设备去重
│   │   ├── net.rs           # /proc/net/dev 计数器 + 白名单过滤 + 速度差值
│   │   ├── conn.rs          # /proc/net/{tcp,tcp6,udp,udp6} 行计数
│   │   ├── load.rs          # /proc/loadavg
│   │   ├── process.rs       # /proc 数字目录计数
│   │   └── sysinfo.rs       # static：os/kernel/cpu_name/boot_time/virtualization 等
│   ├── worker/
│   │   ├── mod.rs           # 异步 worker：只发布最新快照（watch channel），真实测量 ts
│   │   ├── ping.rs          # 每组 [[pings]] 独立 task/间隔；HashMap<key, 记录> 快照
│   │   ├── slow.rs          # 慢变指标（disk/conn/procs）快照
│   │   ├── public_ip.rs     # cloudflare trace，强制 tcp4/tcp6
│   │   └── gpu.rs           # nvidia-smi 快照
│   └── netstatic/
│       ├── mod.rs           # 对外 API：record()/query(start, now)/flush()
│       ├── sampler.rs       # 2s 采样 task：/proc/net/dev → per-iface delta
│       └── store.rs         # 内存时序 + 31 天滚动修剪 + 10min 落盘 + 启动加载
└── config.example.toml
```

## 3. 关键实现点

### 3.1 调度（scheduler.rs）

- 用 `tokio::time::interval` 而不是 `sleep` 循环：错过 tick 时 `MissedTickBehavior::Skip`，防止堆积补偿执行。
- intervals 仅校验 >= 1 秒；report 与 collect 无任何关系约束，report 时把缓冲全部发出即可。
- collect tick：`collector::collect_once()` 全程同步文件读取（微秒级），直接 async 里跑，不用 spawn_blocking。
- 同步采集组装 dynamic 记录：fast 字段（cpu/mem/net/load）+ 各异步源快照按 ts 新鲜度摘取的块（slow/pings/gpu）。
- report tick：drain buffer → 组装 `Report`（static 到期才带）→ `reporter::send()`；失败 restore（有界保留）。

### 3.2 快照传递（worker/mod.rs）

- 每个 worker 持有一个 `watch::Sender<T>`，主采集/调度器持 `Receiver`。
- `watch` 语义正好是"只关心最新值"，旧值被覆盖即丢弃——符合"异步只发快照"。
- 采集端为每个异步源记录"上次摘取的 ts"，快照 ts 更新才把数据带入 dynamic 记录（新鲜度去重）；失败 = 快照 ts 停滞。

### 3.3 缓冲（buffer.rs）

- `Mutex<Vec<DynamicRecord>>` 等三个，标准库 `Mutex` 即可（临界区极短，无需 tokio::Mutex）。
- report 时 `std::mem::take` 换出；发送失败 `restore` 放回缓冲头部（保序），每类上限 1000 条，超限丢最旧——断网不丢数据，内存又有界。

### 3.4 netstatic

- 存储：`BTreeMap<String, VecDeque<Entry>>`，Entry = `{ts_ms, rx, tx}`。
- 采样 task 每 2s：读 /proc/net/dev → 与上一帧 per-iface 计数器算 delta → `current < prev` 记 0（纪律 2）→ append 内存 + 标记 dirty。
- 修剪：append 时顺手弹出队首 `ts < now - 31d` 的条目。
- 落盘：每 10min 全量重写 `net_static.json`（tmp + rename 原子写，spawn_blocking）；启动时加载。
- 查询：`sum(period_start..=now)` 按白名单网卡过滤求和——月流量就是这个调用。
- 内存优化（可选二期）：小时粒度合并，见设计书 §5.4。

### 3.5 账期计算（config.rs 或 netstatic）

- 移植 cfsm `traffic.go` 的 `lastResetDate`/`actualResetDate`：reset_day 超过当月天数顺延下月 1 号。
- 用 `time`  crate 的本地时区日期运算；reset_day=0 时 period_start = 0（全量求和）。

### 3.6 探测（worker/ping.rs）

- TCP：`tokio::net::TcpStream::connect` + `tokio::time::timeout(3s)`，计时 connect 耗时。
- HTTP：reqwest `Client`（`pool_max_idle_per_host(0)`），2xx/3xx 视为成功。
- ICMP（可选）：`surge-ping`，需要 root 或 `CAP_NET_RAW`；无权限时降级为 TCP。
- 一轮 = 每目标 4 次测量取中位数；>1000ms 触发防重传重试（移植设计书 §2.3 规则）。
- 每个目标并发执行（`futures::future::join_all`），一轮总耗时 ≈ 最慢目标，不是累加。

### 3.7 远端配置（config.rs + reporter.rs）

- 响应体非空 → 解析为 `RemoteConfig` → 校验：`config_version != 当前`（空版本也跳过）、间隔 >= 1、`reset_day ∈ 0..=31`——全部通过才应用。
- 应用 = 更新内存配置 + 重写本地 TOML（tmp + rename）+ 通知 scheduler 重建 ticker（用 `watch` 发新 intervals，scheduler select 监听变化）。
- 任何一项非法：整体拒绝，`tracing::warn!` 记录原因。

### 3.8 配置热加载

- main 里 3s 轮询配置文件 mtime；变更后重新 load + 校验，失败保持原配置。
- `interfaces`/远程下发的 intervals 经 `SharedConfig::update_local` 即时生效（每 tick 重读 / watch 通知）。
- `pings` / `enable_gpu` 变更时重建对应 worker：channel 在 main 创建一次，任务可中止重建，scheduler 无感。

### 3.9 优雅退出

- `tokio::signal::ctrl_c()` + SIGTERM（`signal::unix`）→ 通知 netstatic flush → 退出。
- 崩溃兜底靠 10min 定期落盘，退出 flush 只是减少丢失窗口。

## 4. 实施顺序

| 阶段 | 内容 | 产出 |
|---|---|---|
| P1 骨架 | cargo 项目、config、model、scheduler、buffer、reporter（可配 mock endpoint）、main | 能跑空循环按间隔 POST |
| P2 同步采集 | cpu/mem/disk/net/conn/load/process + static 采集 | dynamic/static 有真实数据 |
| P3 netstatic | sampler/store/账期计算/落盘 | net_*_monthly 有值 |
| P4 异步 worker | ping（tcp/http 先行）、public-ip、gpu | ping[]/gpu[]/ipv4/ipv6 有值 |
| P5 远端配置 | 响应解析、原子应用、落盘、ticker 重建 | 服务端可调节奏与 reset_day |
| P6 收尾 | tracing 日志完善、systemd unit、release 构建（musl 静态）、README | 可部署 |

每个阶段独立可验证：P1 用 `nc`/`httpbin` 收包，P2 对照 `/proc` 手算，P3 写单测覆盖账期边界（31 天月份、跨年、reset_day 变更），P4 对本机服务探测，P5 起 mock server 下发非法/合法配置各验一次。

## 5. 测试要点（先于代码定下来）

| 测试 | 内容 |
|---|---|
| 账期边界 | reset_day=31 遇 30 天月份顺延；跨年；reset_day 中途变更后求和窗口正确 |
| 计数器回退 | current < prev 时 delta=0（net 与 netstatic 两处） |
| 间隔校验 | 零值拒绝启动；远端配置同样校验（report/collect 无关系约束） |
| 缓冲保留 | 失败 restore 保序；超 1000 条丢最旧 |
| 防重传 | 首次 >1000ms、重测降幅 >800ms 判失败 |
| 磁盘去重 | 同设备多挂载点只计一次；ZFS pool 截断；虚拟 FS 排除 |
| 远端配置 | version 不变不应用；非法字段整体拒绝；应用后 TOML 落盘内容正确 |

## 部署（Linux + systemd）

```bash
cargo build --release          # 产物 target/release/probe-rs（strip + lto）
sudo ./deploy/install.sh       # 装二进制/unit/示例配置；已装过则保留配置并重启
```

- 二进制 → `/usr/local/bin/probe-rs`；配置 → `/etc/probe-rs/config.toml`（600，含 secret）；数据 → `/var/lib/probe-rs/`
- 首次安装需先编辑配置填 `server_id` / `secret` / `worker_url`，再 `systemctl enable --now probe-rs`
- unit 加固：`ProtectSystem=strict` + `ReadWritePaths=/var/lib/probe-rs /etc/probe-rs`（后者因远端配置要回写 config.toml）；未来启用 ICMP ping 时解开 `AmbientCapabilities=CAP_NET_RAW`
- 卸载：`./deploy/install.sh uninstall`（保留配置与数据，加 `--purge` 全清）
