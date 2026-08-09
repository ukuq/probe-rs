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

- 类型显式为 `http/tcp/icmp`；内部按类型 + 规范化目标聚合，重复目标周期取最小值。
- 每轮先解析一次 DNS，再开始 RTT 计时；4 次测量与高延迟重试复用已解析地址。
- TCP：`tokio::net::TcpStream::connect` + `tokio::time::timeout(3s)`，计时 connect 耗时。
- HTTP：reqwest `Client`（`pool_max_idle_per_host(0)`），固定预解析 IP 且禁止重定向；2xx/3xx 视为成功。
- ICMP：先解析为 IP，再调用平台 `ping` 命令；无需给 agent 本身授予 raw socket capability。
- 一轮 = 每目标 4 次测量取中位数；>1000ms 触发防重传重试（移植设计书 §2.3 规则）。
- 每个目标并发执行（`futures::future::join_all`），一轮总耗时 ≈ 最慢目标，不是累加。

### 3.7 远端配置（config.rs + reporter.rs）

- 响应体非空 → 解析为 `RemoteConfig` → 校验：`config_version != 当前`（空版本也跳过）、间隔 >= 1、`reset_day ∈ 0..=31`——全部通过才应用。
- 应用 = 更新内存配置 + 重写本地 TOML（tmp + rename）+ 通知 scheduler 重建 ticker（用 `watch` 发新 intervals，scheduler select 监听变化）。
- 任何一项非法：整体拒绝，`tracing::warn!` 记录原因。

### 3.8 配置热加载

- main 里 3s 轮询配置文件 mtime；变更后重新 load + 校验，失败保持原配置。
- 每个 Reporter 的 `intervals/interfaces/disks/pings/report_gpu` 经 `SharedConfig::update_local` 即时生效；实际周期取最小值、GPU 取 OR，选择项取并集。
- 聚合后的 `pings` / GPU 开关变更时重建对应 worker：channel 在 main 创建一次，任务可中止重建，scheduler 无感。

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

## 本地构建

```bash
make fmt     # cargo fmt + deno fmt（提交前跑）
make check   # CI 同款：fmt --check + deno check + cargo test
make build   # cargo build --release（strip + lto）
make demo    # 本地演示服务端（8080）
```

CI：`.github/workflows/ci.yml` 对所有 push/PR 在 Linux、Windows 跑 Rust 格式化与测试，并在 Linux 跑 Deno 门禁；`release.yml` 在 push master 时按 Cargo.toml version 发版，产出 Linux x86_64/aarch64 与 Windows x86_64 三个文件（资产完整时跳过）。

## 部署

### Linux + systemd

```bash
cargo build --release          # 产物 target/release/probe-rs（strip + lto）
sudo ./deploy/install.sh       # 装二进制/unit/示例配置；已装过则保留配置并重启
```

- 二进制 → `/usr/local/bin/probe-rs`；配置 → `/etc/probe-rs/config.toml`（600，含 secret）；数据 → `/var/lib/probe-rs/`
- 首次安装需先编辑配置填 `server_id` / `secret` / `worker_url`，再 `systemctl enable --now probe-rs`
- unit 加固：`ProtectSystem=strict` + `ReadWritePaths=/var/lib/probe-rs /etc/probe-rs`（后者因远端配置要回写 config.toml）；ICMP 使用系统 `ping`，agent 无需 `CAP_NET_RAW`
- 卸载：`./deploy/install.sh uninstall`（保留配置与数据，加 `--purge` 全清）
- 一键脚本（换 URL 即可装，参数对齐各官方探针）：CF 模式 `deploy/cf-install.sh`（-id/-secret/-url/-ct/-cu/-cm/-bd）；komari 模式 `deploy/komari-install.sh`（-e 面板地址/-t token/-i 间隔，缺省 collect=1 report=3 对齐官方节奏）

### Windows + 计划任务

在管理员 PowerShell 中执行：

```powershell
cargo build --release
.\deploy\install.ps1
```

- 二进制 → `%ProgramFiles%\probe-rs\probe-rs.exe`；配置与流量数据 → `%ProgramData%\probe-rs\`
- 使用 `SYSTEM`、最高权限的开机计划任务常驻；异常退出后每分钟重启
- 首次安装会保留示例配置但禁用任务；填好 `server_id` / `secret` / `worker_url` 后执行 `.\deploy\install.ps1 start`
- 状态/停止：`.\deploy\install.ps1 status` / `.\deploy\install.ps1 stop`
- 卸载：`.\deploy\install.ps1 uninstall`（保留配置与数据，加 `-Purge` 全清）
- Release 资产名为 `probe-rs-windows-x86_64.exe`，可用 `-BinaryPath` 指向下载后的文件

## CF 协议模式（protocol = "cf"）

agent 可切换为 CF-Server-Monitor 的 `POST /update` 协议（适配官方服务端，零改动对接）。

**配置**：`protocol = "cf"`（🔒 本地，重启生效）；`server_id` 填 CF 后台分配的 UUID，`secret` 填 `API_SECRET`，`worker_url` 填 `https://<worker>/update`。

Windows 使用同一套 CF 协议逻辑；CF 一键安装默认启用 GPU 采集（无 `nvidia-smi` 时记录诊断但不影响其他指标）。可在管理员 PowerShell 中直接一键安装（`CollectInterval=0` 同样映射为内部 1 秒）：

```powershell
.\deploy\cf-install.ps1 install -Id <UUID> -Secret <API_SECRET> `
  -Url https://<worker>/update -CollectInterval 0 -Interval 60
```

脚本默认下载 `probe-rs-windows-x86_64.exe`，也可用 `-BinarySource`/`-Bin` 指定本地文件或 URL；卸载使用 `.\deploy\cf-install.ps1 uninstall`，加 `-Purge` 可清除配置与流量数据。也可以通过通用 `deploy/install.ps1` 安装后，手动将 `%ProgramData%\probe-rs\config.toml` 的 `protocol` 改为 `"cf"`。

**上报映射**（reporter_cf.rs）：顶层 `{id, secret, metrics, samples[]}`；ram/swap/disk 字节→MB；load 转空格字符串；GPU → `gpu_info:[{id,name,info}]`（显存/温度丢弃）；ping 按组名落 `ping_ct/cu/cm/bd` + `loss_*`（bgp 是 bd 别名，未配置为 `false`，已配置但失败为 `null`）；`ip_v4/v6` 不可达报数值 `0`；`dynamic[]` → `samples[]`（`ext.cf.batch=false` 时只发单条 metrics）。空上报周期复用最近一次独立采集快照，避免 CF 将缺失动态字段写成假 0，但不会由 report 触发采集。errors/self/virtualization 无落点，CF 模式下不产生。

**配置下发**：请求头 `X-Agent-Config-Schema: 3` + `X-Agent-Config-Md5`（复用 `config_version` 字段存 MD5，空 = `none`）。响应 204 = 无变更；200 + URL-encoded body → 解析 collect_interval/report_interval/reset_day/custom_ct/cu/cm/bd/interface，合成 `RemoteConfig`（config_version 取响应头 MD5）走 `apply_remote_for`。`collect=0` 兼容映射为当前 CF Reporter 的 1 秒采集需求，随后参与机器级最小值聚合；逗号分隔的 interface 拆成多个过滤项。CF 未覆盖的 ping/slow/gpu/ip 子间隔与输出开关保持该 Reporter 现值。

**流量校正**：响应尾部 `rx_correction/tx_correction`（GB，覆盖当月累计）。netstatic 记账期偏移（offset = 校正字节 − 原始月累计，period_start 匹配才生效，翻页自动失效），立即落盘；校正确认用**独立请求**回传（CF 服务端见到 correction 字段会把整个请求当确认、丢弃 metrics），服务端清空后停止。`ext.cf.correction = false` 时整个回路忽略。`update=1`（自升级）永远忽略。

## komari 协议模式（protocol = "komari"）

对接 komari 面板的 WS v2 JSON-RPC（`/api/clients/v2/rpc?token=<secret>`，worker_url 填面板地址）。

- **上行**：`agent.report`（最新值快照，字节单位；无 ts/批量语义，断线期间数据不保留）+ `agent.basicInfo`（连接建立与 static 刷新时）；errors 事件拼进 report 的 `message` 字段
- **下行**：不执行远程控制调用，但**友好回绝**（不干等）：exec → POST task/result "Remote control is disabled."(exit -1)；terminal → 拨终端 WS 发说明即关闭（否则面板空转 30s）。我们从不调 agent.pull 声明远控能力
- **Ping**：收到 `agent.ping` 后按 `type + target` 写入该 Komari Reporter 的 `ext.komari.learned_pings`；最多 5 个，按 `last_seen_at` LRU 淘汰。下发请求本身不探测，只读取全局 Ping worker 快照；首轮无缓存回 `-1`，配置热重建后后续任务返回新鲜缓存（最大年龄为本地 ping 周期的 2 倍，且至少 10s）。HTTP 裸 host 自动补 `http://`，path/query/fragment 仍拒绝
- komari 的月流量由面板自算；自动学习目标跟随该 Reporter 的 `intervals.ping`，与其他 Reporter 目标统一去重聚合；无面板配置下发通道（仅 Ping 目标发现会写本地 TOML）
- **保活**：komari 服务端读超时 11s 且只有数据帧续期（ping 无效）→ 心跳为每 5s 重发最新 report 文本帧
- 映射见 reporter_komari.rs（纯函数）；WS 机械（重连/心跳/下行忽略）在 worker/komari.rs
