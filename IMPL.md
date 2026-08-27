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
│   ├── updater.rs           # GitHub Release 检查、SemVer/通道过滤、SHA-256 校验与自替换
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

- 用 `tokio::time::interval` 而不是 `sleep` 循环：错过 tick 时 `MissedTickBehavior::Skip`，防止堆积补偿执行（scheduler、各采集 worker 与 netstatic 采样任务统一使用 `worker::ticker`/`worker::ticker_at` 工厂）。
- intervals 仅校验 >= 1 秒；report 与 collect 无任何关系约束，report 时把缓冲全部发出即可。
- collect tick：`collector::collect_once()` 全程同步文件读取（微秒级），直接 async 里跑，不用 spawn_blocking。
- 同步采集组装 dynamic 记录：fast 字段（cpu/mem/net/load）+ 各异步源快照按 ts 新鲜度摘取的块（slow/pings/gpu）。
- report tick：drain buffer → 组装 `Report`（static 到期才带）→ `reporter::send()`；失败 restore（有界保留）。

### 3.2 快照传递（worker/mod.rs）

- 每个 worker 持有一个 `watch::Sender<T>`，主采集/调度器持 `Receiver`。
- `watch` 语义正好是“只关心最新值”，旧值被覆盖即丢弃——符合“异步只发快照”。
- 采集端为每个异步源记录“上次摘取的 ts”，快照 ts 更新才把数据带入 dynamic 记录（新鲜度去重）；失败 = 快照 ts 停滞。
- 公网 IPv4/IPv6 各自携带真实测量时间；失败保留旧测量但不刷新时间戳，Reporter 按 IP 采集周期与上报周期过滤过期地址。

### 3.3 缓冲（buffer.rs）

- 共享有界事件日志（`MAX_JOURNAL_RECORDS = 512`，dynamic/async/errors 三类共用一个 journal），每 Reporter 独立 seq 游标：`read()` 非破坏读取游标之后的全部事件；HTTP 在响应成功后 `ack(through)`，CF WSS 按 1 秒节奏异步发送、由后台收到对应服务端 ACK 后再推进游标；日志只裁剪到所有 Reporter 都已确认的位置（min-cursor）。
- 慢端点不会阻塞采集：日志满时丢最旧事件；丢弃**未确认**事件时按 64 条节流注入一条 `source=buffer` 错误事件并打 warn 日志，长中断不静默。
- errors 同源同文去重在入队前完成；上报失败无需"restore"，未 ack 的事件自然留在日志里待下轮重发。

### 3.4 netstatic

- 存储：每网卡 `VecDeque<Entry>` 明细 + `archived_totals` 永久归档基数，Entry = `{ts_ms, rx, tx}`。
- 采样 task 每 2s：读 /proc/net/dev → 与上一帧 per-iface 计数器算 delta → `current < prev` 记 0（纪律 2）→ append 内存 + 标记 dirty。
- 保留：**32 天**明细 + 永久归档基数（严格大于最长 31 天账期，账期首日明细不会被归档；reset_day 28-31 的月流量不因修剪少计）
- 落盘：每 10min 全量重写 `net_static.json`（tmp + rename 原子写，spawn_blocking）；启动时加载。
- 查询：`sum(period_start..=now)` 按白名单网卡过滤求和；`period_start=0` 额外加归档基数，实现真正永久累计。
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
- 应用 = 校验候选配置 → 在 `SharedConfig` 写锁内用同目录唯一临时文件写入并 fsync → 原子替换本地 TOML → 更新内存配置 → 用 `watch` 通知 scheduler；落盘失败不会改内存。该替换流程同时覆盖 Windows 已有 `config.toml`，不会依赖平台不同的 `std::fs::rename` 覆盖语义。
- 任何一项非法：整体拒绝，`tracing::warn!` 记录原因。

### 3.8 配置热加载

- main 里 3s 轮询配置文件 mtime；变更后由 `SharedConfig::update_local_from_disk` 持写锁读取、校验并提交同一文件快照，失败保持原配置。远端落盘和本地热加载因此不会发生“旧文件快照覆盖新内存配置”的竞态。
- 每个 Reporter 的 `intervals/interfaces/disks/pings/report_gpu` 经热加载即时生效；实际周期取最小值、GPU 取 OR，选择项取并集。
- 聚合后的 `pings` / GPU 开关变更时重建对应 worker：channel 在 main 创建一次，任务可中止重建，scheduler 无感。

### 3.9 优雅退出

- `tokio::signal::ctrl_c()` + SIGTERM（`signal::unix`）→ 通知 netstatic flush → 退出。
- 崩溃兜底靠 10min 定期落盘，退出 flush 只是减少丢失窗口。

### 3.10 自动更新

- `[auto_update] enabled=false` 默认关闭；`stable` 只读取正式 Release，`prerelease` 同时接受预发布版及之后更高的正式版。
- 仅当远端版本按 SemVer precedence 严格大于编译版本时更新；draft、缺少当前平台资产或缺少 `SHA256SUMS` 的 Release 均跳过。
- GitHub 仓库、下载路径和平台资产名编译期固定；二进制下载后必须通过 Release 附带的 SHA-256 校验。
- Linux 原子替换后由 systemd `Restart=always` 拉起；Windows 使用新版 helper 等旧 Agent 退出后重新运行计划任务（等待超时会安全失败、不会双 Agent 并存），托盘 companion 单独替换、会话内替换需下次登录生效。
- GitHub/API/下载/校验失败只记日志，不中断采集和上报；检查周期最低 300 秒，默认 6 小时。

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

CI：`.github/workflows/ci.yml` 对所有 push/PR 在 Linux、Windows 跑 Rust 格式化与测试，并在 Linux 跑 Deno 门禁；`release.yml` 在 push master 时按 Cargo.toml version 发版，产出 Linux x86_64/aarch64/loong64 与 Windows x86_64 四个文件（资产完整时跳过）。版本包含 SemVer 预发布后缀（如 `-beta.1`）时发布为 GitHub prerelease。CF 一键安装脚本（cf-install.sh/cf-install.ps1）默认 pin 当前版本以保持可复现安装，`latest`/`-install-version` 为显式可选项；komari-install.sh 默认走 `latest`。

## 部署

### Linux + systemd

```bash
cargo build --release          # 产物 target/release/probe-rs（strip + lto）
sudo ./deploy/install.sh       # 装二进制/unit/示例配置；已装过则保留配置并重启
./deploy/install.sh            # 非 root：安装到当前用户并使用 systemctl --user
```

- root 安装：二进制 → `/usr/local/bin/probe-rs`；配置 → `/etc/probe-rs/config.toml`（600，含 secret）；数据 → `/var/lib/probe-rs/`
- 普通用户安装：二进制 → `~/.local/bin/probe-rs`；配置/数据遵循 `XDG_CONFIG_HOME`、`XDG_DATA_HOME`（缺省为 `~/.config/probe-rs`、`~/.local/share/probe-rs`）；unit → `~/.config/systemd/user/probe-rs.service`
- 首次安装需先编辑配置填 `server_id` / `secret` / `worker_url`，再执行 `systemctl enable --now probe-rs`（root）或 `systemctl --user enable --now probe-rs`（普通用户）
- 用户服务需要有效的 systemd 用户会话；未启用 linger 时退出登录可能停止服务，管理员可执行 `loginctl enable-linger <user>` 允许其后台常驻
- unit 加固：`ProtectSystem=strict` + `ReadWritePaths=/var/lib/probe-rs /etc/probe-rs /usr/local/bin`（分别用于流量落盘、配置回写和校验后的原子自替换）；ICMP 使用系统 `ping`，agent 无需 `CAP_NET_RAW`
- 卸载：`./deploy/install.sh uninstall`（保留配置与数据，加 `--purge` 全清）
- 一键脚本同样按执行身份选择系统服务或用户服务（换 URL 即可装，参数对齐各官方探针）：CF 模式 `deploy/cf-install.sh`（-id/-secret/-url/-ct/-cu/-cm/-bd）；komari 模式 `deploy/komari-install.sh`（-e 面板地址/-t token/-i 间隔，缺省 collect=1 report=3 对齐官方节奏）

### Windows

默认免管理员安装到当前用户（仅在该用户登录期间运行）：

```powershell
cargo build --release
.\deploy\install.ps1
```

如需开机未登录也常驻，在管理员 PowerShell 中显式安装为机器级计划任务：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\deploy\install.ps1 install -Scope Machine
```

- 默认 User 模式将二进制、配置和数据分别放到 `%LocalAppData%\probe-rs\`、同目录 `config.toml` 和 `data\`，用当前用户启动项运行 Agent/托盘，不注册 SYSTEM 任务，控制与编辑不触发 UAC；注销后停止，重新登录后恢复。自动更新直接重启用户进程，不调用机器级计划任务
- `-Scope Machine` 将二进制放到 `%ProgramFiles%\probe-rs\probe-rs.exe`，配置与流量数据放到 `%ProgramData%\probe-rs\`，使用 `SYSTEM`、最高权限的计划任务常驻；开机、任意用户登录及休眠唤醒（延迟 10 秒）时触发，异常退出后每分钟重启
- 登录用户的托盘伴随程序显示探针运行状态和 PID；检测到多个探针进程时会注明数量并列出全部 PID，同时提供启动、停止、重启和查看/编辑配置。托盘本身保持普通权限，仅在执行控制操作或编辑受保护配置时通过 UAC 启动短生命周期管理员 helper；编辑器只打开临时副本，保存时执行完整 TOML/业务校验、并发修改检查和备份，全部通过后才原子替换正式配置，校验失败不会损坏现有配置
- 首次安装会保留示例配置但禁用任务；填好 `server_id` / `secret` / `worker_url` 后执行 `.\deploy\install.ps1 start`
- 状态/停止：`.\deploy\install.ps1 status` / `.\deploy\install.ps1 stop`；Machine 模式需追加 `-Scope Machine`
- 卸载：`.\deploy\install.ps1 uninstall`（保留配置与数据，加 `-Purge` 全清）；Machine 模式需追加 `-Scope Machine`
- Release 资产名为 `probe-rs-windows-x86_64.exe`，可用 `-BinaryPath` 指向下载后的文件
- CF 与 Komari reporter 均可在 User 或 Machine 模式运行；协议与采集逻辑不依赖安装范围。Windows `cf-install.ps1` 为兼容既有部署仍明确使用 Machine 模式；User 模式可通过通用安装器和配置文件启用 CF/Komari reporter

## CF 协议模式（`[reporters.cf]` 段）

agent 可切换为 CF-Server-Monitor 的 `/update` 协议（HTTP POST 或 WSS，适配官方服务端，零改动对接）。

**配置**：`[reporters.cf]` 段（连接身份 🔒 本地，重启生效），命名对齐 cfsm-agent：`server_id` 填 CF 后台分配的 UUID，`secret` 填 `API_SECRET`，`url` 填 `https://<worker>/update`；`interval`（上报周期）、`collect_interval`、`reset_day`、`interface`、`ct/cu/cm/bd` 同原版语义。连接固定 auto（WSS 不可用时按原上报周期 POST 兜底），校正回路固定启用，上报固定 samples[] 批量——旧 `ext.cf.correction/batch/connection_mode` 开关已删除，安装脚本传入 `--connection-mode` 仅兼容解析、不再生效。

Windows 使用同一套 CF 协议逻辑；CF 一键安装默认启用 GPU 采集（无 `nvidia-smi` 时记录诊断但不影响其他指标）。可在管理员 PowerShell 中直接一键安装（`CollectInterval=0` 同样映射为内部 1 秒）：

```powershell
.\deploy\cf-install.ps1 install -Id <UUID> -Secret <API_SECRET> `
  -Url https://<worker>/update -CollectInterval 0 -Interval 60 -ConnectionMode auto
```

脚本默认下载 `probe-rs-windows-x86_64.exe`，也可用 `-BinarySource`/`-Bin` 指定本地文件或 URL；卸载使用 `.\deploy\cf-install.ps1 uninstall`，加 `-Purge` 可清除配置与流量数据。也可以通过通用 `deploy/install.ps1` 安装后，在 User 模式的 `%LocalAppData%\probe-rs\config.toml` 或 Machine 模式的 `%ProgramData%\probe-rs\config.toml` 中配置 CF reporter。

**上报映射**（reporter_cf.rs）：顶层 `{id, secret, config_schema, config_md5, collect_interval, report_interval, metrics, samples[]}`；ram/swap/disk 字节→MB；load 转空格字符串；GPU → `gpu_info:[{id,name,info}]`（`id` 来自采集端稳定设备标识，显存/温度丢弃，利用率未知的设备不输出）；ping 按组名落 `ping_ct/cu/cm/bd` + `loss_*`（bgp 是 bd 别名，未配置为 `false`，已配置但失败或缓存过期为 `null`）；`ip_v4/v6` 不可达报数值 `0`；`dynamic[]` → `samples[]`。顶层 dynamic/slow/GPU/Ping/disk I/O 快照按各自采集周期与上报周期校验新鲜度，过期字段不再输出；带 `ts` 的 `samples[]` 仍保留历史批量语义，report 不会触发采集。errors/self/virtualization 无落点，CF 模式下不产生。

**WSS 上报**：`auto` 模式把 `https/http` 的 `worker_url` 映射为 `wss/ws`，保留路径和业务查询参数，并用 query + Header 携带 Schema 5 和配置 MD5。握手必须收到 `type=hello, protocol=update`；连接建立或重连后先用 2 秒默认节奏发布与 POST 相同的最新 JSON 文本，随后接受 ACK 或 `realtimeHint` 的 `nextWssReportAfterMs` 动态调整到 1 秒至 5 分钟。无人查看面板时可按服务端提示降频，前端恢复实时订阅时由 hint 立即缩短间隔；hint 只改节奏，不推进 journal 游标。发送槽使用 `watch` 单值覆盖：socket 暂时变慢时只保留最新帧，不会堆积；被覆盖帧的 journal 游标不会提前推进，其记录会合并进替代帧。写出的帧只保留紧凑的游标元数据，后台收到对应 ACK 后才推进 journal；因此 ACK 决定数据确认，但不阻塞下一次发送。从最老未确认报告起连续 15 秒没有 ACK，或单次 socket 写入超过 5 秒，会主动关闭半开连接，随后停止 WSS 发布、按 `report_interval` POST 兜底并重连。`config` 和 `remote_config` 帧也由后台读循环独立处理，其中配置仍按 MD5 幂等校验和原子落盘。普通网络错误按 60 秒到 5 分钟指数退避；认证或配置类 `type=error` 策略帧会同时暂停 WSS 重连和 POST 至少 120 秒。CF 2.8.4 Beta7+ 的 WSS 时段关闭信号是例外：握手 HTTP `409`、`error code=409` 或 close code `1013` 仅在 reason 为 `wss_schedule_inactive` / `wss_schedule_empty` 时临时关闭运行时 WSS，保持本地 `connection_mode=auto` 并按 `report_interval` POST；后续 POST 响应头 `X-Agent-Wss-Mode: active` 会立即解除临时开关并唤醒 WSS actor。配置 body 的 `connection_mode` 字段不再应用（连接固定 auto），仅用于识别配置版本差异。

**配置下发**：请求头升级为 `X-Agent-Config-Schema: 5` + `X-Agent-Config-Md5`（复用 `config_version` 字段存 MD5，空 = `none`）。POST 响应或 WSS ack/config 帧中的 URL-encoded body 会解析 collect_interval/report_interval/wss_report_interval/reset_day/custom_ct/cu/cm/bd/interface/connection_mode，合成 `RemoteConfig`（config_version 取响应/帧 MD5）走 `apply_remote_for`；`wss_report_interval` 参与 Schema 5 配置识别和无 MD5 版本指纹，实际每帧节奏以 `nextWssReportAfterMs` 为准。`collect=0` 兼容映射为当前 CF Reporter 的 1 秒采集需求，随后参与机器级最小值聚合；逗号分隔的 interface 拆成多个过滤项。`custom_*` 字段缺席时保留对应 Ping，出现空值时清除；非空值只替换对应线路并保留原 interval，`bd` 兼容旧名 `bgp`，HTTP(S) URL 推断为 HTTP 探测，其余按 TCP 探测。落点限制：cf 段只能落 `collect_interval`/`interface`/`ct/cu/cm/bd`/`interval`（上报周期）/`reset_day`，远端推送其他可下发项（非 collect 子间隔、非空 disks、`report_gpu=false`、非四大线路 Ping 名）时整体拒绝。

**流量校正**：响应尾部 `rx_correction/tx_correction`（GB，覆盖当月累计）。netstatic 记账期偏移（offset = 校正字节 − 原始月累计，period_start 匹配才生效，翻页自动失效），立即落盘；校正确认用**独立请求**回传（CF 服务端见到 correction 字段会把整个请求当确认、丢弃 metrics），服务端清空后停止。`update=1`（自升级）永远忽略。

## komari 协议模式（`[reporters.komari]` 段）

对接 komari 面板的 WS v2 JSON-RPC（`/api/clients/v2/rpc?token=<token>`，`endpoint` 填面板地址）；段内命名对齐 komari-agent：`token`/`interval`（采集周期，komari 按采集周期上报）/`month_rotate`/`enable_gpu`/`include_nics`/`include_mountpoints`。

- **上行**：`agent.report`（最新值快照，字节单位；无 ts/批量语义，断线期间数据不保留）+ `agent.basicInfo`（持久保留最新一份，连接建立及静态信息变化时发送）；errors 事件拼进 report 的 `message` 字段。GPU 未采到的字段不输出，平均利用率只统计有效 usage
- **下行**：不执行远程控制调用，但**友好回绝**（不干等）：exec → POST task/result "Remote control is disabled."(exit -1)；terminal → 拨终端 WS 发说明即关闭（否则面板空转 30s）。我们从不调 agent.pull 声明远控能力
- **Ping**：收到 `agent.ping` 后按 `type + target` 写入该 Komari Reporter 的 `[reporters.komari.ext]` 下的 `learned_pings`；最多 5 个，按 `last_seen_at` LRU 淘汰。下发请求本身不探测，只读取全局 Ping worker 快照；首轮无缓存回 `-1`，配置热重建后后续任务返回新鲜缓存（最大年龄为本地 ping 周期的 2 倍，且至少 10s）。HTTP 裸 host 自动补 `http://`，path/query/fragment 仍拒绝
- komari 的月流量由面板自算；自动学习目标跟随全局 ping 周期，与其他 Reporter 目标统一去重聚合；无面板配置下发通道（仅 Ping 目标发现会写本地 TOML）
- **保活**：komari 服务端读超时 11s 且只有数据帧续期（WebSocket ping 无效）→ 每 5s 发送无参数、无 `id` 的 `agent.heartbeat` notification；不重发旧 report，也不调用 `agent.pull`
- 映射见 reporter_komari.rs（纯函数）；WS 机械（重连/心跳/下行忽略）在 worker/komari.rs
