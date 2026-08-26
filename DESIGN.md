# probe-rs 设计书

服务器监控探针。采集服务器指标，通过 HTTP POST 上报到中心服务端。
参考项目：komari-agent（采集规则、netstatic 流量统计）、cfsm-agent（远端配置下发机制）。

相关文档：[REPORT.md](REPORT.md)（上报协议完整字段定义）、[IMPL.md](IMPL.md)（Rust 实现方案）。

## 1. 总体架构

```
┌─────────────────────────────────────────────────┐
│ shared collectors                               │
│  └─ collect ticker (所有 Reporter 需求的最小值) │
│       └─ sync collectors → shared event log     │
├─────────────────────────────────────────────────┤
│ async workers（各自独立节奏，只发布最新快照）      │
│  ├─ ping worker      (type+目标聚合后的独立间隔)│
│  ├─ slow/gpu/ip/diskio（各路需求取最小值）      │
│  └─ netstatic        (2s 采样 / 10min 落盘)      │
├─────────────────────────────────────────────────┤
│ [[reporters]]（每路独立）                        │
│  ├─ report ticker (report_interval)             │
│  ├─ 独立事件游标 / ACK / 重试                    │
│  └─ probe / CF / Komari → 各自服务端             │
└─────────────────────────────────────────────────┘
```

核心原则：

1. **采集分三类**：静态（低频变化）/ 动态-同步（高频变化、采集便宜）/ 动态-异步（采集贵或有网络依赖）。
2. **采集与上报分离**：全局采集频率与每个 Reporter 的上报频率完全解耦，二者没有任何关系约束；每路按自己的游标读取共享事件，互不 drain 对方的数据。
3. **动态数据用数组**：每次采集追加一条带 `ts` 的记录，上报时整体携带并清空。
4. **异步只发快照**：异步 worker 只保留最近一次测量（带真实测量 ts）；采集端按 ts 新鲜度摘取——快照 ts 变了才带入记录，同一份异步数据不会重复出现，失败表现为 ts 停滞。
5. **需求聚合、出口分流**：每个 Reporter 声明自己的采集周期、Ping、网卡和磁盘需求；机器级周期取最小值，GPU 取 OR，网卡/磁盘/Ping 取并集。内部逐项采集，出口再按 Reporter 过滤并重算合计。

### 1.1 本地配置结构（schema = 1）

- 顶层：`schema`（结构版本；文件缺省该键 = 旧版 schema 0，启动时自动迁移、备份后回写为 1）、`data_dir`（运行态数据目录，net_static.json 等）、`auto_update`。
- 每条 `[[reporters]]` 只含 `id` + 一个协议段（`[reporters.cf]` / `[reporters.komari]` / `[reporters.probe]`），协议由出现的段决定；连接身份在段内，变更需重启。
- 协议段内原生参数命名对齐原版 agent：cf 对齐 cfsm-agent（`server_id/secret/url/interval/collect_interval/reset_day/interface/ct/cu/cm/bd`）；komari 对齐 komari-agent（`endpoint/token/interval/month_rotate/enable_gpu/include_nics/include_mountpoints`）；probe 是原生完整形态（含 `intervals/interfaces/disks/report_gpu/pings` 与 `report_errors/report_self`）。
- 采集需求仍按"每路声明 + 聚合"：各协议段先转换为统一的采集配置实体（即 probe 段的采集字段），再按 §1 原则 5 的规则合并出机器级实际配置。
- Agent 托管状态放在 `[reporters.<协议>.ext]` 下：cf/probe 的 `config_version`、komari 的 `learned_pings`；不进示例，勿手填。
- `report_errors/report_self` 只有 probe 段可配；cf/komari 线固定 errors=true、self=false。cf 线固定启用 GPU、固定 samples[] 批量上报、固定 auto（WSS+POST 回退）连接。

## 2. 指标分类

### 2.1 静态（static）

启动时全量上报一次，之后每 10 分钟周期刷新；服务端在字段缺失时保留旧值。

| 字段 | 来源 | 备注 |
|---|---|---|
| `os` | /etc/os-release | |
| `ts` | — | static 信息采集时刻，毫秒时间戳；缓存生成时绑定当时校准结果 |
| `kernel` | uname -r | |
| `arch` | 编译/运行时 | |
| `cpu_name` | /proc/cpuinfo | |
| `cpu_cores` | 逻辑核数 | |
| `cpu_physical_cores` | 物理核数 | 可选 |
| `mem_total` | /proc/meminfo | 字节 |
| `swap_total` | /proc/meminfo | 字节 |
| `disk_total` / `disks` | statfs 去重 | 当前 Reporter 选中卷的合计与逐卷 `{id,name,mount_point,file_system,total,used}`；扩容靠周期刷新 |
| `gpu_name` | nvidia-smi 等 | |
| `virtualization` | systemd-detect-virt 等 | |
| `boot_time` | /proc/stat btime | 毫秒时间戳；缓存生成时绑定当时校准结果 |
| `ipv4` / `ipv6` | cloudflare trace | 公网 IP，会变，靠周期刷新 |
| `agent_version` | 编译注入 | |
| `config` | 全局采集摘要 + Reporter 拓扑 + 当前 Reporter 生效配置 | `global` 是实际 collector/async worker 配置；`reporters[]` 是全部线路的脱敏输出策略；同级 intervals/reset_day/... 保持当前 Reporter 视角，供服务端展示/核对 |

`static.config.reporters[]` 只带 `id/protocol`、采集需求与输出策略，不带 `secret`、`worker_url`、其他线路的 `server_id/config_version`。接收端只能给产生当前请求的 Reporter 下发配置；Agent 原子应用该路后重新聚合机器级实际配置，避免多路直接争用一份全局配置。

### 2.2 动态-同步（dynamic，每个 collect tick 内联采集）

全部是本地文件读取 / syscall，微秒级，无网络 IO。只含 fast 字段（ts = tick 时刻）；异步数据经快照新鲜度摘取后进入独立的 `async[]`。

| 字段 | 来源 | 备注 |
|---|---|---|
| `ts` | 本机墙钟 | collect tick 的本地毫秒时间 |
| `accurate_ts` | Agent 时钟 | 可选；该 tick 采集时保存的校准毫秒时间，首次校准前缺席 |
| `cpu_usage` | /proc/stat 差值 | 百分比，0-100 |
| `mem_used` | /proc/meminfo | 字节，total − MemAvailable |
| `swap_used` | /proc/meminfo | 字节 |
| `load` | /proc/loadavg | `[load1, load5, load15]` 数组 |
| `net_rx` / `net_tx` | /proc/net/dev | 字节，开机起累计 |
| `net_rx_speed` / `net_tx_speed` | 计数器差值 ÷ 时间差 | 字节/秒，主采集内计算，不经过 netstatic |
| `net_rx_monthly` / `net_tx_monthly` | netstatic 现查 | 字节，账期累计，见 §5 |
| `net_interfaces` | 逐网卡计数器 + netstatic | 当前 Reporter 选中的逐网卡累计、速率与账期流量；兼容合计字段由其求和 |
（异步数据不进 dynamic，见 §2.3 与 §3 的 `async[]`）

网卡过滤：内部采集全部默认物理网卡；每个 Reporter 用 glob（如 `eth*`）独立筛选，空数组表示全部。默认排除 `br/cni/docker/podman/flannel/lo/veth/virbr/vmbr/tap/fwbr/fwpr` 前缀的虚拟网卡。

磁盘口径：遍历挂载点 statfs，排除虚拟/网络文件系统（tmpfs/overlay/nfs 等）与 `/tmp`、`/var/lib/docker` 等路径前缀，按设备 ID 去重（ZFS 按 pool 名截断）。内部保留逐卷/逐物理盘；每个 Reporter 用 `disks` glob 筛选，再重算容量和 IO 兼容合计。Windows 容量通常按盘符，IO 使用 `PhysicalDisk(*)` 逐物理盘计数器。

### 2.3 动态-异步（async worker，只发布最新快照）

**两层分类：机制同类，语义分流。** 所有异步 worker 在机制上完全相同（独立 ticker → 采集 → 发快照 → 采集端按 ts 新鲜度摘取），它们的间隔统一归 `intervals` 管理；分流发生在数据出口，按"数据是什么"决定落点：

| 数据 | 语义 | 存在性 | 故障域 | 落点 |
|---|---|---|---|---|
| disk / conn / procs | 系统状态指标（与 dynamic 的 cpu/mem 同族，只是变化慢、采集贵） | 每台机器必有 | 本机采集问题 | `kind:"slow"` |
| gpu 利用率 | 可选硬件指标（多卡时每卡一条，形状不同） | 仅部分机器 | nvidia-smi/ioreg 不可用 | `kind:"gpu"` |
| 公网 IP | 身份信息（"你是谁"，不是被测量的指标） | 依赖外网 | 外网不通 ≠ agent 坏 | **static**，不进 async[] |
| ping rtt/loss | 主动探测指标（目标在配置里，多组各自节奏） | 看配置 | 目标不可达 | `kind:"ping"` |
| 磁盘 IO 速率 | 系统指标但各平台采集成本悬殊（Linux 免费 / macOS 子进程） | 必有硬盘 | 本机采集问题 | `kind:"diskio"` |

| worker | 节奏 | 采集内容 | 快照 |
|---|---|---|---|
| ping | 聚合后每目标独立 interval | 显式 `http/tcp/icmp`；每轮 DNS 先解析一次，4 次测量与重试复用 IP；一轮取中位数 + 丢包率 | `HashMap<task_id, PingRecord>` |
| slow | 实际 `intervals.slow` | 逐卷 disks + disk_used / tcp_conn / udp_conn / processes | `SlowBlock` |
| gpu | `intervals.gpu`（缺省 60s） | GPU 名称 + 使用率（nvidia-smi；macOS 走 system_profiler + ioreg，可本地开关） | `Vec<GpuRecord>` |
| diskio | 实际 `intervals.diskio` | 逐物理盘 IO 速率/iops/await/usage + 聚合值（Linux /proc/diskstats；Windows PDH；macOS ioreg） | `DiskIoRecord` |
| public-ip | `intervals.ip`（缺省 600s） | 公网 IPv4/IPv6（cloudflare trace，强制 tcp4/tcp6 分流） | `(ipv4, ipv6)`，供 static |
| netstatic | 2s 采样 / 10min 落盘 | 每网卡流量 delta 时序，见 §5 | 可查询时序 |

**快照规则：只保留最近一次，ts 为真实测量时刻。**

- 采集端为每个异步源记录"上次摘取的 ts"，快照 ts 更新才产生一条 `async[]` 记录（kind 标记来源）——同一份异步数据不会重复出现；
- 一个 collect 周期内的多次异步更新只保留最新一次（粒度 = max(collect, 异步间隔)）；
- 异步 worker 失败 → 快照不更新 → ts 停滞，服务端凭 ts 停滞识别"没采成功"；有记录但值相同 = 没变；
- 主采集永不阻塞于异步 worker。

ping 防重传规则（沿用 komari/cfsm）：单次延迟 >1000ms 时重测最多 3 次；TCP 探测若重测降幅 >800ms，判定为 SYN 重传污染，本次记为失败。

Ping 去重键是“类型 + 规范化目标”：TCP 为小写 host + 有效端口，ICMP 为小写 host；HTTP 为 scheme + 小写 host + 有效端口，HTTP 与 HTTPS 不合并。HTTP target 仅允许 `http(s)://host[:port]`（根 `/` 等价可接受），所有类型均禁止 path/query/fragment。重复任务周期取最小值（而非最大公约数）：最小值直接满足每个消费者要求且不会产生比任何一路更密的额外采样。结果与错误在出口恢复为该 Reporter 自己的逻辑名称。

## 3. 上报模型

完整协议定义（含全部字段类型/单位/来源）见 [REPORT.md](REPORT.md)，本节只讲规则。

### 3.1 报文结构

```json
{
  "server_id": "server-xxx",
  "config_version": "2026-08-06T15:30:45.123+08:00",
  "time": { "local_ts": 1754300060123, "accurate_ts": 1754300060000,
    "offset_ms": -123, "source": "ntp:time.cloudflare.com",
    "round_trip_ms": 18, "sample_age_ms": 29982 },
  "static": { "ts": 1754300050000, "os": "Debian 12", "...": "..." },
  "dynamic": [
    { "ts": 1754300060000, "accurate_ts": 1754300059877,
      "cpu_usage": 12.3, "mem_used": 4294967296,
      "net_rx_speed": 102400, "net_tx_speed": 51200, "...": "..." }
  ],
  "async": [
    { "kind": "ping", "ts": 1754300058000, "name": "telecom", "rtt": 32, "loss": 0 },
    { "kind": "slow", "ts": 1754300055000, "disk_used": 53687091200, "tcp_conn": 120, "processes": 230 },
    { "kind": "gpu",  "ts": 1754300050000, "id": "0", "name": "NVIDIA A100", "usage": 42.5 }
  ]
}
```

认证：`secret` 通过 HTTP header `X-Secret` 携带，不出现在 body（避免日志/抓包泄露）。

### 3.2 规则

- **每个 report tick 必报**：`dynamic` 为空数组也照发，天然承担心跳职能，服务端按"最后收到时间"判离线。
- **Agent 级时间校准**：Agent 启动后立即执行一次独立 NTP 任务，此后每 10 分钟刷新，不依赖 Reporter 上报周期；全部 Reporter 共享校准结果。每轮并行查询 Cloudflare / Google / NIST / Aliyun NTP；每个域名并发尝试全部 IPv4/IPv6 地址，再按偏差中位数选源并生成单调时钟锚点；UDP/123 不通时才回退到原生响应 `server_time`。NTP 的 32 位秒字段按本次请求时间解析 era，跨 2036 回绕仍保持正确。原生上报同时带本地时间、准确时间、差值与来源；CF 当前时间在组包时校准，static 与 dynamic 时间在各自生成时绑定校准结果；Komari 的 uptime 在统一纠正时间域计算。Agent 不修改系统时钟。
- **`static` 可省略**：未到期且无变化时不带，服务端保留旧值。
- **`dynamic` 每条带 `ts` / 可选 `accurate_ts`**：`ts` 是采集时本地墙钟，`accurate_ts` 是同一 tick 保存的校准时间；不是上报时刻，重试时也不重算。
- **三段结构**：`static` obj + `dynamic[]`（fast，ts = tick 时刻）+ `async[]`（kind 区分来源，ts = 各自测量时刻）——两个数组的 ts 语义各自单一，异步频率互不迁就。
- **异步按新鲜度产生**：异步记录仅当对应源快照 ts 更新时才进入 `async[]`（见 §2.3 规则）；worker 失败 = ts 停滞，有记录但值相同 = 没变。
- **月流量/累计值**：放在 `dynamic` 记录中带当前值（上报时向 netstatic 现查）；netstatic 明细及每条 dynamic 的查询窗口均使用采集时的 Agent 时间，避免本地墙钟跳变跨越 `reset_day` 时错账期。
- **上报失败有界保留**：失败的记录留在共享事件日志中待下次重发；日志上限 512 条（dynamic/async/errors 三类共享一个 journal），超限丢最旧并注入一条 `source=buffer` 错误事件（只覆盖短暂抖动，长断网历史不补发）。
- **数据陈旧判断交给服务端**：动态数据有 `ts`，静态数据本来不变，不报 `measured_at` 之类的额外字段。
- **配置回执带完整 Ping 定义**：`static.config.global.pings` 是按 type + 规范化 target 聚合后的无 name/type 实际 worker 配置 `{target,interval}`，类型编码进 URI target（如 `tcp://host:80`、`https://host:443`、`icmp://host`），周期取各路最小值；`static.config.reporters[].pings` 保留每路原始 name/type/target/interval。target 用于配置核对，不脱敏；secret、worker_url 及其他线路身份仍不回传。

### 3.3 数据口径约定

- 容量/流量一律**字节**，速率一律**字节/秒**，数值用 JSON number（不用字符串）。
- 时间戳一律**毫秒**（含 netstatic 条目、boot_time、各数组的 ts 与原生协议 `time`）。
- 百分比（cpu_usage、gpu usage、loss）为 0-100 的 number。
- 单项采集失败：该字段置 null，不中断本轮采集与上报。

### 3.4 命名规范

| 规范 | 说明 |
|---|---|
| 方向词汇 | 一律 `rx`（下行/接收）/ `tx`（上行/发送），全文档与代码禁用 in/out、up/down |
| 探测术语 | 一律 `ping`（worker、intervals.ping、ping[] 数组），禁用 probe 指代该子系统 |
| 配置 key | snake_case；间隔类收敛到 `intervals.{collect, report, ping, slow, gpu, ip, diskio}`（注意两层形态：Reporter 需求为六项 `intervals.{collect,ping,slow,gpu,ip,diskio}` + 独立 `report_interval`；static.config 回执中的 `intervals` 为七项、含 `report`，两者是不同结构） |
| 使用率字段 | 后缀 `_usage`（`cpu_usage`、gpu 记录的 `usage`），与 `_name`/`_total`/`_used` 后缀风格一致 |
| 时间字段 | 一律 `ts`（毫秒）；静态开机时间用 `boot_time` |
| 账期字段 | 后缀 `_monthly` 表示账期累计（reset_day=0 时为永久累计） |

术语表：

| 术语 | 定义 |
|---|---|
| static | 静态信息，低频上报，服务端保留旧值 |
| dynamic | 同步采集的动态指标数组，每个 collect tick 一条 |
| ping | 网络探测子系统及其结果数组 |
| collect tick | 采样周期触发点，间隔 `intervals.collect` |
| report tick | 某一路 Reporter 的上报周期触发点，间隔 `reporters[].report_interval` |
| 账期 | 以 `reset_day` 为起点的月流量统计周期 |
| netstatic | 网卡流量 delta 时序模块（§5） |

## 4. 远端动态配置

### 4.1 可下发项（第一版仅这些）

```json
{
  "config": {
    "config_version": "2026-08-06T16:00:00.000+08:00",
    "intervals": { "collect": 2, "ping": 60, "slow": 60, "gpu": 60, "ip": 600, "diskio": 10 },
    "report_interval": 60,
    "reset_day": 15,
    "interfaces": ["eth*"],
    "disks": ["nvme*"],
    "pings": [ { "name": "edge", "type": "icmp", "target": "1.1.1.1", "interval": 60 } ],
    "report_gpu": true,
    "report_errors": true,
    "report_self": false
  }
}
```

| 项 | 约束 |
|---|---|
| `intervals` | 当前 Reporter 的六项采集需求，均 >= 1；应用后参与机器级最小值聚合 |
| `report_interval` | 当前 Reporter 上报间隔（秒），>= 1；与全局 collect 无任何关系约束 |
| `reset_day` | 月流量账期重置日，1-31；0 = 不重置（永久累计） |
| `interfaces` | 当前 Reporter 的网卡白名单（glob 数组） |
| `disks` | 当前 Reporter 的卷/物理盘白名单（glob 数组） |
| `pings` | 当前 Reporter 的整组 Ping 需求（显式 type/target/interval）；target 不允许 path/query/fragment |
| `report_gpu` | 当前 Reporter 是否输出 GPU，并参与机器级 GPU worker 的 OR 聚合 |
| `report_errors` | 当前 Reporter 是否输出错误事件 |
| `report_self` | 当前 Reporter 是否输出探针自身指标 |
| `config_version` | 配置版本（人类可读的 UTC+8 时间戳字符串），幂等机制，见下 |

响应信封的 `server_time` 提供公共 NTP 不可用时的毫秒级兜底时间，`config` 收配置（除 config_version 外均可选，出现的才应用），`next` 放对下一次上报的指令（如 `next.static`）。响应只能修改产生该响应的 Reporter；机器级实际配置没有可直接写入口，而是每次从全部 Reporter 重新聚合，因此多路下发不会互相覆盖。🔒 连接身份与 `data_dir` 仍只接受本地 TOML。允许远端 Ping 时，服务端必须自行限制目标范围。

落点随协议段不同：probe 段全量落地（上表所有项）；cf 段只能落 `intervals.collect`（→ `collect_interval`）、`report_interval`（→ `interval`）、`reset_day`、`interfaces`（→ `interface` 串）、`pings`（仅 ct/cu/cm/bd 槽位），出现其他可下发项（非 collect 子间隔、非空 disks、`report_gpu=false`、非四大线路 Ping 名）整体拒绝，`report_errors/report_self` 对 cf 固定、直接忽略；komari 协议没有下发通道。

### 4.2 下发机制

- 搭上报便车：POST /report 的响应体携带 `server_time` 与可选配置对象；时钟校准不新增外部请求。
- agent 上报时带当前 `config_version`；服务端比对，不一致才下发。
- **幂等**：version 与本地一致则不应用。
- **原子**：整个配置对象全部校验通过才应用 + 落盘本地配置文件；任何一项非法（零值间隔、reset_day 越界等）整体拒绝，agent 日志记录原因。
- 应用后立即生效：重建所属 Reporter ticker，并按需调整共享 ticker、Ping/GPU worker；无需重启。

## 5. 流量统计（netstatic）

移植 komari-agent 的 netstatic 设计：时序明细 + 查询式统计。

**月流量完全由客户端计算**：netstatic 在 agent 本地维护，上报时现查后作为当前值放入 `dynamic`；服务端只存展示值，不参与流量统计与重置。

### 5.1 数据模型（net_static.json）

```json
{
  "interfaces": {
    "eth0": [
      {"ts": 1754300000000, "rx": 102400, "tx": 51200}
    ]
  },
  "archived_totals": {
    "eth0": {"through_ms": 1751621599999, "rx": 987654321, "tx": 123456789}
  }
}
```

- 每条是**相邻两次采样的 delta**（非累计值），`ts` 为毫秒时间戳。
- **按网卡分开存**：网卡白名单变更后可用历史明细重新求和，统计不出错。
- `archived_totals` 是移出明细窗口前按网卡折叠的永久基数；只参与 `reset_day=0` 查询，普通月账期仍使用带时间戳的明细。

### 5.2 运行机制

| 环节 | 参数 | 说明 |
|---|---|---|
| 采样 | 每 2 秒 | 读 /proc/net/dev 计数器算 delta，以当时 Agent 时间记 ts，只写内存 |
| 落盘 | 每 10 分钟 | 内存增量合并写入 JSON，崩溃最多丢 10 分钟 |
| 保留 | 32 天明细 + 永久归档基数 | 保留期必须严格大于最长账期（31 天），否则 reset_day 28-31 的账期首日明细会在账期内被归档、月流量永久少计；月账期可重算，`reset_day=0` 不会因明细裁剪丢失累计 |
| 查询 | `sum(period_start ≤ ts ≤ now)` | 月流量现查；`period_start=0` 时再加所选网卡的归档基数 |

### 5.3 增量正确性纪律（配置变更/重启不出错）

1. **delta 按网卡分别算、分别存**：`interfaces` 白名单变更只影响查询时的求和集合，各网卡增量序列不受影响，杜绝"聚合计数器跳变导致 diff 出垃圾值"。
2. **计数器回退 → 本轮 delta 记 0**：`current < prev`（重启/换卡）时不做减法，本轮记 0，宁可少记一轮，不出错误增量。
3. **崩溃只产生空洞，不产生错误**：落盘窗口内未保存的 delta 丢失表现为数据缺失，绝不会产生错误增量。
4. **reset_day 变更**：账期求和窗口改用新起点即可；31 天明细在手可精确重算（白送，非承诺），下一账期起自然全对；切换为 0 时使用从安装后持续保存的永久归档基数。

### 5.4 内存控制（实现时可选）

2s × 31 天 ≈ 134 万条/网卡，全放内存约 30MB/网卡。可将条目按小时合并压缩（小时内 delta 求和），内存降至 ~1MB，月统计精度不受影响。机制不变，实现时定。

## 6. 模块划分（Rust）

| 模块 | 职责 |
|---|---|
| `config` | 本地配置加载/校验;旧版(schema 0)配置迁移;远端配置应用、原子校验、落盘 |
| `model` | 上报报文与配置的数据结构定义(协议段 + 采集配置实体) |
| `collector/sync` | 平台门面 + Linux /proc 实现 + sysinfo 跨平台实现 |
| `collector/async` | 异步 worker 框架：独立 task + watch channel 快照 |
| `collector/netstatic` | 流量时序：采样、落盘、滚动保留、区间查询 |
| `scheduler` | 全局 collect ticker + 每 Reporter 独立 report ticker、事件游标和重试状态 |
| `buffer` | dynamic / async 共享有界事件日志，支持多路独立游标读取 |
| `reporter` | probe / CF / Komari 输出；响应解析并定向应用所属 Reporter 配置 |
| `updater` | 固定 GitHub Release 源；通道/SemVer 选择、SHA-256 校验、平台安全替换与重启 |

平台支持：Linux（手写 /proc 解析，零依赖）、macOS/Windows（sysinfo crate 实现，连接数解析 netstat）；`collector` 模块为平台门面，按 cfg 分流。Linux 默认从 `/etc/probe-rs/config.toml` 读取配置并把流量数据写到 `/var/lib/probe-rs/`；Windows 默认把配置和流量数据放在 `%ProgramData%\probe-rs\`，由 SYSTEM 开机计划任务托管。

## 7. 已明确的取舍（备忘）

| 决策 | 结论 | 理由 |
|---|---|---|
| 空 dynamic 是否上报 | **报** | 承担心跳职能 |
| 上报失败缓冲 | **有界保留（共享日志 512 条，超限丢最旧并告警）** | 只覆盖短暂抖动；长断网历史不值得补发 |
| 采集/上报频率关系 | **无约束**（仅要求 >= 1s） | report 时清空缓冲即可，采集节奏无需是上报的约数 |
| 同步采集频率 | **slow 指标异步化** | slow worker 独立节奏直写 dynamic，带真实测量 ts；杜绝"缓存值贴新 ts" |
| 异步数据模型 | **只发快照 + 新鲜度去重** | 异步只保留最近一次（带测量 ts）；采集端按 ts 变化摘取，天然去重 |
| 数据 ts 语义 | **每条数据带自己的测量 ts** | fast/异步块/static 全部如此；展示归并是前端职责 |
| 异步数据陈旧标记 | **不加** | 谁采集谁打 ts；失败即缺席，服务端凭空洞判断 |
| 月流量计算位置 | **客户端自算，服务端只存展示值** | 统计与重置不依赖服务端 |
| 流量统计底层 | **时序明细（非 KV 状态机）** | 按网卡存 delta，配置变更后新增增量始终正确；reset_day/interfaces 变更可重算 |
| 实时网速来源 | **主采集计数器差值** | 比 netstatic 2s 粒度更贴合采集周期 |
| 远端可下发项 | **仅所属 Reporter 的需求与输出策略** | 全局实际值没有写入口，每次从全部 Reporter 聚合，消除多路覆盖冲突 |
| 流量校正 | **客户端记账 + 服务端下发目标值** | netstatic 报诚实累计值；CF 协议的服务端校正以"覆盖当月累计"下发，agent 用 offset 记账（相同命令幂等，账期翻页自动失效），账务层职责仍在服务端 |
| 自升级 | **本地显式开关；仅固定 GitHub Release + SHA-256** | 不接受服务端 URL/命令，版本必须按 SemVer 严格增加 |
| 远程命令 | **不做** | CF/Komari 服务端不能触发命令执行或指定更新来源 |
| 单位/类型 | **字节 + JSON number** | komari 风格，不学 cfsm 全字符串 |
| 方向/探测命名 | **rx/tx、ping** | 避免 in-out/up-down、ping-probe 混用 |
