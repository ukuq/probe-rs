# probe-rs 上报协议（完整版）

agent → 服务端的唯一数据通道。每个 report tick 发送一次。

## HTTP 请求

| 项 | 值 |
|---|---|
| Method | `POST` |
| Path | `/report` |
| Content-Type | `application/json` |
| `X-Secret` | 服务器密钥（认证，不出现在 body） |
| `X-Agent-Version` | agent 版本号 |

## 完整报文示例

```json
{
  "server_id": "srv-01",
  "config_version": "2026-08-06T15:30:45.123+08:00",

  "static": {
    "ts": 1754300050000,
    "os": "Debian GNU/Linux 12 (bookworm)",
    "kernel": "6.1.0-18-amd64",
    "arch": "x86_64",
    "cpu_name": "Intel(R) Xeon(R) Platinum 8375C",
    "cpu_cores": 8,
    "cpu_physical_cores": 4,
    "mem_total": 17179869184,
    "swap_total": 1073741824,
    "disk_total": 107374182400,
    "gpu_name": "NVIDIA A100 80GB",
    "virtualization": "kvm",
    "boot_time": 1754300000000,
    "ipv4": "203.0.113.10",
    "ipv6": "2001:db8::10",
    "agent_version": "0.1.0",
    "config": {
      "reset_day": 1,
      "intervals": { "collect": 10, "report": 60, "ping": 30, "slow": 60, "gpu": 60, "ip": 600, "diskio": 10 },
      "interfaces": ["eth*"],
      "enable_gpu": true,
      "report_errors": true,
      "report_self": false,
      "pings": [ { "name": "ct", "target": "gd-ct-dualstack.ip.zstaticcdn.com:80", "interval": 30 } ],
      "ext": { "cf": { "correction": true, "batch": true } }
    }
  },

  "dynamic": [
    {
      "ts": 1754300060000,
      "cpu_usage": 12.35,
      "mem_used": 4294967296,
      "swap_used": 134217728,
      "load": [0.52, 0.41, 0.30],
      "net_rx": 1073741824,
      "net_tx": 536870912,
      "net_rx_speed": 102400,
      "net_tx_speed": 51200,
      "net_rx_monthly": 858993459200,
      "net_tx_monthly": 429496729600
    }
  ],

  "async": [
    { "kind": "ping", "ts": 1754300058000, "name": "telecom", "rtt": 32, "loss": 0 },
    { "kind": "slow", "ts": 1754300055000, "disk_used": 53687091200, "tcp_conn": 120, "udp_conn": 8, "processes": 230 },
    { "kind": "gpu",  "ts": 1754300050000, "name": "NVIDIA A100 80GB", "usage": 42.5 },
    { "kind": "diskio", "ts": 1754300056000, "read_bps": 1048576, "write_bps": 524288, "read_iops": 40, "write_iops": 18, "await_ms": 1.8, "util": 3.2 }
  ],

  "errors": [
    { "ts": 1754300055000, "source": "gpu", "msg": "nvidia-smi exit 1" },
    { "ts": 1754300058000, "source": "ping:cu", "msg": "dns resolve failed" }
  ]
}
```

## 根字段

| 字段 | 类型 | 必带 | 说明 |
|---|---|---|---|
| `server_id` | string | ✅ | 服务器 ID，本地配置 |
| `config_version` | string | ✅ | 当前配置版本（人类可读的 UTC+8 时间戳）；服务端比对后决定是否下发新配置 |
| `static` | object | 条件 | 启动首报必带；之后每 10 分钟或内容变化时携带；省略时服务端保留旧值 |
| `dynamic` | array | ✅ | 同步采集记录（fast 字段），可为空数组（空也必报，承担心跳） |
| `async` | array | ✅ | 异步记录（kind 区分来源），可为空数组 |
| `errors` | array | ✅ | 错误事件，空数组 = 无错误 |

## static 字段

| 字段 | 类型 | 单位 | 来源 | 说明 |
|---|---|---|---|---|
| `ts` | number | 毫秒时间戳 | — | static 信息采集时刻 |
| `os` | string | — | /etc/os-release PRETTY_NAME | |
| `kernel` | string | — | uname -r | |
| `arch` | string | — | 运行时 | x86_64 / aarch64 等 |
| `cpu_name` | string | — | /proc/cpuinfo | |
| `cpu_cores` | number | 个 | 运行时 | 逻辑核数 |
| `cpu_physical_cores` | number \| null | 个 | /proc/cpuinfo | 物理核数，未知为 null |
| `mem_total` | number | 字节 | /proc/meminfo | |
| `swap_total` | number | 字节 | /proc/meminfo | |
| `disk_total` | number | 字节 | statfs 按设备去重求和 | 扩容后靠周期刷新更新 |
| `gpu_name` | string \| null | — | nvidia-smi 等 | 无 GPU 为 null |
| `virtualization` | string \| null | — | systemd-detect-virt 等 | 物理机为 null |
| `boot_time` | number | 毫秒时间戳 | /proc/stat btime | |
| `ipv4` | string \| null | — | cloudflare trace (tcp4) | 查询失败保留旧值 |
| `ipv6` | string \| null | — | cloudflare trace (tcp6) | 无 v6 出口为 null |
| `agent_version` | string | — | 编译注入 | |
| `config` | object | — | 当前生效配置 | 供服务端展示/核对，字段见下 |

### static.config 字段（当前生效配置回执）

| 字段 | 类型 | 说明 |
|---|---|---|
| `reset_day` | number | 0-31 |
| `intervals` | object | {collect, report, ping, slow, gpu, ip, diskio}（秒） |
| `interfaces` | string[] | 网卡白名单（glob）；空 = 所有非虚拟网卡 |
| `enable_gpu` | boolean | GPU 采集开关 |
| `report_errors` | boolean | 是否上报 errors 错误事件 |
| `report_self` | boolean | 是否上报探针自身占用 kind:"self" |
| `pings` | array | 探测目标组：`[{name, target, interval?}]` |
| `ext` | object | 协议扩展 `{cf: {correction, batch}}`（仅 cf 协议生效） |

## dynamic 记录字段（每条 = 一次 collect tick）

| 字段 | 类型 | 单位 | 来源 | 说明 |
|---|---|---|---|---|
| `ts` | number | 毫秒时间戳 | — | **采集时刻**，非上报时刻 |
| `cpu_usage` | number \| null | % (0-100) | /proc/stat 差值 | 首轮无前值时为 null |
| `mem_used` | number \| null | 字节 | /proc/meminfo | total − MemAvailable |
| `swap_used` | number \| null | 字节 | /proc/meminfo | |
| `load` | number[3] \| null | — | /proc/loadavg | `[load1, load5, load15]` |
| `net_rx` | number \| null | 字节 | /proc/net/dev | 开机起累计，白名单网卡求和 |
| `net_tx` | number \| null | 字节 | 同上 | |
| `net_rx_speed` | number \| null | 字节/秒 | 计数器差值 ÷ 时间差 | 首轮无前值时为 null |
| `net_tx_speed` | number \| null | 字节/秒 | 同上 | |
| `net_rx_monthly` | number \| null | 字节 | netstatic 现查 | 当前账期累计；reset_day=0 时为永久累计 |
| `net_tx_monthly` | number \| null | 字节 | 同上 | |

## async 记录字段（每条 = 一次异步测量，kind 区分来源）

每条都有 `kind` 与 `ts`（**该条的真实测量时刻**，与 dynamic 的 ts 无关）；新增异步源只需新增 kind，协议不变。
kind 按数据语义划分（DESIGN.md §2.3"机制同类、语义分流"）：slow/gpu 是不同存在性与故障域的指标，公网 IP 属身份信息故在 static 而非此数组。

| kind | 其余字段 | 说明 |
|---|---|---|
| `ping` | `name`, `rtt`, `loss` | 探测结果：name = `[[pings]]` 组 key；rtt 毫秒，**-1 = 失败**；loss 0-100 |
| `slow` | `disk_used`, `tcp_conn`, `udp_conn`, `processes` | 慢变指标（disk_used 与 disk_total 同口径；TCP 全状态计数） |
| `gpu` | `name`, `usage`, `mem_total`, `mem_used`, `temp` | GPU 型号、利用率（0-100）、显存（字节）、温度（℃）；多卡时每卡一条；mem/temp 仅 nvidia 路径有，macOS 为 null |
| `self` | `cpu_usage`, `mem_rss` | 探针自身 CPU（单核 %）与常驻内存（字节）；`report_self=true` 才产生（默认 false） |
| `diskio` | `read_bps`, `write_bps`, `read_iops`, `write_iops`, `await_ms`, `util` | 磁盘 IO（整盘合计）：bps 字节/秒、await 毫秒、util %；首轮差值无前值为 null；macOS 无 util（恒 null） |

约定：
- 异步记录**仅当对应源的快照 ts 更新时才产生**（同一份异步数据不会重复出现）；worker 失败 = 快照 ts 停滞；
- 异步数据粒度被 collect 间隔截断：一个 collect 周期内的多次异步更新只保留最新一次；
- 两个数组 ts 语义各自单一：dynamic 一律 tick 时刻，async 一律各自测量时刻；展示归并（按 kind/字段取最新）是服务端/前端职责；
- 各数组内按 ts 升序排列；
- 单项采集失败该字段为 null，不影响其他字段；
- 上报失败的记录由 agent 保留待重发（有界：每类缓冲 10 条，超限丢最旧——只覆盖短暂抖动，长断网历史不补发），服务端可能收到延迟补发的记录，按 ts 去重/排序即可。

## errors 记录字段（每条 = 一次采集/上报失败）

| 字段 | 类型 | 说明 |
|---|---|---|
| `ts` | number | 发生时刻，毫秒时间戳 |
| `source` | string | 来源：`gpu` / `ip` / `reporter` / `ping:<组名>` |
| `msg` | string | 错误信息 |

约定：**同源同文去重**（同一来源上一条信息相同则跳过，防止周期性失败刷屏）；缓冲上限 200 条，超限丢最旧；上报失败后与数据一起保留重发。

## 响应（服务端 → agent）

无配置变更时返回 `200 OK`，body 为空或 `{}`。

有配置变更时：

```json
{
  "config": {
    "config_version": "2026-08-06T16:00:00.000+08:00",
    "reset_day": 15,
    "intervals": {
      "collect": 10,
      "report": 60,
      "ping": 30,
      "slow": 60,
      "gpu": 60,
      "ip": 600,
      "diskio": 10
    }
  }
}
```

| 字段 | 约束 |
|---|---|
| `config_version` | 与 agent 当前版本**不等**才应用（幂等；人类可读时间戳无可靠大小语义，故用不等判断） |
| `reset_day` | 账期重置日 1-31；0 = 不重置 |
| `intervals.collect` | 采样间隔（秒），>= 1；CF 的 0 输入兼容映射为 1 |
| `intervals.report` | 上报间隔（秒），>= 1；与 collect 无任何关系约束 |
| `intervals.ping` | 探测间隔（秒），>= 1；`[[pings]]` 组未设 interval 时的默认 |
| `intervals.slow` | 慢变指标采集间隔（秒），>= 1，缺省 60 |
| `intervals.gpu` | GPU 采集间隔（秒），>= 1，缺省 60 |
| `intervals.ip` | 公网 IP 查询间隔（秒），>= 1，缺省 600 |
| `intervals.diskio` | 磁盘 IO 采集间隔（秒），>= 1，缺省 10 |
| `interfaces` | 可选；网卡白名单（glob 数组） |
| `enable_gpu` | 可选；GPU 采集开关（布尔） |
| `report_errors` | 可选；是否上报 errors 错误事件（布尔，缺省 true） |
| `report_self` | 可选；是否上报探针自身资源占用 kind:"self"（布尔，缺省 false） |
| `pings` | 可选；探测目标组数组，整体替换：`[{name, target, interval?}]`，name 唯一键不可重复 |
| `ext` | 可选；协议扩展 `{cf: {correction?, batch?}}`，仅对应协议启用时生效 |

`config` 内全部字段可选：出现的才应用，缺席的保持现值。pings/interfaces/enable_gpu 应用后由配置 supervisor 重建对应 worker（即时）。

配置收在 `config` 一级（信封后续可扩展其他指令）；`config` 缺席表示无变更。

agent 行为：校验 `config_version` 与基本合法性（间隔 >= 1、reset_day 0-31），全部通过才应用并落盘；任何一项非法则整体拒绝并记录日志。应用后立即生效，无需重启。

**🔒 不允许远端修改**：`server_id` / `secret` / `worker_url` / `net_static_path`（身份与安全边界，只接受本地配置）。

## 服务端判定规则（约定）

| 场景 | 判定依据 |
|---|---|
| agent 离线 | 超过 N × report 间隔未收到任何上报（空数组上报也算在线） |
| 某项数据没采成功 | 对应异步块的 ts 停止前进（slow/pings/gpu）；fast 字段为 null |
| 数据没变 | 有新记录、值相同 |
| 月流量异常 | 客户端自算自报，服务端不参与统计与重置 |
