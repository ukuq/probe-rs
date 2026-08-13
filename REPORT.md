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
| `X-Reporter-Id` | 当前独立上报实例 id（百分号编码）；缺席兼容为 `primary` |
| `X-Reporter-Protocol` | 当前 Reporter 协议（百分号编码）；原始协议通常为 `probe` |

服务端应以 `server_id + X-Reporter-Id` 区分上报实例。这样同一 `server_id` 的多个原始协议 Reporter 不会串数据或共用远端配置；旧服务端可以安全忽略新增请求头。

## 完整报文示例

```json
{
  "server_id": "srv-01",
  "config_version": "2026-08-06T15:30:45.123+08:00",
  "time": {
    "local_ts": 1754300060123,
    "accurate_ts": 1754300060000,
    "offset_ms": -123,
    "source": "ntp:time.cloudflare.com",
    "round_trip_ms": 18,
    "sample_age_ms": 29982
  },

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
    "disks": [ { "id": "/dev/sda1", "name": "sda1", "mount_point": "/", "file_system": "ext4", "total": 107374182400, "used": 53687091200 } ],
    "gpu_name": "NVIDIA A100 80GB",
    "virtualization": "kvm",
    "boot_time": 1754300000000,
    "ipv4": "203.0.113.10",
    "ipv6": "2001:db8::10",
    "agent_version": "0.1.3-beta.5",
    "config": {
      "global": {
        "intervals": { "collect": 1, "ping": 30, "slow": 60, "gpu": 60, "ip": 600, "diskio": 10 },
        "enable_gpu": true,
        "interfaces": [], "all_interfaces": true,
        "disks": [], "all_disks": true,
        "pings": [ { "target": "tcp://gd-ct-dualstack.ip.zstaticcdn.com:80", "interval": 30 } ]
      },
      "reporters": [
        { "id": "primary", "protocol": "probe", "intervals": { "collect": 1, "ping": 30, "slow": 60, "gpu": 60, "ip": 600, "diskio": 10 }, "report_interval": 60, "reset_day": 1, "interfaces": ["eth*"], "disks": [], "report_gpu": true, "report_errors": true, "report_self": false, "pings": [ { "name": "ct", "type": "tcp", "target": "gd-ct-dualstack.ip.zstaticcdn.com:80", "interval": 30 } ] }
      ],
      "reset_day": 1,
      "intervals": { "collect": 1, "report": 60, "ping": 30, "slow": 60, "gpu": 60, "ip": 600, "diskio": 10 },
      "interfaces": ["eth*"],
      "disks": [],
      "enable_gpu": true,
      "report_errors": true,
      "report_self": false,
      "pings": [ { "name": "ct", "type": "tcp", "target": "gd-ct-dualstack.ip.zstaticcdn.com:80", "interval": 30 } ]
    }
  },

  "dynamic": [
    {
      "ts": 1754300060000,
      "accurate_ts": 1754300059877,
      "cpu_usage": 12.35,
      "mem_used": 4294967296,
      "swap_used": 134217728,
      "load": [0.52, 0.41, 0.30],
      "net_rx": 1073741824,
      "net_tx": 536870912,
      "net_rx_speed": 102400,
      "net_tx_speed": 51200,
      "net_rx_monthly": 858993459200,
      "net_tx_monthly": 429496729600,
      "net_interfaces": {
        "eth0": { "rx": 1073741824, "tx": 536870912, "rx_speed": 102400, "tx_speed": 51200, "rx_monthly": 858993459200, "tx_monthly": 429496729600 }
      }
    }
  ],

  "async": [
    { "kind": "ping", "ts": 1754300058000, "name": "ct", "rtt": 32, "loss": 0 },
    { "kind": "slow", "ts": 1754300055000, "disk_used": 53687091200, "disks": [ { "id": "/dev/sda1", "name": "sda1", "mount_point": "/", "file_system": "ext4", "total": 107374182400, "used": 53687091200 } ], "tcp_conn": 120, "udp_conn": 8, "processes": 230 },
    { "kind": "gpu",  "ts": 1754300050000, "name": "NVIDIA A100 80GB", "usage": 42.5 },
    { "kind": "diskio", "ts": 1754300056000, "read_bps": 1048576, "write_bps": 524288, "read_iops": 40, "write_iops": 18, "await_ms": 1.8, "usage": 3.2, "disks": [ { "name": "sda", "read_bps": 1048576, "write_bps": 524288, "read_iops": 40, "write_iops": 18, "await_ms": 1.8, "usage": 3.2 } ] }
  ],

  "errors": [
    { "ts": 1754300055000, "source": "gpu", "msg": "nvidia-smi exit 1" },
    { "ts": 1754300058000, "source": "ping:ct", "msg": "dns resolve failed" }
  ]
}
```

## 根字段

| 字段 | 类型 | 必带 | 说明 |
|---|---|---|---|
| `server_id` | string | ✅ | 服务器 ID，本地配置 |
| `config_version` | string | ✅ | 当前配置版本（人类可读的 UTC+8 时间戳）；服务端比对后决定是否下发新配置 |
| `time` | object | ✅ | 本机墙钟与原生服务端时间校准状态；字段见下 |
| `static` | object | 条件 | 启动首报必带；之后每 10 分钟或内容变化时携带；省略时服务端保留旧值 |
| `dynamic` | array | ✅ | 同步采集记录（fast 字段），可为空数组（空也必报，承担心跳） |
| `async` | array | ✅ | 异步记录（kind 区分来源），可为空数组 |
| `errors` | array | ✅ | 错误事件，空数组 = 无错误 |

## time 字段（原生协议时钟校准）

| 字段 | 类型 | 说明 |
|---|---|---|
| `local_ts` | number | 本次上报组包时的本机 Unix 毫秒墙钟 |
| `accurate_ts` | number \| null | 根据最近一次 NTP（优先）或服务端时间（兜底）与单调时钟推算的 Unix 毫秒时间；首次校准前为 null |
| `offset_ms` | number \| null | `accurate_ts - local_ts`；正数表示本机慢，负数表示本机快 |
| `source` | string \| null | `ntp:<host>`；UDP/123 不可用时为 `server`；首次校准前为 null |
| `round_trip_ms` | number \| null | 最近一次成功校准请求的往返耗时；误差量级约为其一半 |
| `sample_age_ms` | number \| null | 最近校准样本到本次组包的单调时钟年龄 |

Agent 不修改系统时间。Agent 启动后会立即运行一次独立的 NTP 校准任务，此后每 10 分钟刷新，不依赖任何 Reporter 的上报周期；全部 Reporter 共享校准结果。每轮并行查询 `time.cloudflare.com`、`time.google.com`、`time.nist.gov`、`ntp.aliyun.com`，每个域名会并发尝试 DNS 返回的全部 IPv4/IPv6 地址，再按成功服务器样本的偏差中位数选源。NTP 样本的 **24 小时寿命用于新鲜源选择**：超过 24 小时未刷新的样本不再参与 NTP/server 源优先级比较，但已建立的校准锚点继续沿单调时钟推进（不回退本机墙钟，避免时间域跳变）；UDP/123 被阻断或全部 NTP 查询失败时，才回退到原生服务端 `server_time`，并通过 `source` 明示。校准成功后，准确时间锚定在单调时钟上继续推进，因此本机墙钟随后发生跳变时，下一次上报的 `offset_ms` 会立即反映出来。CF 的 `timestamp` 使用组包时准确时间；`boot_time` 在 static 缓存生成时校准，`samples[].ts` 使用各 dynamic 记录采集时保存的 `accurate_ts`，重试不会用当前偏差移动旧记录。Komari 的 `uptime` 使用准确当前时间与缓存生成时已校准的启动时间。公共 NTP 是未认证 UDP，多源中位数能排除单个异常源，但不能抵御控制本机网络的攻击者。

## static 字段

| 字段 | 类型 | 单位 | 来源 | 说明 |
|---|---|---|---|---|
| `ts` | number | 毫秒时间戳 | — | static 信息采集时刻；缓存生成时按当时校准结果换算 |
| `os` | string | — | /etc/os-release PRETTY_NAME | |
| `kernel` | string | — | uname -r | |
| `arch` | string | — | 运行时 | x86_64 / aarch64 等 |
| `cpu_name` | string | — | /proc/cpuinfo | |
| `cpu_cores` | number | 个 | 运行时 | 逻辑核数 |
| `cpu_physical_cores` | number \| null | 个 | /proc/cpuinfo | 物理核数，未知为 null |
| `mem_total` | number | 字节 | /proc/meminfo | |
| `swap_total` | number | 字节 | /proc/meminfo | |
| `disk_total` | number | 字节 | statfs 按设备去重求和 | 扩容后靠周期刷新更新 |
| `disks` | array | — | statfs / sysinfo | 当前 Reporter 选中的逐卷 `{id,name,mount_point,file_system,total,used}`；`disk_total` 为其 total 合计 |
| `gpu_name` | string \| null | — | nvidia-smi 等 | 无 GPU 为 null |
| `virtualization` | string \| null | — | systemd-detect-virt 等 | 物理机为 null |
| `boot_time` | number \| null | 毫秒时间戳 | /proc/stat btime | static 缓存生成时按当时校准结果换算，之后重试不重新移动；采集失败为 null |
| `ipv4` | string \| null | — | cloudflare trace (tcp4) | 查询失败保留缓存；测量时间戳超过新鲜度窗口（ip + min(ip, report)）后该字段不输出（置 null），不会永久上报旧 IP |
| `ipv6` | string \| null | — | cloudflare trace (tcp6) | 无 v6 出口为 null |
| `agent_version` | string | — | 编译注入 | 原生为版本号；CF 出站格式为 `<版本>_probe-rs` |
| `config` | object | — | 当前生效配置 | 供服务端展示/核对，字段见下 |

### static.config 字段（当前生效配置回执）

| 字段 | 类型 | 说明 |
|---|---|---|
| `global` | object | Agent 全局实际采集摘要：`{intervals, enable_gpu, interfaces, all_interfaces, disks, all_disks, pings}`；周期为各 Reporter 最小值，GPU 为 OR，选择项为并集；Ping 是无 name/type 的 `{target,interval}`，类型编码在规范化 URI target（如 `tcp://host:80`、`https://host:443`、`icmp://host`）中，并取各路最小 interval |
| `reporters` | array | 本机全部 Reporter 的脱敏摘要，字段见下；不含连接凭据、上报地址及其他线路身份，但会包含 Ping target |
| `reset_day` | number | 0-31 |
| `intervals` | object | {collect, report, ping, slow, gpu, ip, diskio}（秒） |
| `interfaces` | string[] | 网卡白名单（glob）；空 = 所有非虚拟网卡 |
| `disks` | string[] | 卷/物理盘白名单（glob）；空 = 全部 |
| `enable_gpu` | boolean | 当前 Reporter 是否输出 GPU（沿用 wire 字段名）；任一 Reporter 开启即启动全局 GPU worker |
| `report_errors` | boolean | 是否上报 errors 错误事件 |
| `report_self` | boolean | 是否上报探针自身占用 kind:"self" |
| `pings` | array | 当前 Reporter 的探测需求：`[{name, type: http|tcp|icmp, target, interval?}]`；HTTP target 仅允许 `http(s)://host[:port]`，所有类型均不允许 path/query/fragment |
| `ext` | object | 协议扩展；仅对应协议存在扩展时携带。当前 `{cf: {correction, batch}}` 只出现在 cf Reporter 的回执中，probe 不携带 |

`reporters[]` 每项包含：`id`、`protocol`、`intervals`、`report_interval`、`reset_day`、`interfaces`、`disks`、`report_gpu`、`report_errors`、`report_self`、`pings`。`pings` 保留该 Reporter 自己的原始 `name`、`type`、`target` 和可选 `interval`。

安全边界：摘要不会回传 `secret`、`worker_url`，也不会回传其他线路的 `server_id` / `config_version`；Ping 的 `target` 属于配置核对信息，会按原值回传。当前上报线路仍由请求头 `X-Reporter-Id` / `X-Reporter-Protocol` 标识，同级旧字段仍表示当前 Reporter 的完整有效配置。

## dynamic 记录字段（每条 = 一次 collect tick）

| 字段 | 类型 | 单位 | 来源 | 说明 |
|---|---|---|---|---|
| `ts` | number | 毫秒时间戳 | — | **采集时刻**，非上报时刻 |
| `accurate_ts` | number（可选） | 毫秒时间戳 | Agent 时钟 | 该记录采集时保存的校准时间；首次校准前缺席；CF sample 与月流量账期优先使用此值 |
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
| `net_interfaces` | object | — | 逐网卡计数器 + netstatic | 当前 Reporter 选中网卡的 `{name: {rx,tx,rx_speed,tx_speed,rx_monthly,tx_monthly}}`；上方兼容字段由这些网卡求和 |

## async 记录字段（每条 = 一次异步测量，kind 区分来源）

每条都有 `kind` 与 `ts`（**该条的真实测量时刻**，与 dynamic 的 ts 无关）；新增异步源只需新增 kind，协议不变。
kind 按数据语义划分（DESIGN.md §2.3"机制同类、语义分流"）：slow/gpu 是不同存在性与故障域的指标，公网 IP 属身份信息故在 static 而非此数组。

| kind | 其余字段 | 说明 |
|---|---|---|
| `ping` | `name`, `rtt`, `loss` | 探测结果：name = `[[pings]]` 组 key；rtt 毫秒，**-1 = 失败**；loss 0-100 |
| `slow` | `disk_used`, `disks`, `tcp_conn`, `udp_conn`, `processes` | 慢变指标；disks 为逐卷容量，disk_used 为当前 Reporter 选中卷合计 |
| `gpu` | `name`, `usage`, `mem_total`, `mem_used`, `temp` | GPU 型号、利用率（0-100）、显存（字节）、温度（℃）；多卡时每卡一条；mem/temp 仅 nvidia 路径有，macOS 为 null |
| `self` | `cpu_usage`, `mem_rss` | 探针自身 CPU（单核 %）与常驻内存（字节）；始终随 slow worker 采集，`report_self=true` 的 Reporter 才输出 |
| `diskio` | `read_bps`, `write_bps`, `read_iops`, `write_iops`, `await_ms`, `usage`, `disks` | `disks` 为逐物理盘 IO；上层字段为当前 Reporter 选中盘的聚合，usage 取最大；首轮差值无前值为 null |

Ping 聚合规则：机器内部按“类型 + 规范化目标”去重，TCP 使用小写 host + 有效端口，ICMP 使用小写 host，HTTP 使用 scheme + 小写 host + 有效端口（HTTP 与 HTTPS 不合并）。HTTP target 仅允许 `http(s)://host[:port]`（根 `/` 等价可接受），所有类型均禁止 path/query/fragment。重复任务的实际周期取所有 Reporter 需求的最小值；每轮 DNS 只在计时前解析一次，4 次采样与重试复用解析结果，因此 RTT 不含 DNS。结果与错误在出口映射回各 Reporter 自己的 `name`，不会串到未声明该任务的线路；`global.pings` 是无 name/type 的实际 worker 配置，规范化 URI target 自带探测类型。

约定：
- 异步记录**仅当对应源的快照 ts 更新时才产生**（同一份异步数据不会重复出现）；worker 失败 = 快照 ts 停滞；
- 异步数据粒度被 collect 间隔截断：一个 collect 周期内的多次异步更新只保留最新一次；
- 两个数组 ts 语义各自单一：dynamic 一律 tick 时刻，async 一律各自测量时刻；展示归并（按 kind/字段取最新）是服务端/前端职责；
- 各数组按采集序（seq）排列、近似 ts 升序；墙钟回拨或异步源交错时可能局部倒挂，服务端按 ts 归并即可；
- 单项采集失败该字段为 null，不影响其他字段；
- 上报失败的记录由 agent 保留待重发（dynamic/async/errors 三类共享一个 512 条的有界日志，按 Reporter 游标非破坏读取、成功才 ACK；超限丢最旧并注入 `source=buffer` 错误事件——只覆盖短暂抖动，长断网历史不补发），服务端可能收到延迟补发的记录，按 ts 去重/排序即可。

## errors 记录字段（每条 = 一次采集/上报失败）

| 字段 | 类型 | 说明 |
|---|---|---|
| `ts` | number | 发生时刻，毫秒时间戳 |
| `source` | string | 来源：`gpu` / `ip` / `reporter` / `ping:<组名>` |
| `msg` | string | 错误信息 |

约定：**同源同文去重**（同一来源上一条信息相同则跳过，防止周期性失败刷屏）；errors 与 dynamic/async 共用同一 512 条有界日志；上报失败后与数据一起保留重发。

## 响应（服务端 → agent）

支持时钟校准的服务端应在每次成功响应中返回尽量靠近响应发送时刻生成的 `server_time`：

```json
{ "server_time": 1754300060011 }
```

`server_time` 是服务端 Unix 毫秒墙钟，仅在公共 NTP 不可用时作为兜底。Agent 结合请求 RTT 中点估算该样本，且不会修改系统时钟。服务端仍应由 NTP/chrony 等保持准确。旧服务端可返回空 body 或 `{}`；只要 Agent 能访问公共 NTP，仍可得到独立准确时间，否则准确时间相关字段保持 null（或保留此前未过期的校准样本）。

有配置变更时：

```json
{
  "server_time": 1754300060011,
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

| 字段 | 约束 |
|---|---|
| `config_version` | 与 agent 当前版本**不等**才应用（幂等；人类可读时间戳无可靠大小语义，故用不等判断） |
| `intervals` | 当前 Reporter 的六项采集需求（秒，均 >= 1）；应用后重新计算机器级最小周期 |
| `report_interval` | 当前 Reporter 的上报间隔（秒），>= 1；与全局 collect 无任何关系约束 |
| `reset_day` | 账期重置日 1-31；0 = 不重置 |
| `interfaces` | 可选；网卡白名单（glob 数组，最多 32 项，超限整体拒绝） |
| `disks` | 可选；卷/物理盘白名单（glob 数组，最多 32 项，超限整体拒绝） |
| `pings` | 可选；整组替换当前 Reporter 的 Ping 需求（最多 64 项，超限整体拒绝）；`type` 必须为 http/tcp/icmp，target 不允许 path/query/fragment |
| `report_gpu` | 可选；当前 Reporter 是否输出 GPU（布尔），同时参与机器级 GPU worker 的 OR 聚合 |
| `report_errors` | 可选；是否上报 errors 错误事件（布尔，缺省 true） |
| `report_self` | 可选；是否上报探针自身资源占用 kind:"self"（布尔，缺省 false） |
| `ext` | 可选；协议扩展 `{cf: {correction?, batch?}}`，仅对应协议启用时生效 |

`config` 内除 `config_version` 外的字段均可选：出现的才应用，缺席的保持现值。响应只修改产生该响应的 Reporter，不会影响其他上报线路。

`server_time`、`config`、`next` 都位于响应信封一级；`config` 缺席表示无配置变更。

agent 行为：校验 `config_version`、所有周期、glob 与 Ping 目标，全部通过才应用并落盘；任何一项非法则整体拒绝并记录日志。应用后重新聚合全局 worker 配置并立即生效，无需重启。

**🔒 不允许远端修改**：连接身份 `id` / `protocol` / `server_id` / `secret` / `worker_url`，以及机器级 `net_static_path`。远端的 `intervals` / `pings` 等只修改响应所属 Reporter，不能直接写全局实际值，也不能修改其他线路。允许远端下发 Ping 的服务端应限制目标范围，避免 SSRF/内网探测。

## 服务端判定规则（约定）

| 场景 | 判定依据 |
|---|---|
| agent 离线 | 超过 N × report 间隔未收到任何上报（空数组上报也算在线） |
| 某项数据没采成功 | 对应异步块的 ts 停止前进（slow/pings/gpu）；fast 字段为 null |
| 数据没变 | 有新记录、值相同 |
| 月流量异常 | 客户端自算自报，服务端不参与统计与重置 |
