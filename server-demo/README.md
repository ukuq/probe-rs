# probe-rs 演示服务端

Deno + TypeScript 单文件、零依赖。实现 [REPORT.md](../REPORT.md) 协议。

## 运行

```bash
deno run --allow-net server.ts        # 默认 8080 端口
deno run --allow-net server.ts 9000   # 指定端口
```

打开 http://localhost:8080 看监控面板。

## 面板

每个 Reporter 实例一张卡片；同一 `server_id`
的多路原始协议上报独立展示、独立下发配置：

- **KPI 磁贴**：CPU / 内存 / 磁盘（带用量 meter，>70% 黄 >90% 红）、下行 /
  上行网速、连接数
- **趋势图**：CPU%（面积+线）、网速（↓↑ 双系列 + legend
  点击隔离）、探测延迟（按目标分色，端点标注）
- **交互**：crosshair + tooltip（悬停查看任意时刻全系列数值，键盘 focus 亦可）
- **明细表格**：图表的 table 双生，承载全部当前值，并展示
  NTP/服务端校准来源、本地时间、准确时间及快慢偏差（tabular-nums）

配色与图表规范遵循 dataviz 方法（dark 调色板经 `validate_palette.js`
六项检查）。

## 接口

| 接口                            | 说明                                                               |
| ------------------------------- | ------------------------------------------------------------------ |
| `POST /report`                  | agent 上报；响应携带毫秒级 `server_time` 与该实例的待下发配置      |
| `GET /`                         | 监控面板（3s 自动刷新）                                            |
| `GET /api/servers`              | 全部 Reporter 实例最新数据 JSON                                    |
| `POST /api/config/:instance_id` | 设置该 Reporter 的待下发配置；`instance_id` 由 `/api/servers` 返回 |

## 演示流程

1. 启动服务端：`deno run --allow-net server.ts`
2. 在 agent 的 `config.toml` 中增加一条 `[[reporters]]`：`protocol = "probe"`、
   `worker_url = "http://127.0.0.1:8080/report"`、`secret = "change-me"`
3. 启动 agent：`probe-rs -c config.toml`，面板上出现数据
4. 下发配置（演示远端热更新）：

```bash
curl -X POST localhost:8080/api/config/URL编码后的instance_id \
  -H 'Content-Type: application/json' \
  -d '{"intervals":{"collect":2,"ping":60,"slow":60,"gpu":60,"ip":600,"diskio":10},"report_interval":10,"reset_day":1,"interfaces":["eth*"],"disks":["nvme*"],"pings":[{"name":"edge","type":"icmp","target":"1.1.1.1","interval":60}],"report_gpu":true}'
```

服务端校验周期、glob 与 Ping 基本格式 → 下次上报随响应下发 → agent 原子应用该
Reporter 配置并落盘，面板上的 `cfg v` 版本号随之更新。Agent
随后自动重算实际机器级配置：周期取所有 Reporter 最小值、GPU 取
OR、网卡/磁盘/Ping 取并集。生产服务端允许下发 Ping 时应额外限制目标， 避免
SSRF/内网探测。

## 注意

- 数据全在内存，重启即丢（演示定位，不接数据库）
- 全局单一密钥 `change-me`，生产应按 server_id 分配并换 HTTPS
- 面板只能列出实际向本 Demo 上报的实例；发往外部 CF/Komari
  面板的连接地址、密钥和状态不会被枚举
- Reporter 的新增/删除及 `server_id`、`secret`、`worker_url`、`protocol`
  只允许改本地配置；远端配置始终只作用于响应所属实例
