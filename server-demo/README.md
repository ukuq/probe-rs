# probe-rs 演示服务端

Deno + TypeScript 单文件、零依赖。实现 [REPORT.md](../REPORT.md) 协议。

## 运行

```bash
deno run --allow-net server.ts        # 默认 8080 端口
deno run --allow-net server.ts 9000   # 指定端口
```

打开 http://localhost:8080 看监控面板。

## 面板

每服务器一张卡片：

- **KPI 磁贴**：CPU / 内存 / 磁盘（带用量 meter，>70% 黄 >90% 红）、下行 / 上行网速、连接数
- **趋势图**：CPU%（面积+线）、网速（↓↑ 双系列 + legend 点击隔离）、探测延迟（按目标分色，端点标注）
- **交互**：crosshair + tooltip（悬停查看任意时刻全系列数值，键盘 focus 亦可）
- **明细表格**：图表的 table 双生，承载全部当前值（tabular-nums）

配色与图表规范遵循 dataviz 方法（dark 调色板经 `validate_palette.js` 六项检查）。

## 接口

| 接口 | 说明 |
|---|---|
| `POST /report` | agent 上报，头 `X-Secret: change-me`；响应携带待下发配置 |
| `GET /` | 监控面板（3s 自动刷新） |
| `GET /api/servers` | 全部服务器最新数据 JSON |
| `POST /api/config/:server_id` | 设置待下发配置，下次上报时随响应便车下发 |

## 演示流程

1. 启动服务端：`deno run --allow-net server.ts`
2. 配置 agent（`config.toml`）：`worker_url = "http://127.0.0.1:8080/report"`、`secret = "change-me"`
3. 启动 agent：`probe-rs -c config.toml`，面板上出现数据
4. 下发配置（演示远端热更新）：

```bash
curl -X POST localhost:8080/api/config/服务器ID \
  -d '{"collect":5,"report":10,"ping":15,"reset_day":1}'
```

服务端校验基本合法性（间隔 >= 1、reset_day 0-31）→ 下次上报随响应下发 → agent 原子应用、重建 ticker 并落盘，面板上的 `cfg v` 版本号随之 +1。

## 注意

- 数据全在内存，重启即丢（演示定位，不接数据库）
- 全局单一密钥 `change-me`，生产应按 server_id 分配并换 HTTPS
