#!/usr/bin/env -S deno run --allow-net
/**
 * probe-rs 演示服务端（Deno + TypeScript，零依赖单文件）
 *
 * 实现 REPORT.md 协议：
 *   POST /report            agent 上报（X-Secret 认证），响应可携带远端配置
 *   GET  /                  监控面板（KPI 磁贴 + 趋势图）+ 上报记录调试视图
 *   GET  /api/servers       全部服务器最新数据 JSON
 *   GET  /api/reports       最近收到的原始上报（debug 用）
 *   GET  /ws                面板 WebSocket 实时推送
 *   POST /api/config/:id    设置待下发配置，下次上报时随响应便车下发
 *
 * 运行: deno run --allow-net server.ts [PORT]   默认 8080，密钥见 SECRET
 */

// ---------- 协议类型（对应 REPORT.md） ----------

interface Intervals {
  collect: number;
  report: number;
  ping: number;
  slow?: number;
  gpu?: number;
  ip?: number;
  diskio?: number;
}

interface PingTarget {
  name: string;
  target: string;
  interval?: number;
}

interface RemoteConfig {
  config_version: string; // 人类可读的 UTC+8 时间戳；与 agent 当前值不等才应用
  intervals?: Intervals;
  reset_day?: number;
  interfaces?: string[];
  enable_gpu?: boolean;
  pings?: PingTarget[];
  report_errors?: boolean;
  report_self?: boolean;
}

interface StaticInfo {
  ts: number; // static 信息采集时刻，毫秒时间戳
  os: string;
  kernel: string;
  arch: string;
  cpu_name: string;
  cpu_cores: number;
  cpu_physical_cores: number | null;
  mem_total: number;
  swap_total: number;
  disk_total: number;
  gpu_name: string | null;
  virtualization: string | null;
  boot_time: number;
  ipv4: string | null;
  ipv6: string | null;
  agent_version: string;
  /** 当前生效配置（供展示/核对） */
  config: {
    reset_day: number;
    intervals: Intervals;
    interfaces: string[];
    enable_gpu: boolean;
    report_errors: boolean;
    report_self: boolean;
    pings: PingTarget[];
    /** 协议扩展（ext.cf：correction/batch） */
    ext?: { cf?: { correction?: boolean; batch?: boolean } };
  };
}

interface SlowBlock {
  ts: number;
  disk_used: number | null;
  tcp_conn: number | null;
  udp_conn: number | null;
  processes: number | null;
}

interface PingRecord {
  ts: number;
  name: string;
  rtt: number; // -1 = 探测失败
  loss: number;
}

interface GpuRecord {
  ts: number;
  name: string;
  usage: number | null;
  mem_total: number | null; // 字节；macOS 为 null（统一内存）
  mem_used: number | null;
  temp: number | null; // ℃；macOS 为 null（需 root）
}

interface DynamicRecord {
  ts: number;
  cpu_usage: number | null;
  mem_used: number | null;
  swap_used: number | null;
  load: [number, number, number] | null;
  net_rx: number | null;
  net_tx: number | null;
  net_rx_speed: number | null;
  net_tx_speed: number | null;
  net_rx_monthly: number | null;
  net_tx_monthly: number | null;
}

/** 异步记录：kind 区分来源，每条 ts 为各自真实测量时刻 */
interface SelfRecord {
  ts: number;
  cpu_usage: number | null; // 自身 CPU（单核 %）
  mem_rss: number | null; // 自身常驻内存，字节
}

interface DiskIoRecord {
  ts: number;
  read_bps: number | null; // 字节/秒
  write_bps: number | null;
  read_iops: number | null;
  write_iops: number | null;
  await_ms: number | null; // 平均等待 ms
  usage: number | null; // IO 利用率 %（各盘取最大）；macOS 为 null
}

type AsyncRecord =
  | ({ kind: "ping" } & PingRecord)
  | ({ kind: "slow" } & SlowBlock)
  | ({ kind: "gpu" } & GpuRecord)
  | ({ kind: "diskio" } & DiskIoRecord)
  | ({ kind: "self" } & SelfRecord);

interface ErrorRecord {
  ts: number;
  source: string; // gpu / ip / reporter / ping:<组名> ...
  msg: string;
}

interface Report {
  server_id: string;
  config_version: string;
  static?: StaticInfo;
  dynamic?: DynamicRecord[];
  async?: AsyncRecord[];
  errors?: ErrorRecord[];
}

// ---------- 存储（全内存，演示定位） ----------

interface ServerState {
  staticInfo: StaticInfo | null;
  /** agent 版本（X-Agent-Version 头），用于下发兼容判断 */
  agentVersion: string;
  dynamic: DynamicRecord[];
  asyncs: AsyncRecord[];
  errors: ErrorRecord[];
  lastSeen: number;
  configVersion: string;
}

interface RawReport {
  seq: number;
  received_at: number;
  server_id: string;
  report: Report;
}

const PORT = Number(Deno.args[0]) || 8080;
const SECRET = "change-me"; // 演示用全局密钥；生产应按 server_id 分配
const KEEP_DYNAMIC = 300;
const KEEP_REPORTS = 100;

const servers = new Map<string, ServerState>();
/** 待下发配置（下次上报时随响应发出） */
const pendingConfig = new Map<string, RemoteConfig>();
/** 要求该服务器下次上报强制带 static（一次性） */
const needStatic = new Set<string>();
/** 最近收到的原始上报（debug 视图用），全局环形 */
const rawReports: RawReport[] = [];
let reportSeq = 0;

/** 面板 WebSocket 客户端：每次收到上报后广播最新视图 */
const clients = new Set<WebSocket>();

function broadcast(): void {
  if (!clients.size) return;
  const msg = JSON.stringify(serversView());
  for (const ws of clients) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

function getServer(id: string): ServerState {
  let s = servers.get(id);
  if (!s) {
    s = {
      staticInfo: null,
      agentVersion: "",
      dynamic: [],
      asyncs: [],
      errors: [],
      lastSeen: 0,
      configVersion: "",
    };
    servers.set(id, s);
  }
  return s;
}

// ---------- 协议处理 ----------

function handleReport(req: Request, report: Report): Response {
  const id = report.server_id;
  if (!id) return json({ error: "missing server_id" }, 400);
  if (req.headers.get("x-secret") !== SECRET) {
    return json({ error: "bad secret" }, 401);
  }

  rawReports.push({
    seq: ++reportSeq,
    received_at: Date.now(),
    server_id: id,
    report,
  });
  if (rawReports.length > KEEP_REPORTS) {
    rawReports.splice(0, rawReports.length - KEEP_REPORTS);
  }

  const s = getServer(id);
  s.lastSeen = Date.now();
  s.agentVersion = req.headers.get("x-agent-version") ?? s.agentVersion;
  s.configVersion = report.config_version ?? "";
  if (report.static) s.staticInfo = report.static;
  if (Array.isArray(report.dynamic)) {
    s.dynamic.push(...report.dynamic);
    if (s.dynamic.length > KEEP_DYNAMIC) {
      s.dynamic = s.dynamic.slice(-KEEP_DYNAMIC);
    }
  }
  if (Array.isArray(report.async)) {
    s.asyncs.push(...report.async);
    if (s.asyncs.length > KEEP_DYNAMIC) {
      s.asyncs = s.asyncs.slice(-KEEP_DYNAMIC);
    }
  }
  if (Array.isArray(report.errors)) {
    s.errors.push(...report.errors);
    if (s.errors.length > KEEP_DYNAMIC) {
      s.errors = s.errors.slice(-KEEP_DYNAMIC);
    }
  }

  broadcast();

  // 组装响应信封：config 缺席 = 无变更；next.static = 强制下次带 static（一次性）
  const resp: { config?: RemoteConfig; next?: { static: boolean } } = {};
  const pending = pendingConfig.get(id);
  if (pending && pending.config_version !== s.configVersion) {
    pendingConfig.delete(id);
    resp.config = pending;
    console.log(
      `[config] 下发到 ${id}: ${
        s.configVersion || "(无版本)"
      } -> ${pending.config_version}`,
      pending,
    );
  }
  if (needStatic.delete(id)) {
    resp.next = { static: true };
    console.log(`[static] 要求 ${id} 下次上报带 static`);
  }
  return json(resp);
}

/** 生成人类可读的 UTC+8 版本字符串，如 2026-08-06T15:30:45.123+08:00 */
function newConfigVersion(): string {
  const d = new Date(Date.now() + 8 * 3600_000);
  const p = (n: number, w = 2) => String(n).padStart(w, "0");
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${
    p(d.getUTCDate())
  }` +
    `T${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}.${
      p(d.getUTCMilliseconds(), 3)
    }+08:00`;
}

function handleSetConfig(id: string, cfg: Record<string, unknown>): Response {
  const {
    collect,
    report,
    ping,
    slow,
    gpu,
    ip,
    diskio,
    reset_day,
    interfaces,
    enable_gpu,
    pings,
    report_errors,
    report_self,
  } = cfg;
  // intervals：7 项任一提供即整体下发；未提供的按该机器 static 回执的当前值补齐
  const hasAnyInterval = [collect, report, ping, slow, gpu, ip, diskio].some((
    v,
  ) => v !== undefined);
  const next: Partial<RemoteConfig> = {};
  if (hasAnyInterval) {
    const curIv = getServer(id).staticInfo?.config?.intervals;
    const fill = (
      v: unknown,
      cur: number | undefined,
      def: number,
      name: string,
    ) => {
      if (v !== undefined) {
        if (!Number.isInteger(v) || (v as number) < 1) {
          throw { status: 400, msg: `intervals.${name} 必须为 >=1 的整数` };
        }
        return v as number;
      }
      return cur ?? def;
    };
    try {
      next.intervals = {
        collect: fill(collect, curIv?.collect, 10, "collect"),
        report: fill(report, curIv?.report, 60, "report"),
        ping: fill(ping, curIv?.ping, 30, "ping"),
        slow: fill(slow, curIv?.slow, 60, "slow"),
        gpu: fill(gpu, curIv?.gpu, 60, "gpu"),
        ip: fill(ip, curIv?.ip, 600, "ip"),
        diskio: fill(diskio, curIv?.diskio, 10, "diskio"),
      };
    } catch (e) {
      const err = e as { status: number; msg: string };
      return json({ error: err.msg }, err.status);
    }
    // 0.1.0 agent 的 Intervals 是 deny_unknown_fields：带 diskio 会让它解析响应失败
    if (getServer(id).agentVersion === "0.1.0" && next.intervals) {
      delete (next.intervals as unknown as Record<string, unknown>).diskio;
    }
  }
  if (reset_day !== undefined) {
    if (
      !Number.isInteger(reset_day) || (reset_day as number) < 0 ||
      (reset_day as number) > 31
    ) {
      return json({ error: "reset_day 必须在 0-31" }, 400);
    }
    next.reset_day = reset_day as number;
  }
  if (interfaces !== undefined) {
    if (
      !Array.isArray(interfaces) ||
      !interfaces.every((x) =>
        typeof x === "string" && x.length > 0 && x.length <= 64
      )
    ) {
      return json({
        error: "interfaces 必须为非空字符串数组（单个 <= 64 字符）",
      }, 400);
    }
    next.interfaces = interfaces as string[];
  }
  if (enable_gpu !== undefined) {
    if (typeof enable_gpu !== "boolean") {
      return json({ error: "enable_gpu 必须为布尔值" }, 400);
    }
    next.enable_gpu = enable_gpu;
  }
  if (report_errors !== undefined) {
    if (typeof report_errors !== "boolean") {
      return json({ error: "report_errors 必须为布尔值" }, 400);
    }
    next.report_errors = report_errors;
  }
  if (report_self !== undefined) {
    if (typeof report_self !== "boolean") {
      return json({ error: "report_self 必须为布尔值" }, 400);
    }
    next.report_self = report_self;
  }
  if (pings !== undefined) {
    if (!Array.isArray(pings)) return json({ error: "pings 必须为数组" }, 400);
    const names = new Set<string>();
    for (const p of pings as PingTarget[]) {
      if (!p || typeof p.name !== "string" || !p.name.trim()) {
        return json({ error: "pings 存在空 name" }, 400);
      }
      if (typeof p.target !== "string" || !p.target.trim()) {
        return json({ error: `pings ${p.name} 的 target 为空` }, 400);
      }
      if (names.has(p.name)) {
        return json({ error: `pings name 重复: ${p.name}` }, 400);
      }
      names.add(p.name);
      if (
        p.interval !== undefined &&
        (!Number.isInteger(p.interval) || p.interval < 1)
      ) {
        return json({ error: `pings ${p.name} 的 interval 必须 >= 1` }, 400);
      }
    }
    next.pings = pings as PingTarget[];
  }
  if (
    !hasAnyInterval && reset_day === undefined && interfaces === undefined &&
    enable_gpu === undefined && pings === undefined &&
    report_errors === undefined && report_self === undefined
  ) {
    return json({ error: "没有可下发的字段" }, 400);
  }
  const pending: Partial<RemoteConfig> = pendingConfig.get(id) ?? {};
  const merged: RemoteConfig = {
    ...pending,
    ...next,
    config_version: newConfigVersion(),
  } as RemoteConfig;
  pendingConfig.set(id, merged);
  console.log(`[config] ${id} 待下发:`, merged);
  return json({ ok: true, pending: merged });
}

// ---------- 视图数据 ----------

function serversView() {
  const now = Date.now();
  return [...servers.entries()].map(([id, s]) => {
    const recent = s.dynamic.slice(-150);
    const asyncs = s.asyncs.slice(-300);
    const pings = asyncs.filter((a) => a.kind === "ping");
    return {
      server_id: id,
      online: now - s.lastSeen < 90_000,
      last_seen: s.lastSeen,
      config_version: s.configVersion,
      pending_config: pendingConfig.get(id) ?? null,
      static: s.staticInfo,
      dynamic_count: s.dynamic.length,
      async_count: s.asyncs.length,
      recent,
      ping_list: pings,
      diskio_list: asyncs.filter((a) => a.kind === "diskio"),
      slow_latest: asyncs.findLast((a) => a.kind === "slow") ?? null,
      gpu_latest: asyncs.findLast((a) => a.kind === "gpu") ?? null,
      self_latest: asyncs.findLast((a) => a.kind === "self") ?? null,
      diskio_latest: asyncs.findLast((a) => a.kind === "diskio") ?? null,
      errors: s.errors.slice(-10),
      error_count: s.errors.length,
    };
  });
}

function reportsView() {
  return rawReports.slice(-KEEP_REPORTS).reverse().map((r) => ({
    seq: r.seq,
    received_at: r.received_at,
    server_id: r.server_id,
    has_static: !!r.report.static,
    dynamic_count: r.report.dynamic?.length ?? 0,
    ping_count: (r.report.async ?? []).filter((a) => a.kind === "ping").length,
    gpu_count: (r.report.async ?? []).filter((a) => a.kind === "gpu").length,
    async_count: r.report.async?.length ?? 0,
    error_count: r.report.errors?.length ?? 0,
    report: r.report,
  }));
}

function json(obj: unknown, status = 200): Response {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// ---------- 路由 ----------

Deno.serve({ port: PORT }, async (req) => {
  const url = new URL(req.url);
  if (req.method === "POST" && url.pathname === "/report") {
    try {
      return handleReport(req, await req.json());
    } catch {
      return json({ error: "invalid json" }, 400);
    }
  }
  const nsMatch = url.pathname.match(/^\/api\/need-static\/([^/]+)$/);
  if (req.method === "POST" && nsMatch) {
    needStatic.add(nsMatch[1]);
    return json({
      ok: true,
      note: "下次该 agent 上报时响应将带 next.static=true",
    });
  }
  const cfgMatch = url.pathname.match(/^\/api\/config\/([^/]+)$/);
  if (req.method === "POST" && cfgMatch) {
    try {
      return handleSetConfig(cfgMatch[1], await req.json());
    } catch {
      return json({ error: "invalid json" }, 400);
    }
  }
  if (req.method === "GET" && url.pathname === "/api/servers") {
    return json(serversView());
  }
  if (req.method === "GET" && url.pathname === "/api/reports") {
    return json(reportsView());
  }
  if (req.method === "GET" && url.pathname === "/ws") {
    const { socket, response } = Deno.upgradeWebSocket(req);
    socket.onopen = () => socket.send(JSON.stringify(serversView()));
    socket.onclose = () => clients.delete(socket);
    socket.onerror = () => clients.delete(socket);
    clients.add(socket);
    return response;
  }
  if (req.method === "GET" && url.pathname === "/") {
    // 面板是内联脚本单页：禁止缓存，避免改了 server.ts 后浏览器还跑旧 JS
    return new Response(PAGE, {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store",
      },
    });
  }
  return json({ error: "not found" }, 404);
});

console.log(`probe-rs 演示服务端: http://localhost:${PORT} (SECRET=${SECRET})`);

// ---------- 演示面板 ----------
// macOS 风格：窗口标题栏（红绿灯）+ 分段控件 + 玻璃卡片 + 系统色；canvas 手绘图表

const PAGE = `<!doctype html>
<html lang="zh"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>probe-rs 监控</title>
<style>
  :root {
    --bg: #161617; --win: #1f1f21; --card: rgba(255,255,255,.05); --card-2: rgba(255,255,255,.07);
    --fill: rgba(255,255,255,.09); --sep: rgba(255,255,255,.09); --hairline: rgba(255,255,255,.07);
    --text: #f5f5f7; --text2: #98989d; --text3: #6e6e73;
    --blue: #0a84ff; --green: #30d158; --red: #ff453a; --orange: #ff9f0a;
    --yellow: #ffd60a; --purple: #bf5af2; --teal: #64d2ff;
    --tip: rgba(38,38,40,.96); --code: #d4d4d6; --seg-on: rgba(255,255,255,.16);
    --titlebar: rgba(255,255,255,.035);
  }
  body.light {
    --bg: #e8e8ed; --win: #f6f6f8; --card: rgba(0,0,0,.028); --card-2: rgba(0,0,0,.035);
    --fill: rgba(0,0,0,.055); --sep: rgba(0,0,0,.1); --hairline: rgba(0,0,0,.07);
    --text: #1d1d1f; --text2: #6e6e73; --text3: #aeaeb2;
    --blue: #007aff; --green: #28a745; --red: #d70015; --orange: #c93400;
    --yellow: #b25000; --purple: #8944ab; --teal: #0071a4;
    --tip: rgba(255,255,255,.97); --code: #3a3a3c; --seg-on: #ffffff;
    --titlebar: rgba(0,0,0,.03);
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--text);
    font: 13px/1.5 -apple-system, BlinkMacSystemFont, "SF Pro Text", "PingFang SC", sans-serif;
    -webkit-font-smoothing: antialiased; }
  .window { max-width: 1180px; margin: 28px auto 48px; background: var(--win);
    border: 0.5px solid var(--sep); border-radius: 14px; overflow: hidden;
    box-shadow: 0 24px 70px rgba(0,0,0,.5); }
  /* 标题栏 */
  .titlebar { position: relative; display: flex; align-items: center; height: 50px;
    padding: 0 16px; background: var(--titlebar); border-bottom: 0.5px solid var(--sep); }
  .lights { display: flex; gap: 8px; }
  .tl { width: 12px; height: 12px; border-radius: 50%; }
  .tl.r { background: #ff5f57; } .tl.y { background: #febc2e; } .tl.g { background: #28c840; }
  .titlebar .title { position: absolute; left: 0; right: 0; text-align: center;
    font-size: 13px; font-weight: 600; pointer-events: none; }
  .theme-btn { appearance: none; border: none; background: none; color: var(--text2);
    font: inherit; font-size: 14px; cursor: pointer; padding: 2px 6px; border-radius: 6px;
    margin-left: auto; }
  .theme-btn:hover { background: var(--fill); }
  .theme-btn.on { color: var(--blue); background: var(--fill); font-weight: 600; }
  .conn { font-size: 11.5px; color: var(--text2); display: flex; align-items: center; gap: 6px; }
  .conn::before { content: ""; width: 7px; height: 7px; border-radius: 50%; background: var(--text3); }
  .conn.on::before { background: var(--green); }
  /* 工具栏：分段控件 + 摘要 */
  .toolbar { display: flex; align-items: center; gap: 14px; padding: 12px 20px;
    border-bottom: 0.5px solid var(--hairline); flex-wrap: wrap; }
  .seg { display: inline-flex; background: var(--fill); border-radius: 9px; padding: 2px; gap: 1px; }
  .seg button { appearance: none; border: none; background: none; color: var(--text2);
    font: inherit; font-size: 12.5px; padding: 4px 16px; border-radius: 7px; cursor: pointer; }
  .seg button.on { background: var(--seg-on); color: var(--text);
    box-shadow: 0 1px 3px rgba(0,0,0,.25); }
  .summary { margin-left: auto; font-size: 12px; color: var(--text2); font-variant-numeric: tabular-nums; }
  main { padding: 20px; }
  /* 卡片 */
  .card { background: var(--card); border: 0.5px solid var(--sep); border-radius: 14px;
    padding: 16px 18px; margin-bottom: 16px; }
  .card.offline { opacity: .55; }
  .card-head { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
  .dot { width: 9px; height: 9px; border-radius: 50%; flex: none; }
  .dot.on { background: var(--green); box-shadow: 0 0 6px rgba(48,209,88,.6); }
  .dot.off { background: var(--red); }
  .name { font-size: 15px; font-weight: 600; }
  .meta { color: var(--text2); font-size: 12px; }
  .spacer { flex: 1; }
  .badge { font-size: 11px; color: var(--orange); background: rgba(255,159,10,.14);
    border-radius: 999px; padding: 2px 10px; }
  /* 按钮 */
  .btn { appearance: none; border: none; border-radius: 7px; font: inherit; font-size: 12.5px;
    padding: 5px 14px; cursor: pointer; background: var(--fill); color: var(--text); }
  .btn:hover { background: rgba(255,255,255,.14); }
  .btn.primary { background: var(--blue); color: #fff; font-weight: 500; }
  .btn.primary:hover { background: #3396ff; }
  .btn:disabled { opacity: .45; cursor: default; }
  /* KPI 磁贴 */
  .tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-top: 14px; }
  .tile { background: var(--card-2); border-radius: 10px; padding: 10px 14px; }
  .tile .label { color: var(--text2); font-size: 11px; }
  .tile .value { font-size: 20px; font-weight: 600; margin-top: 1px; font-variant-numeric: tabular-nums; }
  .tile .extra { color: var(--text2); font-size: 11px; margin-top: 1px; font-variant-numeric: tabular-nums; }
  .meter { margin-top: 7px; height: 3px; border-radius: 2px; background: rgba(255,255,255,.12); overflow: hidden; }
  .meter > div { height: 100%; border-radius: 2px; background: var(--blue); }
  .meter.warn > div { background: var(--orange); }
  .meter.crit > div { background: var(--red); }
  /* 图表 */
  .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(310px, 1fr)); gap: 12px; margin-top: 14px; }
  .chart { background: var(--card-2); border-radius: 10px; padding: 10px 12px; }
  .chart-title { color: var(--text2); font-size: 12px; margin-bottom: 6px; }
  .legend { display: flex; gap: 14px; margin-bottom: 4px; flex-wrap: wrap; }
  .legend-item { display: inline-flex; align-items: center; gap: 6px; background: none; border: none;
    color: var(--text2); font: inherit; font-size: 11.5px; cursor: pointer; padding: 0; }
  .legend-item.off { opacity: .35; }
  .legend-key { width: 12px; height: 3px; border-radius: 2px; }
  .plot { position: relative; outline: none; }
  .crosshair { position: absolute; top: 10px; bottom: 20px; width: 1px;
    background: var(--text3); pointer-events: none; opacity: .6; }
  .tooltip { position: absolute; pointer-events: none; background: var(--tip);
    border: 0.5px solid var(--sep); border-radius: 8px; padding: 6px 10px; font-size: 11.5px;
    white-space: nowrap; z-index: 2; box-shadow: 0 6px 20px rgba(0,0,0,.35); }
  .tooltip .tt-time { color: var(--text2); margin-bottom: 2px; }
  .tooltip .tt-row { display: flex; align-items: center; gap: 6px; }
  .tooltip .tt-key { width: 10px; height: 3px; border-radius: 2px; }
  .tooltip .tt-val { font-weight: 600; font-variant-numeric: tabular-nums; }
  .tooltip .tt-name { color: var(--text2); }
  /* 明细表（macOS 分隔列表） */
  .detail { margin-top: 14px; width: 100%; border-collapse: collapse; font-size: 12.5px;
    font-variant-numeric: tabular-nums; }
  .detail td { padding: 5px 8px; border-top: 0.5px solid var(--hairline); }
  .detail tr:first-child td { border-top: none; }
  .detail td.k { color: var(--text2); white-space: nowrap; width: 1%; }
  .detail td.v { color: var(--text); padding-right: 26px; }
  .detail .err { color: var(--red); }
  /* 空态 */
  .empty { color: var(--text2); text-align: center; padding: 90px 0; }
  .empty code { color: var(--blue); background: var(--fill); padding: 2px 10px; border-radius: 6px; }
  /* 上报记录：inset 列表 + 详情 */
  .rpt-list { background: var(--card); border: 0.5px solid var(--sep); border-radius: 12px;
    overflow: hidden; max-height: 320px; overflow-y: auto; }
  .rpt-row { display: flex; gap: 14px; align-items: baseline; padding: 9px 16px; cursor: pointer;
    border-top: 0.5px solid var(--hairline); font-variant-numeric: tabular-nums; }
  .rpt-row:first-child { border-top: none; }
  .rpt-row:hover { background: rgba(255,255,255,.04); }
  .rpt-row.sel { background: rgba(10,132,255,.16); }
  .rpt-seq { color: var(--text3); width: 46px; flex: none; }
  .rpt-time { color: var(--text2); flex: none; }
  .rpt-id { font-weight: 600; }
  .rpt-sum { color: var(--text2); font-size: 12px; margin-left: auto; }
  .rpt-detail { margin-top: 14px; background: var(--card); border: 0.5px solid var(--sep);
    border-radius: 12px; padding: 12px 16px; }
  .rpt-detail .d-head { color: var(--text2); font-size: 12px; margin-bottom: 8px; }
  pre.code { margin: 0; font: 11.5px/1.55 "SF Mono", ui-monospace, Menlo, monospace;
    color: var(--code); white-space: pre-wrap; word-break: break-all; }
  /* 上报记录过滤条 */
  .rpt-bar { display: flex; align-items: center; gap: 10px; margin-bottom: 12px; }
  .rpt-bar .lab { color: var(--text2); font-size: 12px; }
  .rpt-bar select { background: var(--fill); border: 0.5px solid var(--sep); border-radius: 6px;
    color: var(--text); font: inherit; font-size: 12.5px; padding: 4px 10px; }
  .rpt-detail pre.code { max-height: 480px; overflow: auto; }
  /* 配置分组与逐项编辑 */
  .cfg-groups { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
    gap: 12px; margin-top: 14px; }
  .cfg-group { background: var(--card-2); border-radius: 10px; padding: 12px 14px; }
  .cfg-group h3 { font-size: 11px; color: var(--text2); margin: 0 0 8px; font-weight: 600;
    letter-spacing: .02em; }
  .cfg-item { display: grid; grid-template-columns: minmax(0,1fr) auto 104px; align-items: center;
    gap: 12px; padding: 4px 0; font-size: 12.5px; }
  .cfg-item.plain { grid-template-columns: minmax(0,1fr) auto; }
  .cfg-item .lab { display: flex; align-items: center; gap: 7px; color: var(--text);
    cursor: pointer; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .cfg-item.plain .lab { cursor: default; color: var(--text2); }
  .cfg-item input[type=checkbox] { accent-color: var(--blue); width: 14px; height: 14px; margin: 0; flex: none; }
  .cfg-item .cur { color: var(--text2); font-variant-numeric: tabular-nums; text-align: right;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 180px; }
  .cfg-item.on .cur { color: var(--text); }
  .cfg-item input[type=number], .cfg-item input[type=text], .cfg-item select {
    width: 100%; background: var(--fill); border: 0.5px solid var(--sep); border-radius: 6px;
    color: var(--text); font: inherit; font-size: 12.5px; padding: 4px 8px; }
  .cfg-item input:focus, .cfg-item select:focus, .ping-row input:focus {
    outline: none; border-color: var(--blue); box-shadow: 0 0 0 3px rgba(10,132,255,.3); }
  .cfg-item input:disabled, .cfg-item select:disabled { opacity: .3; }
  .cfg-item input.wide { min-width: 0; }
  .cfg-pings { grid-column: 1 / -1; }
  .ping-row { display: grid; grid-template-columns: 104px minmax(0,1fr) 72px 26px; gap: 6px;
    margin-top: 6px; align-items: center; }
  .ping-row input { background: var(--fill); border: 0.5px solid var(--sep); border-radius: 6px;
    color: var(--text); font: inherit; font-size: 12.5px; padding: 4px 8px; min-width: 0; }
  .ping-row input:disabled { opacity: .3; }
  .ping-del { background: none; border: none; color: var(--text3); cursor: pointer;
    font-size: 13px; padding: 0; text-align: center; }
  .ping-del:hover { color: var(--red); }
  .ping-add { margin-top: 8px; }
  .cfg-foot { display: flex; align-items: center; gap: 12px; margin-top: 14px; }
  .cfg-status { color: var(--orange); font-size: 12px; }
  .cfg-hint { color: var(--text3); font-size: 11.5px; margin-left: auto; }
  /* 样例 */
  .example .note { color: var(--text2); font-size: 12px; margin-top: 10px; }
  .example .cm { color: var(--text3); }
</style></head><body>
<div class="window">
  <header class="titlebar">
    <div class="lights"><span class="tl r"></span><span class="tl y"></span><span class="tl g"></span></div>
    <div class="title">probe-rs 监控</div>
    <button class="theme-btn" id="theme-btn" title="切换浅色/深色">☾</button>
    <div class="conn" id="conn">连接中…</div>
  </header>
  <nav class="toolbar">
    <div class="seg" id="seg">
      <button data-view="dash" class="on">监控</button>
      <button data-view="reports">上报记录</button>
      <button data-view="config">配置</button>
      <button data-view="cfview">CF 预览</button>
      <button data-view="example">上报样例</button>
      <button data-view="cfgex">配置样例</button>
    </div>
    <div class="summary" id="summary"></div>
  </nav>
  <main id="app"></main>
</div>
<script>
var PALETTE_DARK = ['#0a84ff', '#ff9f0a', '#30d158', '#bf5af2', '#64d2ff', '#ffd60a'];
var PALETTE_LIGHT = ['#007aff', '#c93400', '#28a745', '#8944ab', '#0071a4', '#b25000'];
var CHART_DARK = { grid: 'rgba(255,255,255,.07)', label: '#6e6e73', dotBg: '#2c2c2e', endText: '#98989d' };
var CHART_LIGHT = { grid: 'rgba(0,0,0,.08)', label: '#8e8e93', dotBg: '#e8e8ed', endText: '#6e6e73' };
var PALETTE = PALETTE_DARK;
var CHART = CHART_DARK;
function setTheme(t) {
  document.body.classList.toggle('light', t === 'light');
  PALETTE = t === 'light' ? PALETTE_LIGHT : PALETTE_DARK;
  CHART = t === 'light' ? CHART_LIGHT : CHART_DARK;
  document.getElementById('theme-btn').textContent = t === 'light' ? '☀' : '☾';
  try { localStorage.setItem('probe-theme', t); } catch (e) {}
  switchView(view);  // 重建图表/视图以应用新配色
}
(function initTheme() {
  var t = null;
  try { t = localStorage.getItem('probe-theme'); } catch (e) {}
  if (t !== 'light' && t !== 'dark') {
    t = window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
  }
  document.body.classList.toggle('light', t === 'light');
  PALETTE = t === 'light' ? PALETTE_LIGHT : PALETTE_DARK;
  CHART = t === 'light' ? CHART_LIGHT : CHART_DARK;
  document.getElementById('theme-btn').textContent = t === 'light' ? '☀' : '☾';
})();
document.getElementById('theme-btn').onclick = function () {
  setTheme(document.body.classList.contains('light') ? 'dark' : 'light');
};

/* ---- CF 协议换算（对齐 agent 端 reporter_cf.rs；CF 预览 tab 用） ----
   注意：这是 agent 线协议映射的 JS 镜像，改 reporter_cf.rs 时需同步 */
var CF_COMPAT_VERSION = '1.3.8'; // 同步 reporter_cf.rs 的 CF_COMPAT_VERSION
var serverStatics = {};   // server_id -> 最新 static
var serverLatest = {};    // server_id -> serversView 条目（异步快照兜底用）
function cfMb(b) { return b == null ? null : Math.floor(b / 1048576); }
function cfRound2(v) { return v == null ? null : Math.round(v * 100) / 100; }
function cfLoad(l) { return l ? l.map(function (x) { return x.toFixed(2); }).join(' ') : null; }
function cfBody(r) {
  var rep = r.report || {};
  var st = rep.static || serverStatics[r.server_id] || {};
  var view = serverLatest[r.server_id] || {};
  var dyn = rep.dynamic || [];
  var last = dyn.length ? dyn[dyn.length - 1] : {};
  var slow = null, gpus = [], pings = {}, dio = null;
  (rep.async || []).forEach(function (a) {
    if (a.kind === 'slow') slow = a;
    else if (a.kind === 'gpu') gpus.push(a);
    else if (a.kind === 'ping') pings[a.name] = a;
    else if (a.kind === 'diskio') dio = a;
  });
  /* 与 agent 一致：异步字段读最新快照而非仅本条报文（async[] 有新鲜度去重，
     大多数报文不含 ping/slow/gpu 记录） */
  if (!slow && view.slow_latest) slow = view.slow_latest;
  if (!gpus.length && view.gpu_latest) gpus = [view.gpu_latest];
  if (!dio && view.diskio_latest) dio = view.diskio_latest;
  (view.ping_list || []).forEach(function (p) { if (!pings[p.name]) pings[p.name] = p; });
  function pingOf(names) {
    for (var i = 0; i < names.length; i++) {
      var p = pings[names[i]];
      if (p && p.rtt >= 0) return p;
    }
    return null;
  }
  function lossOf(names) {
    for (var i = 0; i < names.length; i++) if (pings[names[i]]) return pings[names[i]].loss;
    return undefined;
  }
  var m = {};
  function set(k, v) { if (v !== null && v !== undefined) m[k] = v; }
  set('cpu', cfRound2(last.cpu_usage));
  m.ram_total = cfMb(st.mem_total) || 0;
  set('ram_used', cfMb(last.mem_used));
  m.swap_total = cfMb(st.swap_total) || 0;
  set('swap_used', cfMb(last.swap_used));
  m.disk_total = cfMb(st.disk_total) || 0;
  set('disk_used', cfMb(slow && slow.disk_used));
  if (dio && (dio.read_bps != null || dio.write_bps != null || dio.read_iops != null)) {
    var dk = {};
    ['read_bps', 'write_bps', 'read_iops', 'write_iops', 'await_ms'].forEach(function (k) {
      if (dio[k] != null) dk[k] = cfRound2(dio[k]);
    });
    if (dio.usage != null) dk.util = cfRound2(dio.usage); // CF 侧字段名叫 util
    m.disk = dk;
  }
  set('load_avg', cfLoad(last.load));
  m.boot_time = st.boot_time || 0;
  set('net_rx', last.net_rx); set('net_tx', last.net_tx);
  set('net_rx_monthly', last.net_rx_monthly); set('net_tx_monthly', last.net_tx_monthly);
  set('net_in_speed', last.net_rx_speed); set('net_out_speed', last.net_tx_speed);
  m.os = st.os || ''; m.arch = st.arch || ''; m.kernel_version = st.kernel || '';
  m.cpu_info = st.cpu_name || ''; m.cpu_cores = st.cpu_cores || 0;
  m.agent_version = CF_COMPAT_VERSION + '_probe-rs_' + (st.agent_version || '?');
  m.timestamp = r.received_at;
  if (gpus.length) {
    m.gpu_info = gpus.map(function (g, i) {
      return { id: String(i), name: g.name, info: cfRound2(g.usage) || 0 };
    });
  }
  set('processes', slow && slow.processes);
  set('tcp_conn', slow && slow.tcp_conn);
  set('udp_conn', slow && slow.udp_conn);
  m.ip_v4 = st.ipv4 || 0; m.ip_v6 = st.ipv6 || 0;
  var groups = [['ct', ['ct']], ['cu', ['cu']], ['cm', ['cm']], ['bd', ['bd', 'bgp']]];
  groups.forEach(function (g) {
    var p = pingOf(g[1]);
    if (p) m['ping_' + g[0]] = p.rtt;
    var l = lossOf(g[1]);
    if (l !== undefined) m['loss_' + g[0]] = l;
  });
  var samples = [];
  /* ext.cf.batch=false 时 agent 不发送 samples 字段（skip_serializing_if） */
  var cfExt = (st.config && st.config.ext && st.config.ext.cf) || {};
  if (cfExt.batch !== false) {
    samples = dyn.map(function (d) {
      var sm = {};
      function sset(k, v) { if (v !== null && v !== undefined) sm[k] = v; }
      sset('cpu', cfRound2(d.cpu_usage));
      sset('ram_used', cfMb(d.mem_used));
      sset('swap_used', cfMb(d.swap_used));
      sset('load_avg', cfLoad(d.load));
      sset('net_rx', d.net_rx); sset('net_tx', d.net_tx);
      sset('net_in_speed', d.net_rx_speed); sset('net_out_speed', d.net_tx_speed);
      sset('net_rx_monthly', d.net_rx_monthly); sset('net_tx_monthly', d.net_tx_monthly);
      return { ts: d.ts, metrics: sm };
    });
  }
  var out = { id: r.server_id, secret: '<API_SECRET>', metrics: m };
  if (samples.length) out.samples = samples;
  return out;
}

/* CF 配置下发串预览（对齐 CF 服务端 buildAgentConfig 的输出格式） */
function cfConfigString(cf) {
  var pings = {};
  (cf.pings || []).forEach(function (p) { pings[p.name] = p.target; });
  var iv = cf.intervals || {};
  return 'collect_interval=' + (iv.collect ?? 0)
    + '&report_interval=' + (iv.report ?? 60)
    + '&reset_day=' + (cf.reset_day ?? 1)
    + '&schema_version=3'
    + '&custom_ct=' + (pings.ct || '')
    + '&custom_cu=' + (pings.cu || '')
    + '&custom_cm=' + (pings.cm || '')
    + '&custom_bd=' + (pings.bd || pings.bgp || '')
    + '&interface=' + ((cf.interfaces || [])[0] || '');
}
function el(tag, cls, text) { var e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; }
function fmtB(n) { if (n == null) return '–';
  if (n >= 1e12) return (n/1e12).toFixed(2)+' TB'; if (n >= 1e9) return (n/1e9).toFixed(2)+' GB';
  if (n >= 1e6) return (n/1e6).toFixed(1)+' MB'; if (n >= 1e3) return (n/1e3).toFixed(1)+' KB';
  return n+' B'; }
function fmtS(n) { return n == null ? '–' : fmtB(n)+'/s'; }
function pad2(n) { return String(n).padStart(2,'0'); }
function fmtTime(ts) { var d = new Date(ts); return pad2(d.getHours())+':'+pad2(d.getMinutes())+':'+pad2(d.getSeconds()); }
function fmtHM(ts) { var d = new Date(ts); return pad2(d.getHours())+':'+pad2(d.getMinutes()); }
/* 配置版本展示：UTC+8 时间戳字符串缩短为 "08-06 15:30:45"，其他原样 */
function fmtVer(v) { if (v == null || v === '') return '–'; var s = String(v);
  return /^\\d{4}-\\d{2}-\\d{2}T/.test(s) ? s.slice(5, 19).replace('T', ' ') : s; }
/* 展示归并（前端职责）：取每字段最近的非 null 值 */
function mergeLatest(recent) {
  var d = {};
  for (var i = recent.length - 1; i >= 0; i--) {
    var r = recent[i];
    for (var k in r) if (k !== 'ts' && r[k] != null && d[k] == null) d[k] = r[k];
  }
  return d;
}

/* ---- 折线图 ---- */
function makeChart(opts) {
  var root = el('div', 'chart');
  root.append(el('div', 'chart-title', opts.title));
  var hidden = {};
  if (opts.series.length > 1) {
    var legend = el('div', 'legend');
    opts.series.forEach(function (s, si) {
      var item = el('button', 'legend-item'); item.type = 'button';
      var key = el('span', 'legend-key'); key.style.background = s.color;
      item.append(key, el('span', null, s.name));
      item.onclick = function () { hidden[si] = !hidden[si]; item.classList.toggle('off'); draw(); };
      legend.append(item);
    });
    root.append(legend);
  }
  var plot = el('div', 'plot'); plot.tabIndex = 0;
  var canvas = document.createElement('canvas');
  var cross = el('div', 'crosshair'); cross.style.display = 'none';
  var tip = el('div', 'tooltip'); tip.style.display = 'none';
  plot.append(canvas, cross, tip);
  root.append(plot);

  var H = 132, PL = 46, PR = opts.endLabels ? 58 : 10, PT = 10, PB = 20;
  var pts = opts.series.map(function (s) { return s.points; });

  function tRange() {
    var lo = Infinity, hi = -Infinity;
    pts.forEach(function (p) { p.forEach(function (pt) { if (pt.t < lo) lo = pt.t; if (pt.t > hi) hi = pt.t; }); });
    return [lo, hi];
  }
  function niceMax(v) {
    if (v <= 0) return 1;
    var p = Math.pow(10, Math.floor(Math.log10(v)));
    var m = v / p;
    var n = m <= 1 ? 1 : m <= 2 ? 2 : m <= 2.5 ? 2.5 : m <= 5 ? 5 : 10;
    return n * p;
  }
  function draw() {
    var W = plot.clientWidth || 300;
    var dpr = window.devicePixelRatio || 1;
    canvas.width = W * dpr; canvas.height = H * dpr;
    canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
    var ctx = canvas.getContext('2d'); ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    var tr = tRange(), t0 = tr[0], t1 = tr[1];
    if (!isFinite(t0)) return;
    var x = function (t) { return PL + (t - t0) / Math.max(t1 - t0, 1) * (W - PL - PR); };
    var ymax = opts.ymax || 0;
    if (!ymax) {
      pts.forEach(function (p, si) { if (!hidden[si]) p.forEach(function (pt) { if (pt.v != null && pt.v > ymax) ymax = pt.v; }); });
      ymax = niceMax(ymax);
    }
    var y = function (v) { return PT + (1 - Math.min(v / ymax, 1)) * (H - PT - PB); };
    var ticks = opts.ymax === 100 ? [0, 25, 50, 75, 100] : [0, ymax / 2, ymax];
    ctx.font = '10px -apple-system, sans-serif'; ctx.lineWidth = 1;
    ticks.forEach(function (tv) {
      ctx.strokeStyle = CHART.grid; ctx.beginPath();
      ctx.moveTo(PL, y(tv) + .5); ctx.lineTo(W - PR, y(tv) + .5); ctx.stroke();
      ctx.fillStyle = CHART.label; ctx.textAlign = 'right'; ctx.fillText(opts.unit(tv), PL - 6, y(tv) + 3);
    });
    ctx.fillStyle = CHART.label;
    ctx.textAlign = 'left'; ctx.fillText(fmtHM(t0), PL, H - 6);
    ctx.textAlign = 'center'; ctx.fillText(fmtHM((t0 + t1) / 2), x((t0 + t1) / 2), H - 6);
    ctx.textAlign = 'right'; ctx.fillText(fmtHM(t1), W - PR, H - 6);
    opts.series.forEach(function (s, si) {
      if (hidden[si]) return;
      var p = pts[si];
      var single = opts.series.length === 1 && !opts.noArea;
      if (single) {
        ctx.beginPath(); var started = false, firstX = 0, lastX = 0;
        p.forEach(function (pt) { if (pt.v == null) return;
          if (!started) { ctx.moveTo(x(pt.t), y(pt.v)); firstX = x(pt.t); started = true; }
          else ctx.lineTo(x(pt.t), y(pt.v)); lastX = x(pt.t); });
        if (started) {
          ctx.lineTo(lastX, y(0)); ctx.lineTo(firstX, y(0)); ctx.closePath();
          ctx.fillStyle = s.color + '14'; ctx.fill();
        }
      }
      ctx.strokeStyle = s.color; ctx.lineWidth = 1.5; ctx.lineJoin = 'round'; ctx.lineCap = 'round';
      ctx.beginPath(); var started2 = false;
      p.forEach(function (pt) { if (pt.v == null) { started2 = false; return; }
        var px = x(pt.t), py = y(pt.v);
        started2 ? ctx.lineTo(px, py) : ctx.moveTo(px, py); started2 = true; });
      ctx.stroke();
      for (var i = p.length - 1; i >= 0; i--) {
        if (p[i].v == null) continue;
        var ex = x(p[i].t), ey = y(p[i].v);
        ctx.fillStyle = CHART.dotBg; ctx.beginPath(); ctx.arc(ex, ey, 5.5, 0, 7); ctx.fill();
        ctx.fillStyle = s.color; ctx.beginPath(); ctx.arc(ex, ey, 3.5, 0, 7); ctx.fill();
        if (opts.endLabels) {
          ctx.fillStyle = CHART.endText; ctx.textAlign = 'left';
          ctx.fillText(opts.unit(p[i].v), ex + 9, ey + 3);
        }
        break;
      }
    });
  }
  function nearest(t) {
    var best = null, bd = Infinity;
    pts.forEach(function (p, si) { if (hidden[si]) return;
      p.forEach(function (pt) { var d = Math.abs(pt.t - t); if (d < bd) { bd = d; best = pt.t; } });
    });
    return best;
  }
  function show(t) {
    var W = plot.clientWidth, tr = tRange();
    var px = PL + (t - tr[0]) / Math.max(tr[1] - tr[0], 1) * (W - PL - PR);
    cross.style.display = 'block'; cross.style.left = px + 'px';
    tip.textContent = '';
    tip.append(el('div', 'tt-time', fmtTime(t)));
    opts.series.forEach(function (s, si) {
      if (hidden[si]) return;
      var pt = null, bd = Infinity;
      pts[si].forEach(function (q) { var d = Math.abs(q.t - t); if (d < bd) { bd = d; pt = q; } });
      if (!pt || pt.v == null) return;
      var row = el('div', 'tt-row');
      var key = el('span', 'tt-key'); key.style.background = s.color;
      row.append(key, el('span', 'tt-val', opts.unit(pt.v)), el('span', 'tt-name', s.name));
      tip.append(row);
    });
    tip.style.display = 'block';
    var tw = tip.offsetWidth;
    tip.style.left = (px + tw + 16 > W ? px - tw - 12 : px + 12) + 'px';
    tip.style.top = '8px';
  }
  plot.addEventListener('pointermove', function (e) {
    var r = plot.getBoundingClientRect(), W = r.width, tr = tRange();
    if (!isFinite(tr[0])) return;
    var t = tr[0] + (e.clientX - r.left - PL) / (W - PL - PR) * (tr[1] - tr[0]);
    var nt = nearest(t); if (nt != null) show(nt);
  });
  plot.addEventListener('pointerleave', function () { cross.style.display = 'none'; tip.style.display = 'none'; });
  plot.addEventListener('focus', function () { var tr = tRange(); if (isFinite(tr[0])) show(tr[1]); });
  plot.addEventListener('blur', function () { cross.style.display = 'none'; tip.style.display = 'none'; });
  draw();
  return { el: root, redraw: draw };
}

/* ---- KPI 磁贴 ---- */
function tile(label, value, extra, meterPct) {
  var t = el('div', 'tile');
  t.append(el('div', 'label', label), el('div', 'value', value));
  if (extra) t.append(el('div', 'extra', extra));
  if (meterPct != null) {
    var m = el('div', 'meter' + (meterPct > 90 ? ' crit' : meterPct > 70 ? ' warn' : ''));
    var fill = el('div'); fill.style.width = Math.min(meterPct, 100) + '%';
    m.append(fill); t.append(m);
  }
  return t;
}

function detailRow(tbody, cells) {
  var tr = el('tr');
  cells.forEach(function (c, i) { tr.append(el('td', i % 2 === 0 ? 'k' : 'v', c)); });
  tbody.append(tr);
}

/* ---- 监控视图 ---- */
var charts = [];
function render(data) {
  if (view !== 'dash') return;
  charts = [];
  var app = document.getElementById('app');
  app.textContent = '';
  var online = data.filter(function (s) { return s.online; }).length;
  document.getElementById('summary').textContent = '在线 ' + online + ' / ' + data.length;
  if (!data.length) {
    var e = el('div', 'empty');
    e.append(el('p', null, '暂无服务器数据'), el('code', null, 'probe-rs -c config.toml'));
    app.append(e); return;
  }
  data.forEach(function (s) {
    var st = s.static || {}, d = mergeLatest(s.recent || []);
    var slow = s.slow_latest || {};
    var card = el('div', 'card' + (s.online ? '' : ' offline'));
    var head = el('div', 'card-head');
    head.append(
      el('span', 'dot ' + (s.online ? 'on' : 'off')),
      el('span', 'name', s.server_id),
      el('span', 'meta', [st.os, st.arch, 'agent v' + (st.agent_version || '?'), 'cfg ' + fmtVer(s.config_version)].filter(Boolean).join(' · ')));
    if (s.pending_config) head.append(el('span', 'badge', '待下发 ' + fmtVer(s.pending_config.config_version)));
    card.append(head);
    var tiles = el('div', 'tiles');
    var memPct = st.mem_total ? (d.mem_used || 0) / st.mem_total * 100 : null;
    var diskPct = st.disk_total && slow.disk_used != null ? slow.disk_used / st.disk_total * 100 : null;
    tiles.append(
      tile('CPU', d.cpu_usage != null ? d.cpu_usage.toFixed(1) + '%' : '–', null, d.cpu_usage),
      tile('内存', fmtB(d.mem_used), memPct != null ? '/ ' + fmtB(st.mem_total) : null, memPct),
      tile('磁盘', fmtB(slow.disk_used), diskPct != null ? '/ ' + fmtB(st.disk_total) : null, diskPct),
      tile('下行', fmtS(d.net_rx_speed), '本月 ' + fmtB(d.net_rx_monthly)),
      tile('上行', fmtS(d.net_tx_speed), '本月 ' + fmtB(d.net_tx_monthly)),
      tile('TCP / UDP', (slow.tcp_conn ?? '–') + ' / ' + (slow.udp_conn ?? '–'), '进程 ' + (slow.processes ?? '–')));
    card.append(tiles);
    var recent = s.recent || [];
    var chartBox = el('div', 'charts');
    var cpuC = makeChart({ title: 'CPU %', ymax: 100, unit: function (v) { return v % 1 ? v.toFixed(1) : v; },
      series: [{ name: 'CPU %', color: PALETTE[0], points: recent.map(function (r) { return { t: r.ts, v: r.cpu_usage }; }) }] });
    var netC = makeChart({ title: '网速', unit: fmtB,
      series: [
        { name: '↓ 下行', color: PALETTE[0], points: recent.map(function (r) { return { t: r.ts, v: r.net_rx_speed }; }) },
        { name: '↑ 上行', color: PALETTE[1], points: recent.map(function (r) { return { t: r.ts, v: r.net_tx_speed }; }) }] });
    chartBox.append(cpuC.el, netC.el);
    var byTarget = {};
    (s.ping_list || []).forEach(function (p) {
      (byTarget[p.name] = byTarget[p.name] || []).push({ t: p.ts, v: p.rtt >= 0 ? p.rtt : null });
    });
    var targets = Object.keys(byTarget).sort();
    var ioSeries = (s.diskio_list || []).filter(function (d) { return d.read_bps != null; });
    if (ioSeries.length) {
      var ioC = makeChart({ title: '磁盘 IO', unit: fmtB,
        series: [
          { name: '↓ 读', color: PALETTE[0], points: ioSeries.map(function (d) { return { t: d.ts, v: d.read_bps }; }) },
          { name: '↑ 写', color: PALETTE[1], points: ioSeries.map(function (d) { return { t: d.ts, v: d.write_bps }; }) }] });
      chartBox.append(ioC.el);
      charts.push(ioC);
    }
    if (targets.length) {
      var pingC = makeChart({ title: '探测延迟 (ms)', unit: function (v) { return v + 'ms'; }, endLabels: targets.length <= 4, noArea: true,
        series: targets.map(function (name, i) { return { name: name, color: PALETTE[i % PALETTE.length], points: byTarget[name] }; }) });
      chartBox.append(pingC.el);
    }
    charts.push(cpuC, netC);
    card.append(chartBox);
    var tbl = el('table', 'detail'), tb = el('tbody');
    detailRow(tb, ['CPU', (st.cpu_name || '–') + ' ×' + (st.cpu_cores || '?'), '负载', d.load ? d.load.map(function (x) { return x.toFixed(2); }).join('  ') : '–']);
    detailRow(tb, ['内存', fmtB(d.mem_used) + ' / ' + fmtB(st.mem_total), 'Swap', fmtB(d.swap_used) + ' / ' + fmtB(st.swap_total)]);
    detailRow(tb, ['磁盘', fmtB(slow.disk_used) + ' / ' + fmtB(st.disk_total), '累计流量', '↓ ' + fmtB(d.net_rx) + '  ↑ ' + fmtB(d.net_tx)]);
    detailRow(tb, ['月流量', '↓ ' + fmtB(d.net_rx_monthly) + '  ↑ ' + fmtB(d.net_tx_monthly), '开机时间', st.boot_time ? new Date(st.boot_time).toLocaleString('zh-CN') : '–']);
    detailRow(tb, ['公网 IP', (st.ipv4 || '–') + '  /  ' + (st.ipv6 || '–'), '内核', (st.kernel || '–') + (st.virtualization ? ' · ' + st.virtualization : '')]);
    if (s.gpu_latest) {
      var g = s.gpu_latest;
      var gText = g.name
        + (g.usage != null ? ' ' + g.usage + '%' : '')
        + (g.temp != null ? ' · ' + g.temp + '°C' : '')
        + (g.mem_total ? ' · 显存 ' + fmtB(g.mem_used) + ' / ' + fmtB(g.mem_total) : '');
      detailRow(tb, ['GPU', gText, '', '']);
    }
    if (targets.length) detailRow(tb, ['探测', targets.map(function (name) {
      var raw = (s.ping_list || []).filter(function (p) { return p.name === name; }).at(-1);
      return name + ' ' + (raw && raw.rtt < 0 ? '失败' : (raw ? raw.rtt + 'ms/丢' + raw.loss + '%' : '–'));
    }).join('　'), '', '']);
    detailRow(tb, ['最近上报', new Date(s.last_seen).toLocaleString('zh-CN'), '缓存样本', String(s.dynamic_count)]);
    if (s.diskio_latest && s.diskio_latest.read_bps != null) {
      var io = s.diskio_latest;
      detailRow(tb, ['磁盘 IO', '读 ' + fmtS(io.read_bps) + ' · 写 ' + fmtS(io.write_bps)
        + ' · iops ' + (io.read_iops != null ? Math.round(io.read_iops + (io.write_iops || 0)) : '–'),
        'await / usage', (io.await_ms != null ? io.await_ms.toFixed(1) + 'ms' : '–')
        + ' / ' + (io.usage != null ? io.usage.toFixed(1) + '%' : '–')]);
    }
    if (s.self_latest) {
      detailRow(tb, ['探针自身', 'CPU ' + (s.self_latest.cpu_usage != null ? s.self_latest.cpu_usage.toFixed(1) + '%' : '–')
        + ' · RSS ' + fmtB(s.self_latest.mem_rss), '', '']);
    }
    if ((s.errors || []).length) {
      var tr = el('tr');
      tr.append(el('td', 'k', '⚠ 错误'));
      var td = el('td', 'v err', s.errors.map(function (e) {
        return '[' + e.source + '] ' + e.msg + '（' + fmtTime(e.ts) + '）';
      }).join('；'));
      tr.append(td, el('td', 'k', '累计'), el('td', 'v', String(s.error_count)));
      tb.append(tr);
    }
    tbl.append(tb); card.append(tbl);
    app.append(card);
  });
}

/* ---- 上报记录视图：inset 列表 + 详情，支持按机器过滤 ---- */
var rptKnown = {};
var rptData = {};
var rptSelected = null;
var rptFilter = '';   // '' = 全部机器

function rptRow(r) {
  var row = el('div', 'rpt-row');
  row.dataset.seq = r.seq;
  /* async 按 kind 拆开统计：ping ×2 · slow ×1 · gpu ×1 · self ×1 */
  var kindCount = {};
  ((r.report && r.report.async) || []).forEach(function (a) {
    kindCount[a.kind] = (kindCount[a.kind] || 0) + 1;
  });
  var kindSum = ['ping', 'slow', 'gpu', 'self', 'diskio'].filter(function (k) { return kindCount[k]; })
    .map(function (k) { return k + ' ×' + kindCount[k]; }).join(' · ');
  var sum = (r.has_static ? 'static · ' : '') + 'dynamic ×' + r.dynamic_count
    + (kindSum ? ' · ' + kindSum : '')
    + (r.error_count ? ' · ⚠ ×' + r.error_count : '');
  row.append(
    el('span', 'rpt-seq', '#' + r.seq),
    el('span', 'rpt-time', fmtTime(r.received_at)),
    el('span', 'rpt-id', r.server_id),
    el('span', 'rpt-sum', sum));
  row.onclick = function () { selectReport(r.seq); };
  return row;
}

function selectReport(seq) {
  rptSelected = seq;
  var list = document.getElementById('rpt-list');
  Array.prototype.forEach.call(list.children, function (row) {
    row.classList.toggle('sel', Number(row.dataset.seq) === seq);
  });
  var detail = document.getElementById('rpt-detail');
  detail.textContent = '';
  var r = rptData[seq];
  if (!r) return;
  detail.append(
    el('div', 'd-head', '#' + seq + ' · ' + r.server_id + ' · ' + new Date(r.received_at).toLocaleString('zh-CN')),
    el('pre', 'code', JSON.stringify(r.report, null, 2)));
}

function renderReports(list) {
  var app = document.getElementById('app');
  if (!document.getElementById('rpt-list')) {
    app.textContent = '';
    var bar = el('div', 'rpt-bar');
    bar.append(el('span', 'lab', '机器'));
    var sel = el('select'); sel.id = 'rpt-filter';
    var optAll = el('option', null, '全部'); optAll.value = ''; sel.append(optAll);
    sel.onchange = function () { rptFilter = sel.value; rebuildRptList(); };
    bar.append(sel);
    var listDiv = el('div', 'rpt-list'); listDiv.id = 'rpt-list';
    var detailDiv = el('div', 'rpt-detail'); detailDiv.id = 'rpt-detail';
    app.append(bar, listDiv, detailDiv);
    rptKnown = {}; rptData = {}; rptSelected = null;
  }
  document.getElementById('summary').textContent = '最近 ' + list.length + ' 条上报';
  /* 合并新记录 + 维护机器过滤选项 */
  var sel = document.getElementById('rpt-filter');
  list.forEach(function (r) {
    if (rptKnown[r.seq]) return;
    rptKnown[r.seq] = 1;
    rptData[r.seq] = r;
    var exists = false;
    Array.prototype.forEach.call(sel.options, function (o) { if (o.value === r.server_id) exists = true; });
    if (!exists) { var o = el('option', null, r.server_id); o.value = r.server_id; sel.append(o); }
  });
  rebuildRptList();
}

function rebuildRptList() {
  var listEl = document.getElementById('rpt-list');
  if (!listEl) return;
  listEl.textContent = '';
  /* rptData 按 seq 降序（新→旧），应用机器过滤 */
  var seqs = Object.keys(rptData).map(Number).sort(function (a, b) { return b - a; });
  var shown = 0;
  seqs.forEach(function (seq) {
    var r = rptData[seq];
    if (rptFilter && r.server_id !== rptFilter) return;
    var row = rptRow(r);
    row.classList.toggle('sel', seq === rptSelected);
    listEl.append(row);
    shown++;
  });
  if (!shown) listEl.append(el('div', 'empty', rptFilter ? '该机器暂无上报记录' : '暂无上报记录'));
  else if (rptSelected == null || !rptData[rptSelected]
      || (rptFilter && rptData[rptSelected].server_id !== rptFilter)) {
    selectReport(rptData[Number(listEl.firstChild.dataset.seq)].seq);
  }
}

/* ---- 配置编辑：每项一个「更新」勾选框，勾中才可编辑；未勾选项保持现值 ----
   intervals 特例：6 项里勾选任意一项即整体下发（collect/report/ping 必填，
   未勾选的按 static 里当前生效值补齐；slow/gpu/ip 缺省 60/60/600） */
var cfgFormActive = false;  // 编辑中暂停 WS 重渲染，避免表单被上报推送刷掉

function cfgGroup(title, rows) {
  var g = el('div', 'cfg-group');
  g.append(el('h3', null, title));
  rows.forEach(function (r) {
    var row = el('div', 'cfg-item plain');
    row.append(el('span', 'lab', r[0]), el('span', 'cur', r[1]));
    g.append(row);
  });
  return g;
}

function cfgEditForm(s, st) {
  var cf = st.config || {};          // static.config.*：当前生效配置
  var iv = cf.intervals || {};
  var wrap = el('div');
  var groups = el('div', 'cfg-groups');
  wrap.append(groups);
  var items = [];
  var intItems = {};
  function group(title) {
    var g = el('div', 'cfg-group');
    g.append(el('h3', null, title));
    groups.append(g);
    return g;
  }
  function editRow(g, label, curText, input, apply) {
    var row = el('div', 'cfg-item');
    var lab = el('label', 'lab');
    var cb = el('input'); cb.type = 'checkbox';
    lab.append(cb, el('span', null, label));
    input.disabled = true;
    cb.onchange = function () {
      input.disabled = !cb.checked;
      row.classList.toggle('on', cb.checked);
      if (cb.checked) input.focus();
    };
    row.append(lab, el('span', 'cur', curText), input);
    g.append(row);
    items.push({ cb: cb, apply: function (p) { apply(p, input); } });
    return row;
  }
  function plainRow(g, label, curText) {
    var row = el('div', 'cfg-item plain');
    row.append(el('span', 'lab', label), el('span', 'cur', curText));
    g.append(row);
  }
  function numInput(cur, min, max) {
    var inp = el('input'); inp.type = 'number'; inp.min = String(min);
    if (max != null) inp.max = String(max);
    if (cur != null) inp.value = cur;
    return inp;
  }
  function sec(v) { return v != null ? v + ' s' : '–'; }

  var gCollect = group('采集');
  var gReport = group('上报');
  var gAsync = group('异步');
  [['collect', '采样间隔 collect', gCollect],
   ['report', '上报间隔 report', gReport],
   ['ping', '探测 ping', gAsync],
   ['slow', '慢变指标 slow', gAsync],
   ['gpu', 'GPU gpu', gAsync],
   ['ip', '公网 IP ip', gAsync],
   ['diskio', '磁盘 IO diskio', gAsync]].forEach(function (def) {
    var k = def[0], inp = numInput(iv[k], 1);
    editRow(def[2], def[1], sec(iv[k]), inp, function () {});
    intItems[k] = { cb: items[items.length - 1].cb, inp: inp };
  });
  plainRow(gReport, '配置版本 config_version', fmtVer(s.config_version));
  editRow(gCollect, '流量重置日 reset_day',
    cf.reset_day != null ? (cf.reset_day === 0 ? '不重置' : '每月 ' + cf.reset_day + ' 号') : '–',
    numInput(cf.reset_day, 0, 31),
    function (p, inp) {
      var v = Number(inp.value);
      if (!Number.isInteger(v) || v < 0 || v > 31) throw 'reset_day 必须在 0-31';
      p.reset_day = v;
    });

  var gBool = group('开关');
  [['enable_gpu', 'GPU 采集 enable_gpu'],
   ['report_errors', '错误上报 report_errors'],
   ['report_self', '自身占用 report_self']].forEach(function (def) {
    var sel = el('select');
    [['true', '开'], ['false', '关']].forEach(function (o) {
      var opt = el('option', null, o[1]); opt.value = o[0]; sel.append(opt);
    });
    if (cf[def[0]] != null) sel.value = String(cf[def[0]]);
    var cur = cf[def[0]] == null ? '–' : (cf[def[0]] ? '开' : '关');
    editRow(gBool, def[1], cur, sel, function (p, inp) { p[def[0]] = inp.value === 'true'; });
  });

  var gIface = group('网卡 interfaces');
  var ifInp = el('input'); ifInp.type = 'text'; ifInp.className = 'wide';
  ifInp.placeholder = 'eth*, ens*（留空 = 清空）';
  if (Array.isArray(cf.interfaces) && cf.interfaces.length) ifInp.value = cf.interfaces.join(', ');
  var ifCur = Array.isArray(cf.interfaces)
    ? (cf.interfaces.length ? cf.interfaces.join(', ') : '（空）') : '–';
  editRow(gIface, '白名单（glob，逗号分隔）', ifCur, ifInp, function (p, inp) {
    p.interfaces = inp.value.split(',').map(function (x) { return x.trim(); }).filter(Boolean);
  });

  var gPing = group('探测目标 pings（勾选后整体替换）');
  var pingHead = el('div', 'cfg-item plain');
  var pingLab = el('label', 'lab');
  var pingCb = el('input'); pingCb.type = 'checkbox';
  pingLab.append(pingCb, el('span', null, '更新 pings'));
  var pingCur = Array.isArray(cf.pings) && cf.pings.length
    ? cf.pings.map(function (p) { return p.name; }).join(', ')
    : '（空）';
  pingHead.append(pingLab, el('span', 'cur', pingCur));
  gPing.append(pingHead);
  var pingRows = el('div', 'cfg-pings');
  gPing.append(pingRows);
  function addPingRow(name, target, interval) {
    var r = el('div', 'ping-row');
    var n = el('input'); n.placeholder = 'name';
    var t = el('input'); t.placeholder = 'host[:port] / https://…';
    var i = el('input'); i.type = 'number'; i.min = '1'; i.placeholder = '间隔 s';
    if (name) n.value = name;
    if (target) t.value = target;
    if (interval != null) i.value = interval;
    var del = el('button', 'ping-del', '✕'); del.type = 'button'; del.title = '删除';
    del.onclick = function () { r.remove(); };
    [n, t, i].forEach(function (x) { x.disabled = !pingCb.checked; });
    r.append(n, t, i, del);
    pingRows.append(r);
  }
  pingCb.onchange = function () {
    Array.prototype.forEach.call(pingRows.querySelectorAll('input'), function (x) {
      x.disabled = !pingCb.checked;
    });
  };
  var prefill = (Array.isArray(cf.pings) && cf.pings.length ? cf.pings
    : (s.pending_config && s.pending_config.pings)) || [];
  if (prefill.length) prefill.forEach(function (p) { addPingRow(p.name, p.target, p.interval); });
  else addPingRow();
  var addBtn = el('button', 'btn ping-add', '＋ 添加目标'); addBtn.type = 'button';
  addBtn.onclick = function () { addPingRow(); };
  gPing.append(addBtn);

  var gStatic = group('静态（只读）');
  plainRow(gStatic, 'static 刷新周期', '600 s（固定）');
  plainRow(gStatic, 'static 采集于', st.ts ? new Date(st.ts).toLocaleString('zh-CN') : '–');
  plainRow(gStatic, 'agent 版本', 'v' + (st.agent_version || '?'));

  var foot = el('div', 'cfg-foot');
  var btn = el('button', 'btn primary', '下发配置'); btn.type = 'button';
  var status = el('span', 'cfg-status');
  foot.append(btn, status, el('span', 'cfg-hint', '勾选「更新」的项才会下发，随下次上报便车生效'));
  wrap.append(foot);
  wrap.addEventListener('focusin', function () { cfgFormActive = true; });

  var INT_KEYS = ['collect', 'report', 'ping', 'slow', 'gpu', 'ip', 'diskio'];
  var INT_DEFAULT = { slow: 60, gpu: 60, ip: 600, diskio: 10 };
  function buildIntervals() {
    if (!INT_KEYS.some(function (k) { return intItems[k].cb.checked; })) return null;
    var out = {};
    INT_KEYS.forEach(function (k) {
      var it = intItems[k];
      var v = it.cb.checked ? Number(it.inp.value)
        : (iv[k] != null ? iv[k] : INT_DEFAULT[k]);
      if (!Number.isInteger(v) || v < 1) {
        throw k + ' 无法确定：请勾选并填写（或等 static 上报当前生效值）';
      }
      out[k] = v;
    });
    return out;
  }

  btn.onclick = function () {
    var payload = {};
    try {
      var ints = buildIntervals();
      if (ints) INT_KEYS.forEach(function (k) { payload[k] = ints[k]; });
      items.forEach(function (it) { if (it.cb.checked) it.apply(payload); });
      if (pingCb.checked) {
        var ps = [];
        Array.prototype.forEach.call(pingRows.children, function (r) {
          var ins = r.querySelectorAll('input');
          var name = ins[0].value.trim(), target = ins[1].value.trim(), intv = ins[2].value.trim();
          if (!name && !target) return;
          if (!name || !target) throw 'pings 存在只填了一半的行（name/target 必填）';
          var p = { name: name, target: target };
          if (intv) p.interval = Number(intv);
          ps.push(p);
        });
        payload.pings = ps;
      }
    } catch (e) { status.textContent = String(e.message || e); return; }
    if (!Object.keys(payload).length) { status.textContent = '没有勾选任何更新项'; return; }
    btn.disabled = true;
    fetch('/api/config/' + encodeURIComponent(s.server_id), {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload),
    }).then(function (r) {
      return r.json().then(function (body) { return { ok: r.ok, body: body }; });
    }).then(function (res) {
      btn.disabled = false;
      if (res.ok) {
        cfgFormActive = false;
        status.textContent = '已加入待下发 ✓ ' + fmtVer(res.body.pending && res.body.pending.config_version);
      } else {
        status.textContent = '被拒绝: ' + (res.body.error || '未知错误');
      }
    }).catch(function (e) { btn.disabled = false; status.textContent = '请求失败: ' + e; });
  };
  return wrap;
}

/* ---- 配置视图 ---- */
function renderConfig(data) {
  var app = document.getElementById('app');
  app.textContent = '';
  document.getElementById('summary').textContent = data.length + ' 台服务器';
  if (!data.length) { app.append(el('div', 'empty', '暂无服务器数据')); return; }
  data.forEach(function (s) {
    var st = s.static || {};
    var card = el('div', 'card' + (s.online ? '' : ' offline'));
    var head = el('div', 'card-head');
    head.append(
      el('span', 'dot ' + (s.online ? 'on' : 'off')),
      el('span', 'name', s.server_id),
      el('span', 'meta', 'agent v' + (st.agent_version || '?')),
      el('span', 'meta', 'cfg ' + fmtVer(s.config_version)));
    if (s.pending_config) head.append(el('span', 'badge', '待下发 ' + fmtVer(s.pending_config.config_version)));
    var spacer = el('span', 'spacer');
    var refreshBtn = el('button', 'btn', '↻ 刷新 static'); refreshBtn.type = 'button';
    refreshBtn.onclick = function () {
      fetch('/api/need-static/' + encodeURIComponent(s.server_id), { method: 'POST' });
      refreshBtn.textContent = '已安排 ✓';
      setTimeout(function () { refreshBtn.textContent = '↻ 刷新 static'; }, 3000);
    };
    head.append(spacer, refreshBtn);
    card.append(head);
    if (s.pending_config) {
      var p = s.pending_config;
      var rows = [['config_version', fmtVer(s.config_version) + ' → ' + fmtVer(p.config_version)]];
      if (p.intervals) {
        rows.push(['collect / report', p.intervals.collect + 's / ' + p.intervals.report + 's'],
          ['ping/slow/gpu/ip/diskio', p.intervals.ping + 's / ' + (p.intervals.slow ?? '–') + 's / ' + (p.intervals.gpu ?? '–') + 's / ' + (p.intervals.ip ?? '–') + 's / ' + (p.intervals.diskio ?? '–') + 's']);
      }
      if (p.reset_day != null) rows.push(['reset_day', String(p.reset_day)]);
      if (p.interfaces) rows.push(['interfaces', p.interfaces.join(', ') || '(空)']);
      if (p.enable_gpu != null) rows.push(['enable_gpu', String(p.enable_gpu)]);
      if (p.report_errors != null) rows.push(['report_errors', String(p.report_errors)]);
      if (p.report_self != null) rows.push(['report_self', String(p.report_self)]);
      if (p.pings) rows.push(['pings', p.pings.map(function (x) { return x.name; }).join(', ') || '(清空)']);
      var pendWrap = el('div', 'cfg-groups');
      pendWrap.append(cfgGroup('⚠ 待下发（下次上报生效）', rows));
      card.append(pendWrap);
    }
    card.append(cfgEditForm(s, st));
    app.append(card);
  });
}

/* ---- CF 预览视图：每台机器展示换算后的 CF 请求体 + 配置下发串 ----
   servers 可传 WS 推送的最新视图（避免每次推送重复拉取） */
function renderCfView(servers) {
  var reportsP = fetch('/api/reports').then(function (r) { return r.json(); });
  var serversP = servers
    ? Promise.resolve(servers)
    : fetch('/api/servers').then(function (r) { return r.json(); });
  Promise.all([serversP, reportsP]).then(function (res) {
    var servers = res[0], reports = res[1];
    var app = document.getElementById('app');
    app.textContent = '';
    document.getElementById('summary').textContent = 'CF 协议视角（由 probe 报文实时换算）';
    servers.forEach(function (s) {
      if (s.static) serverStatics[s.server_id] = s.static;
      serverLatest[s.server_id] = s;
      var card = el('div', 'card' + (s.online ? '' : ' offline'));
      var head = el('div', 'card-head');
      head.append(
        el('span', 'dot ' + (s.online ? 'on' : 'off')),
        el('span', 'name', s.server_id),
        el('span', 'meta', 'POST /update 预览'));
      card.append(head);

      /* 请求体：取该机器最近一条上报换算 */
      var latest = reports.find(function (r) { return r.server_id === s.server_id; });
      var g1 = el('div', 'cfg-groups');
      var b1 = el('div', 'cfg-group');
      b1.append(el('h3', null, '请求体（最近一条上报换算' + (latest ? '，#' + latest.seq : '') + '）'));
      b1.append(el('pre', 'code', latest
        ? JSON.stringify(cfBody(latest), null, 2)
        : '（暂无上报记录）'));
      g1.append(b1);

      /* 配置下发：当前生效配置 → CF URL-encoded 响应 */
      var body = cfConfigString((s.static || {}).config || {});
      var g2 = el('div', 'cfg-groups');
      var b2 = el('div', 'cfg-group');
      b2.append(el('h3', null, '配置下发（CF 响应格式）'));
      b2.append(el('pre', 'code',
        'HTTP 200（配置 MD5 不一致时；一致则 204 No Content）' + String.fromCharCode(10)
        + 'X-Agent-Config-Schema: 3' + String.fromCharCode(10)
        + 'X-Agent-Config-Md5: <md5(下方配置串)>' + String.fromCharCode(10)
        + 'Content-Type: application/x-www-form-urlencoded' + String.fromCharCode(10)
        + String.fromCharCode(10)
        + body));
      b2.append(el('div', 'note', '流量校正在配置串尾部追加 rx_correction/tx_correction（不参与 MD5），确认由 agent 独立请求回传。'));
      g2.append(b2);

      card.append(g1, g2);
      app.append(card);
    });
    if (!servers.length) app.append(el('div', 'empty', '暂无服务器数据'));
  }).catch(function () {});
}

var EXAMPLE_JSON5 = \`{
  "server_id": "srv-01",                    // 服务器 ID（本地配置，远端不可改）
  "config_version": "2026-08-06T15:30:45.123+08:00",   // 当前配置版本（UTC+8 可读时间戳字符串），服务端按「不等」判断是否下发新配置

  // static 可省略：首报必带，之后每 10 分钟或 IP/GPU/配置变化时携带；缺席时服务端保留旧值
  "static": {
    "ts": 1754300050000,                    // static 信息采集时刻（ms）
    "os": "Debian GNU/Linux 12 (bookworm)",
    "kernel": "6.1.0-18-amd64",
    "arch": "x86_64",
    "cpu_name": "Intel(R) Xeon(R) Platinum 8375C",
    "cpu_cores": 8,
    "cpu_physical_cores": 4,                // 可选，未知为 null
    "mem_total": 17179869184,               // 字节
    "swap_total": 1073741824,
    "disk_total": 107374182400,
    "gpu_name": "NVIDIA A100 80GB",         // 可选；无 GPU 为 null
    "virtualization": "kvm",                // 可选；物理机为 null
    "boot_time": 1754300000000,             // ms 时间戳
    "ipv4": "203.0.113.10",                 // 查询失败保留旧值
    "ipv6": "2001:db8::10",                 // 可选；无 v6 出口为 null
    "agent_version": "0.1.0",
    "config": {                             // 当前生效配置（供服务端展示/核对）
      "reset_day": 1,                       // 月流量账期重置日 0-31；0 = 不重置
      "intervals": {                        // 各间隔（秒），完全独立无关系约束
        "collect": 10, "report": 60, "ping": 30, "slow": 60, "gpu": 60, "ip": 600, "diskio": 10
      },
      "interfaces": ["eth*"],               // 网卡白名单（glob）；空 = 所有非虚拟网卡
      "enable_gpu": true,                   // GPU 采集开关
      "report_errors": true,                // 是否上报 errors 错误事件
      "report_self": false,                 // 是否上报探针自身占用 kind:"self"
      "pings": [                            // 探测目标组
        { "name": "ct", "target": "gd-ct-dualstack.ip.zstaticcdn.com:80", "interval": 30 }
      ],
      "ext": { "cf": { "correction": true, "batch": true } }   // 协议扩展（仅 cf 协议生效）
    }
  },

  // 同步采集记录：每个 collect tick 一条；空数组也必报（承担心跳）
  "dynamic": [
    {
      "ts": 1754300060000,                  // 采集时刻（ms），非上报时刻
      "cpu_usage": 12.35,                   // %（0-100）；首轮无前值为 null
      "mem_used": 4294967296,               // 字节，total − MemAvailable
      "swap_used": 134217728,               // 字节
      "load": [0.52, 0.41, 0.30],           // [load1, load5, load15]
      "net_rx": 1073741824,                 // 开机起累计（字节），白名单网卡求和
      "net_tx": 536870912,
      "net_rx_speed": 102400,               // 字节/秒；首轮无前值为 null
      "net_tx_speed": 51200,
      "net_rx_monthly": 858993459200,       // 账期累计（字节），客户端自算
      "net_tx_monthly": 429496729600
    }
  ],

  // 异步记录：仅当对应源快照 ts 更新才产生；kind 区分来源，ts 为各自真实测量时刻
  "async": [
    { "kind": "ping", "ts": 1754300058000, "name": "ct", "rtt": 32, "loss": 0 },
    // 探测结果；name = [[pings]] 组 key；rtt = -1 表示探测失败
    { "kind": "slow", "ts": 1754300055000, "disk_used": 53687091200, "tcp_conn": 120, "udp_conn": 8, "processes": 230 },
    // 系统慢指标（每台机器必有）；disk_used 与 disk_total 同口径；TCP 全状态计数
    { "kind": "gpu", "ts": 1754300050000, "name": "NVIDIA A100 80GB", "usage": 42.5, "mem_total": 85899345920, "mem_used": 10737418240, "temp": 55 },
    // 可选硬件指标（仅部分机器）；多卡时每卡一条；mem/temp 仅 nvidia 路径有，macOS 为 null；无 GPU 时整个 kind 不出现
    { "kind": "self", "ts": 1754300055000, "cpu_usage": 1.2, "mem_rss": 13631488 },
    // 探针自身资源占用；report_self=true 时才有（默认 false）
    { "kind": "diskio", "ts": 1754300056000, "read_bps": 1048576, "write_bps": 524288, "read_iops": 40, "write_iops": 18, "await_ms": 1.8, "usage": 3.2 }
    // 磁盘 IO（整盘合计）；usage 仅 Linux 有，macOS 为 null
  ],

  // 错误事件：采集/上报失败记录，空数组 = 无错误；同源同文去重
  "errors": [
    { "ts": 1754300055000, "source": "gpu", "msg": "nvidia-smi exit 1" },
    { "ts": 1754300058000, "source": "ping:cu", "msg": "dns resolve failed" },
    { "ts": 1754300059000, "source": "reporter", "msg": "connection refused" }
  ]
}\`;

var CONFIG_EXAMPLE_JSON5 = \`{
  // ===== 🔒 不可远端修改（身份与安全边界） =====
  "server_id": "srv-01",            // 🔒 服务器 ID
  "secret": "change-me",            // 🔒 认证密钥，经 X-Secret 头发送
  "worker_url": "https://monitor.example.com/report",   // 🔒 上报地址
  "net_static_path": "/var/lib/probe-rs/net_static.json",   // 🔒 netstatic 流量时序落盘路径

  // ===== 本地或远端均可修改（本地热加载 ~3s；远端便车下发即时） =====
  "reset_day": 1,                   // 月流量账期重置日 1-31；0 = 不重置（永久累计）
  "intervals": {                    // 各间隔完全独立，无任何关系约束
    "collect": 10,                  // 采样间隔（秒）
    "report": 60,                   // 上报间隔（秒）；report < collect 时多余上报只是空数组心跳
    "ping": 30,                     // 探测默认间隔；[[pings]] 组未设 interval 时生效
    "slow": 60,                     // 慢变指标（磁盘/连接数/进程数）采集间隔
    "gpu": 60,                      // GPU 采集间隔
    "ip": 600,                      // 公网 IP 查询间隔
    "diskio": 10                    // 磁盘 IO 采集间隔（macOS 走 ioreg 子进程建议 >= 10）
  },
  "interfaces": [],                 // 网卡白名单（glob，如 "eth*"）；空 = 所有非虚拟网卡
  "enable_gpu": false,              // GPU 采集开关（nvidia-smi；macOS 用 ioreg）
  "report_errors": true,            // 是否上报 errors 错误事件
  "report_self": false,             // 是否上报探针自身资源占用 kind:"self"
  "pings": [                        // 探测目标组：name 为唯一键（远端下发时 name 不可重复）
    {
      "name": "ct",
      "target": "gd-ct-dualstack.ip.zstaticcdn.com:80",   // http(s):// 开头 → HTTP；否则 TCP（host[:port]，默认 80）
      "interval": 30                // 可选；缺省跟随 intervals.ping
    },
    { "name": "baidu", "target": "https://www.baidu.com", "interval": 60 }
  ],

  "config_version": ""              // 配置版本（机制维护，远端下发幂等用，勿手改）；UTC+8 可读时间戳字符串，不等才应用
}\`;

var RESPONSE_EXAMPLE_JSON5 = \`// 无配置变更时：200 OK，body 为 {} 或空
{}

// 有配置变更时（便车下发，随上报响应返回）：
{
  "config": {                       // 配置收在一级；信封后续可扩展其他指令（如动作类）
    "config_version": "2026-08-06T16:00:00.000+08:00",   // 与 agent 当前版本不等才应用（幂等）
    "reset_day": 15,                // 可选；账期重置日 1-31；0 = 不重置
    "intervals": {                  // 各项 >= 1 秒，相互无任何关系约束
      "collect": 10,                // 采样间隔
      "report": 60,                 // 上报间隔
      "ping": 30,                   // 探测默认间隔（[[pings]] 组未设 interval 时生效）
      "slow": 60,                   // 慢变指标采集间隔
      "gpu": 60,                    // GPU 采集间隔
      "ip": 600,                    // 公网 IP 查询间隔
      "diskio": 10                  // 磁盘 IO 采集间隔
    },
    "interfaces": ["eth*"],         // 可选；网卡白名单（glob）
    "enable_gpu": true,             // 可选；GPU 采集开关
    "report_errors": false,         // 可选；是否上报 errors 错误事件
    "report_self": true,            // 可选；是否上报探针自身资源占用 kind:"self"
    "pings": [                      // 可选；探测目标组（整体替换；name 唯一键，不可重复）
      { "name": "ct", "target": "gd-ct-dualstack.ip.zstaticcdn.com:80", "interval": 30 }
    ]
  },
  "next": {                         // 可选；对下一次上报的指令
    "static": true                  // 为 true 时 agent 下次上报强制带 static（一次性）
  }
}\`;

function annotatedCard(text, commentChar, notes) {
  var card = el('div', 'card example');
  var pre = el('pre', 'code');
  /* 注释着色（内容是我们自己的常量，无注入风险） */
  pre.innerHTML = text
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .split(String.fromCharCode(10)).map(function (line) {
      var i = line.indexOf(commentChar);
      return i < 0 ? line : line.slice(0, i) + '<span class="cm">' + line.slice(i) + '</span>';
    }).join(String.fromCharCode(10));
  card.append(pre);
  (notes || []).forEach(function (n) { card.append(el('div', 'note', n)); });
  return card;
}

function renderExample() {
  var app = document.getElementById('app');
  app.textContent = '';
  document.getElementById('summary').textContent = 'POST /report 请求与响应（JSON5 注释版）';
  app.append(
    annotatedCard(EXAMPLE_JSON5, '//', [
      '请求头：X-Secret（认证）、X-Agent-Version。',
      '上报失败时 agent 有界保留（10 条）待重发——只覆盖短暂抖动，长断网历史不补发；服务端收到延迟记录按 ts 去重排序即可。',
    ]),
    annotatedCard(RESPONSE_EXAMPLE_JSON5, '//', [
      'agent 行为：整体校验（config_version 不等才应用、间隔 >= 1、reset_day 0-31），全部通过才应用并落盘、立即生效；任何一项非法则整体拒绝并记录日志。',
      '🔒 不允许远端修改：server_id / secret / worker_url / net_static_path。',
    ]));
}

function renderConfigExample() {
  var app = document.getElementById('app');
  app.textContent = '';
  document.getElementById('summary').textContent = 'agent 本地配置（JSON5 注释版）';
  app.append(annotatedCard(CONFIG_EXAMPLE_JSON5, '//', [
    '实际配置文件为 TOML（仓库根目录 config.example.toml），此处以 JSON5 等价展示。🔒 标记的字段不允许远端修改；「本地或远端均可」字段两条通道都热生效（本地热加载 ~3s / 远端便车即时）。',
  ]));
}

function loadReports() {
  // 同时拉 servers（CF 视角换算需要各机最新 static）
  fetch('/api/servers').then(function (r) { return r.json(); }).then(function (data) {
    data.forEach(function (s) { if (s.static) serverStatics[s.server_id] = s.static; });
  }).catch(function () {});
  fetch('/api/reports').then(function (r) { return r.json(); }).then(renderReports).catch(function () {});
}

/* ---- 视图切换 + WS ---- */
var view = 'dash';
function switchView(v) {
  view = v;
  cfgFormActive = false;
  Array.prototype.forEach.call(document.querySelectorAll('#seg button'), function (b) {
    b.classList.toggle('on', b.dataset.view === v);
  });
  if (v === 'example') { renderExample(); return; }
  if (v === 'cfgex') { renderConfigExample(); return; }
  if (v === 'cfview') { renderCfView(); return; }
  if (v === 'reports') { loadReports(); return; }
  fetch('/api/servers').then(function (r) { return r.json(); }).then(function (data) {
    v === 'config' ? renderConfig(data) : render(data);
  }).catch(function () {});
}
Array.prototype.forEach.call(document.querySelectorAll('#seg button'), function (b) {
  b.onclick = function () { switchView(b.dataset.view); };
});
if (location.hash === '#reports') switchView('reports');
else if (location.hash === '#config') switchView('config');
else if (location.hash === '#cf') switchView('cfview');
else if (location.hash === '#example') switchView('example');
else if (location.hash === '#config-example') switchView('cfgex');

function connect() {
  var proto = location.protocol === 'https:' ? 'wss' : 'ws';
  var ws = new WebSocket(proto + '://' + location.host + '/ws');
  var conn = document.getElementById('conn');
  ws.onopen = function () { conn.textContent = 'WS 实时推送'; conn.classList.add('on'); };
  ws.onmessage = function (e) {
    var data = JSON.parse(e.data);
    if (view === 'dash') render(data);
    else if (view === 'config') { if (!cfgFormActive) renderConfig(data); }
    else if (view === 'reports') loadReports();
    else if (view === 'cfview') renderCfView(data);
  };
  ws.onclose = function () {
    conn.textContent = '重连中…'; conn.classList.remove('on');
    setTimeout(connect, 3000);
  };
  ws.onerror = function () { ws.close(); };
}
window.addEventListener('resize', function () { charts.forEach(function (c) { c.redraw(); }); });
connect();
</script></body></html>`;
