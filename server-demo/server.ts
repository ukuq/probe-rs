#!/usr/bin/env -S deno run --allow-net --allow-env=HOST
/**
 * probe-rs 演示服务端（Deno + TypeScript，零依赖单文件）
 *
 * 实现 REPORT.md 协议：
 *   POST /report            agent 上报（X-Secret 认证），响应可携带远端配置
 *   GET  /                  监控面板（KPI 磁贴 + 趋势图）+ 上报记录调试视图
 *   GET  /api/servers       全部服务器最新数据 JSON
 *   GET  /api/reports       最近收到的原始上报（debug 用）
 *   GET  /ws                面板 WebSocket 实时推送
 *   POST /api/config/:instance_id  设置该 Reporter 的待下发配置
 *
 * 运行: deno run --allow-net --allow-env=HOST server.ts [PORT]
 * 默认 127.0.0.1:8080，密钥见 SECRET
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
  type: "http" | "tcp" | "icmp";
  target: string;
  interval?: number;
}

interface RemoteConfig {
  config_version: string; // 人类可读的 UTC+8 时间戳；与 agent 当前值不等才应用
  intervals?: CollectionIntervals;
  report_interval?: number;
  reset_day?: number;
  interfaces?: string[];
  disks?: string[];
  pings?: PingTarget[];
  report_gpu?: boolean;
  report_errors?: boolean;
  report_self?: boolean;
  ext?: { cf?: { correction?: boolean; batch?: boolean } };
}

interface CollectionIntervals {
  collect: number;
  ping: number;
  slow: number;
  gpu: number;
  ip: number;
  diskio: number;
}

interface GlobalPingTarget {
  target: string;
  interval: number;
}

interface ReporterSummary {
  id: string;
  protocol: string;
  source_collect_interval: number;
  connection_mode?: "auto" | "http";
  ping_mode?: "tcp" | "icmp";
  wss_report_interval?: number;
  intervals: CollectionIntervals;
  report_interval: number;
  reset_day: number;
  interfaces: string[];
  disks: string[];
  report_gpu: boolean;
  report_errors: boolean;
  report_self: boolean;
  pings: PingTarget[];
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
  disks: DiskVolume[];
  gpu_name: string | null;
  virtualization: string | null;
  boot_time: number;
  ipv4: string | null;
  ipv6: string | null;
  agent_version: string;
  /** 当前生效配置（供展示/核对） */
  config: {
    /** Agent 全局实际采集配置；不包含 Reporter 连接信息 */
    global?: {
      intervals: CollectionIntervals;
      enable_gpu: boolean;
      interfaces: string[];
      all_interfaces: boolean;
      disks: string[];
      all_disks: boolean;
      pings: GlobalPingTarget[];
    };
    /** 全部 Reporter 的脱敏摘要；包含 Ping target，不包含上报地址、密钥、server_id 或配置版本 */
    reporters?: ReporterSummary[];
    reset_day: number;
    intervals: Intervals;
    interfaces: string[];
    disks: string[];
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
  disks: DiskVolume[];
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
  id: string;
  name: string;
  usage: number | null;
  mem_total: number | null; // 字节；macOS 为 null（统一内存）
  mem_used: number | null;
  temp: number | null; // ℃；macOS 为 null（需 root）
}

interface DynamicRecord {
  ts: number;
  accurate_ts?: number | null;
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
  net_interfaces: Record<string, {
    rx: number;
    tx: number;
    rx_speed: number;
    tx_speed: number;
    rx_monthly: number | null;
    tx_monthly: number | null;
  }>;
}

/** 异步记录：kind 区分来源，每条 ts 为各自真实测量时刻 */
interface SelfRecord {
  ts: number;
  cpu_usage: number | null; // 自身 CPU 占整机逻辑核总容量的百分比（0-100）
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
  disks: Array<Omit<DiskIoRecord, "ts" | "disks"> & { name: string }>;
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

interface ReportTime {
  local_ts: number;
  accurate_ts: number | null;
  /** accurate_ts - local_ts；正数表示本机慢，负数表示本机快 */
  offset_ms: number | null;
  source: string | null;
  round_trip_ms: number | null;
  sample_age_ms: number | null;
}

interface Report {
  server_id: string;
  config_version: string;
  /** 新 agent 必带；旧 agent 上报可缺席 */
  time?: ReportTime;
  static?: StaticInfo;
  dynamic?: DynamicRecord[];
  async?: AsyncRecord[];
  errors?: ErrorRecord[];
}

// ---------- 存储（全内存，演示定位） ----------

interface ServerState {
  instanceId: string;
  reporterId: string;
  protocol: string;
  serverId: string;
  staticInfo: StaticInfo | null;
  /** agent 版本（X-Agent-Version 头），用于下发兼容判断 */
  agentVersion: string;
  dynamic: DynamicRecord[];
  asyncs: AsyncRecord[];
  errors: ErrorRecord[];
  time: ReportTime | null;
  lastSeen: number;
  configVersion: string;
}

interface RawReport {
  seq: number;
  received_at: number;
  instance_id: string;
  reporter_id: string;
  protocol: string;
  server_id: string;
  report: Report;
}

interface DiskVolume {
  id: string;
  name: string;
  mount_point: string;
  file_system: string;
  total: number;
  used: number;
}

const PORT = Number(Deno.args[0]) || 8080;
// 默认只监听回环:管理接口无鉴权,绑到 0.0.0.0 会把伪造上报与 Ping 下发
// (SSRF) 暴露给局域网。确需对外演示时显式设置 HOST=0.0.0.0。
const HOST = Deno.env.get("HOST") ?? "127.0.0.1";
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

/**
 * 把新到的带 ts 记录合并进现有窗口:按 keyOf 去重(同 key 保留最新到达,
 * 覆盖延迟补发的旧值),整体按 ts 升序排序。REPORT.md:215 约定服务端
 * 对延迟补发记录按 ts 去重/排序。
 */
function mergeTsWindow<T extends { ts: number }>(
  existing: T[],
  incoming: T[],
  keyOf: (item: T) => string,
): T[] {
  if (incoming.length === 0) return existing;
  const merged = new Map<string, T>();
  for (const item of existing) {
    const key = keyOf(item);
    if (!merged.has(key)) merged.set(key, item);
  }
  for (const item of incoming) {
    merged.set(keyOf(item), item);
  }
  return [...merged.values()].sort((a, b) => a.ts - b.ts);
}

/** 同一批 GPU 共享 ts，Ping 也可能在同一毫秒完成；去重键必须包含记录身份。 */
function asyncRecordKey(record: AsyncRecord): string {
  switch (record.kind) {
    case "gpu":
      return JSON.stringify([record.kind, record.ts, record.id || record.name]);
    case "ping":
      return JSON.stringify([record.kind, record.ts, record.name]);
    default:
      return JSON.stringify([record.kind, record.ts]);
  }
}

function broadcast(): void {
  if (!clients.size) return;
  const msg = JSON.stringify(serversView());
  for (const ws of clients) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

function reporterInstanceId(reporterId: string, serverId: string): string {
  return JSON.stringify([reporterId, serverId]);
}

function decodeReporterHeader(value: string | null, fallback: string): string {
  if (!value) return fallback;
  try {
    return decodeURIComponent(value);
  } catch {
    return fallback;
  }
}

function getServer(
  instanceId: string,
  reporterId: string,
  protocol: string,
  serverId: string,
): ServerState {
  let s = servers.get(instanceId);
  if (!s) {
    s = {
      instanceId,
      reporterId,
      protocol,
      serverId,
      staticInfo: null,
      agentVersion: "",
      dynamic: [],
      asyncs: [],
      errors: [],
      time: null,
      lastSeen: 0,
      configVersion: "",
    };
    servers.set(instanceId, s);
  }
  return s;
}

function resolveInstanceId(routeId: string): string | null {
  let decoded = routeId;
  try {
    decoded = decodeURIComponent(routeId);
  } catch {
    return null;
  }
  if (servers.has(decoded)) return decoded;
  const matches = [...servers.values()].filter((s) => s.serverId === decoded);
  if (matches.length === 1) return matches[0].instanceId;
  return matches.find((s) => s.reporterId === "primary")?.instanceId ?? null;
}

// ---------- 协议处理 ----------

function handleReport(req: Request, report: Report): Response {
  const serverId = report.server_id;
  if (!serverId) return json({ error: "missing server_id" }, 400);
  if (req.headers.get("x-secret") !== SECRET) {
    return json({ error: "bad secret" }, 401);
  }
  const reporterId = decodeReporterHeader(
    req.headers.get("x-reporter-id"),
    "primary",
  );
  const protocol = decodeReporterHeader(
    req.headers.get("x-reporter-protocol"),
    "probe",
  );
  const instanceId = reporterInstanceId(reporterId, serverId);
  const receivedAt = Date.now();

  rawReports.push({
    seq: ++reportSeq,
    received_at: receivedAt,
    instance_id: instanceId,
    reporter_id: reporterId,
    protocol,
    server_id: serverId,
    report,
  });
  if (rawReports.length > KEEP_REPORTS) {
    rawReports.splice(0, rawReports.length - KEEP_REPORTS);
  }

  const s = getServer(instanceId, reporterId, protocol, serverId);
  s.lastSeen = receivedAt;
  s.agentVersion = req.headers.get("x-agent-version") ?? s.agentVersion;
  s.configVersion = report.config_version ?? "";
  if (report.time) s.time = report.time;
  if (report.static) s.staticInfo = report.static;
  if (Array.isArray(report.dynamic)) {
    s.dynamic = mergeTsWindow(s.dynamic, report.dynamic, (r) => String(r.ts));
    if (s.dynamic.length > KEEP_DYNAMIC) {
      s.dynamic = s.dynamic.slice(-KEEP_DYNAMIC);
    }
  }
  if (Array.isArray(report.async)) {
    s.asyncs = mergeTsWindow(s.asyncs, report.async, asyncRecordKey);
    if (s.asyncs.length > KEEP_DYNAMIC) {
      s.asyncs = s.asyncs.slice(-KEEP_DYNAMIC);
    }
  }
  if (Array.isArray(report.errors)) {
    // errors 是同源同文去重后的事件流,仅按 ts 排序,不做去重。
    s.errors.push(...report.errors);
    s.errors.sort((a, b) => a.ts - b.ts);
    if (s.errors.length > KEEP_DYNAMIC) {
      s.errors = s.errors.slice(-KEEP_DYNAMIC);
    }
  }

  broadcast();

  // 组装响应信封：config 缺席 = 无变更；next.static = 强制下次带 static（一次性）
  const resp: { config?: RemoteConfig; next?: { static: boolean } } = {};
  const pending = pendingConfig.get(instanceId);
  if (pending && pending.config_version !== s.configVersion) {
    pendingConfig.delete(instanceId);
    resp.config = pending;
    console.log(
      `[config] 下发到 ${serverId}/${reporterId}: ${
        s.configVersion || "(无版本)"
      } -> ${pending.config_version}`,
      pending,
    );
  }
  const autoStatic = !s.staticInfo && !report.static;
  if (needStatic.delete(instanceId) || autoStatic) {
    resp.next = { static: true };
    if (!autoStatic) {
      console.log(`[static] 要求 ${serverId}/${reporterId} 下次上报带 static`);
    }
  }
  // 尽量靠近响应序列化时取值，agent 会结合请求 RTT 中点估算时钟偏差。
  return json({ server_time: Date.now(), ...resp });
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

function pingTargetError(kind: string, target: string): string | null {
  if (target.trim() !== target) return "target 首尾不能有空白";
  if (kind === "http") {
    let url: URL;
    try {
      url = new URL(target);
    } catch {
      return "HTTP target 必须是合法 URL";
    }
    if (!["http:", "https:"].includes(url.protocol) || !url.hostname) {
      return "HTTP target 只允许 http(s)://host[:port]";
    }
    if (url.pathname !== "/" || url.search || url.hash) {
      return "target 不允许 path/query/fragment";
    }
  } else if ([...target].some((ch) => ["/", "\\", "?", "#"].includes(ch))) {
    return "target 不允许 path/query/fragment";
  }
  return null;
}

function handleSetConfig(
  instanceId: string,
  cfg: Record<string, unknown>,
): Response {
  const state = servers.get(instanceId);
  if (!state) return json({ error: "Reporter 实例不存在" }, 404);
  const {
    intervals,
    report_interval,
    reset_day,
    interfaces,
    disks,
    pings,
    report_gpu,
    report_errors,
    report_self,
    cf_correction,
    cf_batch,
  } = cfg;
  const next: Partial<RemoteConfig> = {};
  if (intervals !== undefined) {
    const keys: Array<keyof CollectionIntervals> = [
      "collect",
      "ping",
      "slow",
      "gpu",
      "ip",
      "diskio",
    ];
    if (
      typeof intervals !== "object" || intervals === null ||
      !keys.every((key) =>
        Number.isInteger((intervals as Record<string, unknown>)[key]) &&
        (intervals as Record<string, number>)[key] >= 1
      )
    ) {
      return json({ error: "intervals 六个周期都必须是 >=1 的整数" }, 400);
    }
    next.intervals = intervals as unknown as CollectionIntervals;
  }
  if (report_interval !== undefined) {
    if (!Number.isInteger(report_interval) || (report_interval as number) < 1) {
      return json({ error: "report_interval 必须为 >=1 的整数" }, 400);
    }
    next.report_interval = report_interval as number;
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
  if (disks !== undefined) {
    if (
      !Array.isArray(disks) ||
      !disks.every((x) =>
        typeof x === "string" && x.length > 0 && x.length <= 64
      )
    ) {
      return json(
        { error: "disks 必须为非空字符串数组（单个 <= 64 字符）" },
        400,
      );
    }
    next.disks = disks as string[];
  }
  if (pings !== undefined) {
    if (!Array.isArray(pings)) {
      return json({ error: "pings 格式非法" }, 400);
    }
    for (let index = 0; index < pings.length; index++) {
      const ping = pings[index];
      if (typeof ping !== "object" || ping === null) {
        return json({ error: `Ping 第 ${index + 1} 行格式非法` }, 400);
      }
      const p = ping as Record<string, unknown>;
      if (
        typeof p.name !== "string" || p.name.length === 0 ||
        !["http", "tcp", "icmp"].includes(String(p.type)) ||
        typeof p.target !== "string" || p.target.length === 0 ||
        (p.interval !== undefined &&
          (!Number.isInteger(p.interval) || Number(p.interval) < 1))
      ) {
        return json({ error: `Ping 第 ${index + 1} 行格式非法` }, 400);
      }
      const targetError = pingTargetError(String(p.type), p.target);
      if (targetError) {
        return json({ error: `Ping 第 ${index + 1} 行 ${targetError}` }, 400);
      }
    }
    next.pings = pings as PingTarget[];
  }
  if (report_gpu !== undefined) {
    if (typeof report_gpu !== "boolean") {
      return json({ error: "report_gpu 必须为布尔值" }, 400);
    }
    next.report_gpu = report_gpu;
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
  const cf: { correction?: boolean; batch?: boolean } = {};
  if (cf_correction !== undefined) {
    if (typeof cf_correction !== "boolean") {
      return json({ error: "cf_correction 必须为布尔值" }, 400);
    }
    cf.correction = cf_correction;
  }
  if (cf_batch !== undefined) {
    if (typeof cf_batch !== "boolean") {
      return json({ error: "cf_batch 必须为布尔值" }, 400);
    }
    cf.batch = cf_batch;
  }
  if (Object.keys(cf).length) next.ext = { cf };
  if (
    intervals === undefined && report_interval === undefined &&
    reset_day === undefined &&
    interfaces === undefined && disks === undefined && pings === undefined &&
    report_gpu === undefined &&
    report_errors === undefined && report_self === undefined &&
    cf_correction === undefined && cf_batch === undefined
  ) {
    return json({ error: "没有可下发的字段" }, 400);
  }
  const pending: Partial<RemoteConfig> = pendingConfig.get(instanceId) ?? {};
  const mergedExt = pending.ext || next.ext
    ? {
      cf: {
        ...(pending.ext?.cf ?? {}),
        ...(next.ext?.cf ?? {}),
      },
    }
    : undefined;
  const merged: RemoteConfig = {
    ...pending,
    ...next,
    ...(mergedExt ? { ext: mergedExt } : {}),
    config_version: newConfigVersion(),
  } as RemoteConfig;
  pendingConfig.set(instanceId, merged);
  console.log(
    `[config] ${state.serverId}/${state.reporterId} 待下发:`,
    merged,
  );
  return json({ ok: true, pending: merged });
}

// ---------- 视图数据 ----------

function serversView() {
  const now = Date.now();
  return [...servers.entries()].map(([instanceId, s]) => {
    const recent = s.dynamic.slice(-150);
    const asyncs = s.asyncs.slice(-300);
    const pings = asyncs.filter((a) => a.kind === "ping");
    return {
      instance_id: instanceId,
      reporter_id: s.reporterId,
      protocol: s.protocol,
      server_id: s.serverId,
      agent_version: s.agentVersion,
      online: now - s.lastSeen < 90_000,
      last_seen: s.lastSeen,
      config_version: s.configVersion,
      time: s.time,
      pending_config: pendingConfig.get(instanceId) ?? null,
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
    instance_id: r.instance_id,
    reporter_id: r.reporter_id,
    protocol: r.protocol,
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

Deno.serve({ hostname: HOST, port: PORT }, async (req) => {
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
    const instanceId = resolveInstanceId(nsMatch[1]);
    if (!instanceId) return json({ error: "Reporter 实例不存在" }, 404);
    needStatic.add(instanceId);
    return json({
      ok: true,
      note: "下次该 agent 上报时响应将带 next.static=true",
    });
  }
  const cfgMatch = url.pathname.match(/^\/api\/config\/([^/]+)$/);
  if (req.method === "POST" && cfgMatch) {
    try {
      const instanceId = resolveInstanceId(cfgMatch[1]);
      if (!instanceId) return json({ error: "Reporter 实例不存在" }, 404);
      return handleSetConfig(instanceId, await req.json());
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
  .cfg-tabs { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 12px; padding-bottom: 10px;
    border-bottom: 0.5px solid var(--sep); }
  .cfg-tab { appearance: none; border: 0.5px solid transparent; border-radius: 999px;
    background: var(--fill); color: var(--text2); font: inherit; font-size: 12px; padding: 5px 12px;
    cursor: pointer; }
  .cfg-tab:hover { color: var(--text); }
  .cfg-tab.on { color: #fff; background: var(--blue); border-color: var(--blue); }
  .cfg-tab .tag { margin-left: 6px; opacity: .75; font-size: 10px; }
  .cfg-tab-panel { padding-top: 2px; }
  .cfg-private { font-family: ui-monospace, "SF Mono", Menlo, monospace;
    letter-spacing: .08em; color: var(--text3); }
  .cfg-block + .cfg-block { margin-top: 22px; padding-top: 2px; border-top: 0.5px solid var(--sep); }
  .cfg-block-head { display: flex; align-items: baseline; gap: 10px; margin-top: 14px; }
  .cfg-block-head h2 { margin: 0; font-size: 14px; font-weight: 650; }
  .cfg-block-head span { color: var(--text3); font-size: 11.5px; }
  .cfg-section-head { display: flex; flex-wrap: wrap; align-items: baseline; gap: 6px 10px; margin: 16px 2px -2px; }
  .cfg-section-title { color: var(--text); font-size: 13px; font-weight: 650; }
  .cfg-section-note { color: var(--text3); font-size: 11.5px; }
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
  .cfg-item textarea { width: 100%; min-height: 76px; resize: vertical; background: var(--fill);
    border: 0.5px solid var(--sep); border-radius: 6px; color: var(--text); font: 11.5px ui-monospace;
    padding: 6px 8px; }
  .cfg-item input:focus, .cfg-item select:focus, .ping-row input:focus, .ping-row select:focus {
    outline: none; border-color: var(--blue); box-shadow: 0 0 0 3px rgba(10,132,255,.3); }
  .cfg-item input:disabled, .cfg-item select:disabled { opacity: .3; }
  .cfg-item input.wide { min-width: 0; }
  .topology .cfg-item .cur { max-width: 360px; }
  .cfg-pings { grid-column: 1 / -1; }
  .ping-toggle { grid-template-columns: minmax(0,1fr) auto; }
  .ping-head, .ping-row { display: grid;
    grid-template-columns: 84px minmax(110px,.55fr) minmax(180px,1fr) 82px 28px; gap: 6px;
    align-items: center; }
  .ping-head { margin-top: 8px; color: var(--text3); font-size: 11px; padding: 0 1px; }
  .ping-row {
    margin-top: 6px; align-items: center; }
  .ping-row input, .ping-row select { background: var(--fill); border: 0.5px solid var(--sep); border-radius: 6px;
    color: var(--text); font: inherit; font-size: 12.5px; padding: 4px 8px; min-width: 0; }
  .ping-row input:disabled, .ping-row select:disabled { opacity: .3; }
  .ping-empty { color: var(--text3); font-size: 12px; padding: 10px 0 2px; }
  .ping-del { background: none; border: none; color: var(--text3); cursor: pointer;
    font-size: 13px; padding: 0; text-align: center; }
  .ping-del:hover { color: var(--red); }
  .ping-del:disabled, .ping-add:disabled { cursor: default; opacity: .3; }
  .ping-add { margin-top: 8px; }
  @media (max-width: 680px) {
    .ping-head { display: none; }
    .ping-row { grid-template-columns: 78px minmax(90px,1fr) 74px 28px; }
    .ping-row .ping-target { grid-column: 1 / -1; grid-row: 2; }
  }
  .cfg-foot { display: flex; align-items: center; gap: 12px; margin-top: 14px; }
  .cfg-status { color: var(--orange); font-size: 12px; }
  .cfg-hint { color: var(--text3); font-size: 11.5px; margin-left: auto; }
  /* 样例 */
  .note { color: var(--text2); font-size: 12px; margin-top: 10px; }
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
      <button data-view="cfview">协议预览</button>
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
var serverStatics = {};   // instance_id -> 最新 static
var serverLatest = {};    // instance_id -> serversView 条目（异步快照兜底用）
function cfMb(b) { return b == null ? null : Math.floor(b / 1048576); }
function cfRound2(v) { return v == null ? null : Math.round(v * 100) / 100; }
function cfLoad(l) { return l ? l.map(function (x) { return x.toFixed(2); }).join(' ') : null; }
/* 收集一条上报的最新值（报文自带 + 服务端快照兜底），CF/komari 换算共用 */
function gatherLatest(r) {
  var rep = r.report || {};
  var key = r.instance_id || r.server_id;
  var st = rep.static || serverStatics[key] || {};
  var view = serverLatest[key] || {};
  if (!st.agent_version && view.agent_version) st = Object.assign({}, st, { agent_version: view.agent_version });
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
  return { rep: rep, st: st, dyn: dyn, last: last, slow: slow, gpus: gpus, pings: pings, dio: dio };
}

function cfBody(r) {
  var g = gatherLatest(r);
  var rep = g.rep, st = g.st, last = g.last, slow = g.slow, gpus = g.gpus, pings = g.pings, dio = g.dio;
  var dyn = g.dyn;
  var tm = rep.time || {};
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
  m.agent_version = (st.agent_version || '?') + '_probe-rs';
  m.timestamp = tm.accurate_ts == null ? r.received_at : tm.accurate_ts;
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
      return { ts: d.accurate_ts == null ? d.ts : d.accurate_ts, metrics: sm };
    });
  }
  var out = { id: r.server_id, secret: '<API_SECRET>', metrics: m };
  if (samples.length) out.samples = samples;
  return out;
}

/* komari 协议换算（镜像 reporter_komari.rs）：JSON-RPC 2.0 通知帧 */
function komariFrames(r) {
  var g = gatherLatest(r);
  var rep = g.rep, st = g.st, last = g.last, slow = g.slow, gpus = g.gpus;
  var tm = rep.time || {};
  var reportNow = tm.accurate_ts == null ? r.received_at : tm.accurate_ts;
  var reportBoot = st.boot_time || 0;
  var network = {};
  if (last.net_tx_speed != null) network.up = last.net_tx_speed;
  if (last.net_rx_speed != null) network.down = last.net_rx_speed;
  if (last.net_tx != null) network.totalUp = last.net_tx;
  if (last.net_rx != null) network.totalDown = last.net_rx;
  var report = {
    cpu: { usage: cfRound2(last.cpu_usage) || 0 },
    ram: { total: st.mem_total || 0, used: last.mem_used || 0 },
    swap: { total: st.swap_total || 0, used: last.swap_used || 0 },
    disk: { total: st.disk_total || 0, used: (slow && slow.disk_used) || 0 },
    network: network,
    uptime: reportBoot ? Math.max(0, Math.floor((reportNow - reportBoot) / 1000)) : 0,
    process: (slow && slow.processes) || 0,
    message: (rep.errors || []).map(function (e) { return '[' + e.source + '] ' + e.msg; }).join('; '),
  };
  if (last.load) report.load = { load1: last.load[0], load5: last.load[1], load15: last.load[2] };
  if (slow) report.connections = { tcp: slow.tcp_conn || 0, udp: slow.udp_conn || 0 };
  if (gpus.length) {
    var sum = 0, n = 0;
    gpus.forEach(function (x) { if (x.usage != null) { sum += x.usage; n++; } });
    report.gpu = {
      count: gpus.length,
      average_usage: n ? sum / n : 0,
      detailed_info: gpus.map(function (x) {
        return {
          name: x.name,
          memory_total: x.mem_total || 0,
          memory_used: x.mem_used || 0,
          utilization: x.usage || 0,
          temperature: x.temp || 0,
        };
      }),
    };
  }
  var info = {
    cpu_name: st.cpu_name || '', cpu_cores: st.cpu_cores || 0,
    cpu_physical_cores: st.cpu_physical_cores || 0, arch: st.arch || '',
    os: st.os || '', kernel_version: st.kernel || '',
    ipv4: st.ipv4 || '', ipv6: st.ipv6 || '',
    mem_total: st.mem_total || 0, swap_total: st.swap_total || 0, disk_total: st.disk_total || 0,
    gpu_name: st.gpu_name || '', virtualization: st.virtualization || '',
    version: 'probe-rs_' + (st.agent_version || '?'),
  };
  return {
    report: { jsonrpc: '2.0', method: 'agent.report', params: { report: report } },
    basic_info: { jsonrpc: '2.0', method: 'agent.basicInfo', params: { info: info } },
  };
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
    + '&ping_mode=' + (cf.ping_mode || 'tcp')
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
function fmtDateTimeMs(ts) { if (ts == null) return '–'; var d = new Date(ts);
  return d.toLocaleString('zh-CN', { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0'); }
function fmtMsSpan(ms) { var n = Math.abs(Number(ms));
  if (n >= 60000) return (n / 60000).toFixed(2) + ' min';
  if (n >= 1000) return (n / 1000).toFixed(3) + ' s';
  return Math.round(n) + ' ms'; }
function fmtClockOffset(ms) { if (ms == null) return '等待首次校准';
  if (Math.abs(ms) <= 1) return '≤ 1 ms';
  return (ms > 0 ? '本地慢 ' : '本地快 ') + fmtMsSpan(ms); }
/* 配置版本展示：UTC+8 时间戳字符串缩短为 "08-06 15:30:45"，其他原样 */
function fmtVer(v) { if (v == null || v === '') return '–'; var s = String(v);
  return /^\\d{4}-\\d{2}-\\d{2}T/.test(s) ? s.slice(5, 19).replace('T', ' ') : s; }
function instanceKey(x) { return x.instance_id || x.server_id; }
function reporterTag(x) { return (x.reporter_id || 'primary') + ' / ' + (x.protocol || 'probe'); }
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
  document.getElementById('summary').textContent = '在线 Reporter ' + online + ' / ' + data.length;
  if (!data.length) {
    var e = el('div', 'empty');
    e.append(el('p', null, '暂无服务器数据'), el('code', null, 'probe-rs -c config.toml'));
    app.append(e); return;
  }
  data.forEach(function (s) {
    var st = s.static || {}, d = mergeLatest(s.recent || []);
    var tm = s.time || {};
    var slow = s.slow_latest || {};
    var card = el('div', 'card' + (s.online ? '' : ' offline'));
    var head = el('div', 'card-head');
    head.append(
      el('span', 'dot ' + (s.online ? 'on' : 'off')),
      el('span', 'name', s.server_id),
      el('span', 'badge', reporterTag(s)),
      el('span', 'meta', [st.os, st.arch, 'agent v' + (st.agent_version || s.agent_version || '?'), 'cfg ' + fmtVer(s.config_version)].filter(Boolean).join(' · ')));
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
    detailRow(tb, ['本地时间', fmtDateTimeMs(tm.local_ts), '准确时间', fmtDateTimeMs(tm.accurate_ts)]);
    detailRow(tb, ['时间偏差', fmtClockOffset(tm.offset_ms), '校准质量', tm.round_trip_ms == null
      ? '等待 NTP / 服务端时间' : (tm.source || '未知来源') + ' · RTT ' + tm.round_trip_ms + ' ms · 样本年龄 ' + fmtMsSpan(tm.sample_age_ms || 0)]);
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
    el('span', 'rpt-id', r.server_id + ' · ' + reporterTag(r)),
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
    el('div', 'd-head', '#' + seq + ' · ' + r.server_id + ' · ' + reporterTag(r) + ' · ' + new Date(r.received_at).toLocaleString('zh-CN')),
    el('pre', 'code', JSON.stringify(r.report, null, 2)));
}

function renderReports(list) {
  var app = document.getElementById('app');
  if (!document.getElementById('rpt-list')) {
    app.textContent = '';
    var bar = el('div', 'rpt-bar');
    bar.append(el('span', 'lab', 'Reporter 实例'));
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
    var key = instanceKey(r);
    Array.prototype.forEach.call(sel.options, function (o) { if (o.value === key) exists = true; });
    if (!exists) { var o = el('option', null, r.server_id + ' · ' + reporterTag(r)); o.value = key; sel.append(o); }
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
    if (rptFilter && instanceKey(r) !== rptFilter) return;
    var row = rptRow(r);
    row.classList.toggle('sel', seq === rptSelected);
    listEl.append(row);
    shown++;
  });
  if (!shown) listEl.append(el('div', 'empty', rptFilter ? '该 Reporter 暂无上报记录' : '暂无上报记录'));
  else if (rptSelected == null || !rptData[rptSelected]
      || (rptFilter && instanceKey(rptData[rptSelected]) !== rptFilter)) {
    selectReport(rptData[Number(listEl.firstChild.dataset.seq)].seq);
  }
}

/* ---- Reporter 配置编辑。全局采集周期与 Ping 定义只读，不能由上报端下发。 ---- */
var cfgFormActive = false;  // 编辑中暂停 WS 重渲染，避免表单被上报推送刷掉

function cfgGroup(title, rows) {
  var g = el('div', 'cfg-group');
  g.append(el('h3', null, title));
  rows.forEach(function (r) {
    var row = el('div', 'cfg-item plain');
    var value = el('span', 'cur', r[1]);
    if (r[2]) value.classList.add(r[2]);
    value.title = String(r[1]);
    row.append(el('span', 'lab', r[0]), value);
    g.append(row);
  });
  return g;
}

function cfgPingTargetError(kind, target) {
  if (target.trim() !== target) return 'target 首尾不能有空白';
  if (kind === 'http') {
    var url;
    try { url = new URL(target); } catch (_) { return 'HTTP target 必须是合法 URL'; }
    if (!['http:', 'https:'].includes(url.protocol) || !url.hostname)
      return 'HTTP target 只允许 http(s)://host[:port]';
    if (url.pathname !== '/' || url.search || url.hash)
      return 'target 不允许 path/query/fragment';
  } else {
    for (var i = 0; i < target.length; i++) {
      if ([47, 92, 63, 35].includes(target.charCodeAt(i)))
        return 'target 不允许 path/query/fragment';
    }
  }
  return null;
}

function cfgEditForm(s, st) {
  var cf = st.config || {};          // static.config.*：当前生效配置
  var iv = cf.intervals || {};
  var wrap = el('div');
  var items = [];
  function section(title, note) {
    var root = el('section', 'cfg-section');
    var head = el('div', 'cfg-section-head');
    head.append(el('span', 'cfg-section-title', title), el('span', 'cfg-section-note', note));
    var sectionGroups = el('div', 'cfg-groups');
    root.append(head, sectionGroups);
    wrap.append(root);
    return { root: root, groups: sectionGroups };
  }
  var readSection = section('当前本地配置', '连接身份只读；敏感字段不会由 Agent 上报');
  var editSection = section('可下发配置', '勾选字段后，仅更新当前 probe Reporter');
  function group(sectionGroups, title) {
    var g = el('div', 'cfg-group');
    g.append(el('h3', null, title));
    sectionGroups.append(g);
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
    var cur = el('span', 'cur', curText); cur.title = String(curText);
    row.append(el('span', 'lab', label), cur);
    g.append(row);
  }
  function numInput(cur, min, max) {
    var inp = el('input'); inp.type = 'number'; inp.min = String(min);
    if (max != null) inp.max = String(max);
    if (cur != null) inp.value = cur;
    return inp;
  }
  function sec(v) { return v != null ? v + ' s' : '–'; }

  var gIdentity = group(readSection.groups, '[[reporters]]');
  plainRow(gIdentity, 'id', s.reporter_id || 'primary');
  plainRow(gIdentity, 'protocol', s.protocol || 'probe');
  plainRow(gIdentity, '状态', s.online ? '当前接入 · 在线 · 可下发' : '当前接入 · 离线');

  var gConnection = group(readSection.groups, '[reporters.probe] · 连接');
  plainRow(gConnection, 'server_id', s.server_id);
  plainRow(gConnection, 'secret', '******');
  plainRow(gConnection, 'worker_url', location.origin + '/report');
  plainRow(gConnection, 'config_version', fmtVer(s.config_version));

  var gDemand = group(editSection.groups, '[reporters.probe.intervals]');
  [['collect', '同步采样 collect'], ['ping', 'Ping 默认 ping'], ['slow', '慢指标 slow'],
   ['gpu', 'GPU gpu'], ['ip', '公网 IP ip'], ['diskio', '磁盘 IO diskio']]
    .forEach(function (def) {
      editRow(gDemand, def[1], sec(iv[def[0]]), numInput(iv[def[0]], 1), function (p, inp) {
        var v = Number(inp.value);
        if (!Number.isInteger(v) || v < 1) throw def[0] + ' 必须 >= 1';
        if (!p.intervals) p.intervals = {
          collect: Number(iv.collect), ping: Number(iv.ping), slow: Number(iv.slow),
          gpu: Number(iv.gpu), ip: Number(iv.ip), diskio: Number(iv.diskio)
        };
        p.intervals[def[0]] = v;
      });
    });
  var gReport = group(editSection.groups, '[reporters.probe] · 上报策略');
  editRow(gReport, '上报间隔 report_interval', sec(iv.report), numInput(iv.report, 1),
    function (p, inp) {
      var v = Number(inp.value);
      if (!Number.isInteger(v) || v < 1) throw 'report_interval 必须 >= 1';
      p.report_interval = v;
    });
  editRow(gReport, '流量重置日 reset_day',
    cf.reset_day != null ? (cf.reset_day === 0 ? '不重置' : '每月 ' + cf.reset_day + ' 号') : '–',
    numInput(cf.reset_day, 0, 31),
    function (p, inp) {
      var v = Number(inp.value);
      if (!Number.isInteger(v) || v < 0 || v > 31) throw 'reset_day 必须在 0-31';
      p.reset_day = v;
    });

  var gBool = group(editSection.groups, '[reporters.probe] · 输出开关');
  [['report_gpu', 'enable_gpu', 'GPU 输出 report_gpu'],
   ['report_errors', 'report_errors', '错误上报 report_errors'],
   ['report_self', 'report_self', '自身占用 report_self']].forEach(function (def) {
    var sel = el('select');
    [['true', '开'], ['false', '关']].forEach(function (o) {
      var opt = el('option', null, o[1]); opt.value = o[0]; sel.append(opt);
    });
    if (cf[def[1]] != null) sel.value = String(cf[def[1]]);
    var cur = cf[def[1]] == null ? '–' : (cf[def[1]] ? '开' : '关');
    editRow(gBool, def[2], cur, sel, function (p, inp) { p[def[0]] = inp.value === 'true'; });
  });

  if ((s.protocol || 'probe') === 'cf') {
    var cfExt = ((cf.ext || {}).cf) || {};
    var gCf = group(editSection.groups, 'CF 扩展 ext.cf');
    [['cf_correction', '流量校正 correction', cfExt.correction],
     ['cf_batch', '批量 samples batch', cfExt.batch]].forEach(function (def) {
      var sel = el('select');
      [['true', '开'], ['false', '关']].forEach(function (o) {
        var opt = el('option', null, o[1]); opt.value = o[0]; sel.append(opt);
      });
      if (def[2] != null) sel.value = String(def[2]);
      var cur = def[2] == null ? '–' : (def[2] ? '开' : '关');
      editRow(gCf, def[1], cur, sel, function (p, inp) { p[def[0]] = inp.value === 'true'; });
    });
  }

  var gIface = group(editSection.groups, '[reporters.probe] · interfaces');
  var ifInp = el('input'); ifInp.type = 'text'; ifInp.className = 'wide';
  ifInp.placeholder = 'eth*, ens*（留空 = 清空）';
  if (Array.isArray(cf.interfaces) && cf.interfaces.length) ifInp.value = cf.interfaces.join(', ');
  var ifCur = Array.isArray(cf.interfaces)
    ? (cf.interfaces.length ? cf.interfaces.join(', ') : '（空）') : '–';
  editRow(gIface, '白名单（glob，逗号分隔）', ifCur, ifInp, function (p, inp) {
    p.interfaces = inp.value.split(',').map(function (x) { return x.trim(); }).filter(Boolean);
  });

  var gDisk = group(editSection.groups, '[reporters.probe] · disks');
  var diskInp = el('input'); diskInp.type = 'text'; diskInp.className = 'wide';
  diskInp.placeholder = 'C:*, 0 C:, nvme*（留空 = 全部）';
  if (Array.isArray(cf.disks) && cf.disks.length) diskInp.value = cf.disks.join(', ');
  var diskCur = Array.isArray(cf.disks) ? (cf.disks.join(', ') || '（全部）') : '–';
  editRow(gDisk, '卷/物理盘 glob', diskCur, diskInp, function (p, inp) {
    p.disks = inp.value.split(',').map(function (x) { return x.trim(); }).filter(Boolean);
  });

  var gPing = group(editSection.groups, '[[reporters.probe.pings]]');
  gPing.classList.add('cfg-pings');
  var pingToggle = el('div', 'cfg-item ping-toggle');
  var pingLab = el('label', 'lab');
  var pingCb = el('input'); pingCb.type = 'checkbox';
  var pingCur = el('span', 'cur');
  pingLab.append(pingCb, el('span', null, '更新整组 Ping'));
  pingToggle.append(pingLab, pingCur);
  gPing.append(pingToggle);

  var pingEditor = el('div');
  var pingHead = el('div', 'ping-head');
  ['type', 'name', 'target', 'interval', ''].forEach(function (label) {
    pingHead.append(el('span', null, label));
  });
  var pingRows = el('div');
  var pingEmpty = el('div', 'ping-empty', '暂无 Ping；勾选更新后可添加');
  var pingAdd = el('button', 'btn ping-add', '＋ 添加 Ping'); pingAdd.type = 'button';
  pingEditor.append(pingHead, pingRows, pingEmpty, pingAdd);
  gPing.append(pingEditor);

  function setPingEnabled(enabled) {
    pingToggle.classList.toggle('on', enabled);
    pingRows.querySelectorAll('input, select, button').forEach(function (control) {
      control.disabled = !enabled;
    });
    pingAdd.disabled = !enabled;
  }
  function updatePingState() {
    var count = pingRows.children.length;
    pingCur.textContent = count + ' 项';
    pingEmpty.style.display = count ? 'none' : '';
  }
  function addPingRow(value) {
    value = value || {};
    var row = el('div', 'ping-row');
    var type = el('select'); type.setAttribute('aria-label', 'Ping 类型');
    ['http', 'tcp', 'icmp'].forEach(function (kind) {
      var opt = el('option', null, kind); opt.value = kind; type.append(opt);
    });
    type.value = ['http', 'tcp', 'icmp'].includes(value.type) ? value.type : 'tcp';
    var name = el('input'); name.type = 'text'; name.placeholder = 'name';
    name.setAttribute('aria-label', 'Ping 名称'); name.value = value.name || '';
    var target = el('input', 'ping-target'); target.type = 'text'; target.placeholder = 'host[:port] 或 http(s)://host（无 path）';
    target.setAttribute('aria-label', 'Ping 目标'); target.value = value.target || '';
    var interval = numInput(value.interval != null ? value.interval : (iv.ping || 1), 1);
    interval.setAttribute('aria-label', 'Ping 间隔（秒）');
    var del = el('button', 'ping-del', '×'); del.type = 'button'; del.title = '删除此 Ping';
    del.onclick = function () { row.remove(); updatePingState(); };
    row.append(type, name, target, interval, del);
    row.pingControls = { type: type, name: name, target: target, interval: interval };
    pingRows.append(row);
    setPingEnabled(pingCb.checked);
    updatePingState();
    return name;
  }
  (Array.isArray(cf.pings) ? cf.pings : []).forEach(addPingRow);
  updatePingState();
  setPingEnabled(false);
  pingCb.onchange = function () {
    setPingEnabled(pingCb.checked);
    if (pingCb.checked) {
      var first = pingRows.querySelector('input');
      (first || pingAdd).focus();
    }
  };
  pingAdd.onclick = function () { addPingRow({}).focus(); };
  items.push({ cb: pingCb, apply: function (payload) {
    var names = new Set();
    payload.pings = Array.from(pingRows.children).map(function (row, index) {
      var controls = row.pingControls;
      var type = controls.type.value;
      var name = controls.name.value.trim();
      var target = controls.target.value.trim();
      var interval = Number(controls.interval.value);
      var line = 'Ping 第 ' + (index + 1) + ' 行';
      if (!['http', 'tcp', 'icmp'].includes(type)) throw line + ' type 非法';
      if (!name) throw line + ' name 不能为空';
      if (!target) throw line + ' target 不能为空';
      var targetError = cfgPingTargetError(type, target);
      if (targetError) throw line + ' ' + targetError;
      if (!Number.isInteger(interval) || interval < 1) throw line + ' interval 必须 >= 1';
      if (names.has(name)) throw 'Ping name 重复：' + name;
      names.add(name);
      return { type: type, name: name, target: target, interval: interval };
    });
  } });

  var foot = el('div', 'cfg-foot');
  var btn = el('button', 'btn primary', '下发配置'); btn.type = 'button';
  var status = el('span', 'cfg-status');
  foot.append(btn, status, el('span', 'cfg-hint', '勾选「更新」的项才会下发，随下次上报便车生效'));
  editSection.root.append(foot);
  wrap.addEventListener('focusin', function () { cfgFormActive = true; });

  btn.onclick = function () {
    var payload = {};
    try {
      items.forEach(function (it) { if (it.cb.checked) it.apply(payload); });
    } catch (e) { status.textContent = String(e.message || e); return; }
    if (!Object.keys(payload).length) { status.textContent = '没有勾选任何更新项'; return; }
    btn.disabled = true;
    fetch('/api/config/' + encodeURIComponent(instanceKey(s)), {
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

/* ---- 配置视图：本地配置 / Reporter / 采集映射三层 ---- */
var cfgReporterSelection = {};
var cfgCollectionSelection = {};

function cfgRouteKey(route) {
  return JSON.stringify([route.id || 'primary', route.protocol || 'probe']);
}

function cfgRoutes(s) {
  var config = ((s.static || {}).config) || {};
  if (Array.isArray(config.reporters) && config.reporters.length) return config.reporters;
  return [{
    id: s.reporter_id || 'primary',
    protocol: s.protocol || 'probe',
    intervals: config.intervals || {},
    report_interval: (config.intervals || {}).report,
    reset_day: config.reset_day,
    interfaces: config.interfaces || [],
    disks: config.disks || [],
    report_gpu: config.enable_gpu,
    report_errors: config.report_errors,
    report_self: config.report_self,
    pings: Array.isArray(config.pings) ? config.pings : [],
  }];
}

function cfgIsCurrent(s, route) {
  return (route.id || 'primary') === (s.reporter_id || 'primary') &&
    (route.protocol || 'probe') === (s.protocol || 'probe');
}

function cfgTabs(defs, active, onSelect) {
  var tabs = el('div', 'cfg-tabs');
  defs.forEach(function (def) {
    var button = el('button', 'cfg-tab' + (def.key === active ? ' on' : ''));
    button.type = 'button';
    button.append(el('span', null, def.label));
    if (def.tag) button.append(el('span', 'tag', def.tag));
    button.onclick = function () { onSelect(def.key); };
    tabs.append(button);
  });
  return tabs;
}

function cfgBlock(title, note) {
  var root = el('section', 'cfg-block');
  var head = el('div', 'cfg-block-head');
  head.append(el('h2', null, title));
  if (note) head.append(el('span', null, note));
  root.append(head);
  return root;
}

function cfgBool(value) {
  return value == null ? '–' : (value ? 'true' : 'false');
}

function cfgList(value, emptyText) {
  return Array.isArray(value) && value.length ? value.join(', ') : emptyText;
}

function cfgMachineForm(s, st) {
  var block = cfgBlock('本地配置', '按 config.toml 顶层结构展示');
  var groups = el('div', 'cfg-groups');
  groups.append(
    cfgGroup('[root]', [
      ['schema', '1'],
      ['data_dir', '******', 'cfg-private'],
    ]),
    cfgGroup('[auto_update]', [
      ['enabled', '未在上报摘要中公开'],
      ['repository', '未在上报摘要中公开'],
      ['channel', '未在上报摘要中公开'],
      ['check_interval', '未在上报摘要中公开'],
      ['proxys', '未在上报摘要中公开'],
    ]),
    cfgGroup('运行回执', [
      ['agent_version', st.agent_version || s.agent_version || '–'],
      ['static.ts', st.ts ? new Date(st.ts).toLocaleString('zh-CN') : '–'],
      ['当前接入', (s.reporter_id || 'primary') + ' / ' + (s.protocol || 'probe')],
    ]),
  );
  block.append(groups, el('div', 'note',
    '页面使用 Agent 上报的脱敏配置摘要；本地路径、连接凭据及其他线路的连接身份不会离开 Agent。'));
  return block;
}

function cfgReadonlyReporterForm(route) {
  var protocol = route.protocol || 'probe';
  var intervals = route.intervals || {};
  var pings = Array.isArray(route.pings) ? route.pings : [];
  var wrap = el('div', 'cfg-tab-panel');
  var groups = el('div', 'cfg-groups');
  groups.append(cfgGroup('[[reporters]]', [
    ['id', route.id || 'primary'],
    ['protocol', protocol],
    ['权限', '外部线路 · 只读'],
  ]));

  if (protocol === 'cf') {
    var pingByName = {};
    pings.forEach(function (ping) { pingByName[ping.name] = ping.target; });
    groups.append(
      cfgGroup('[reporters.cf] · 连接', [
        ['server_id', '******', 'cfg-private'],
        ['secret', '******', 'cfg-private'],
        ['url', '******', 'cfg-private'],
      ]),
      cfgGroup('[reporters.cf] · 采集与上报', [
        ['connection_mode', route.connection_mode || 'auto'],
        ['ping_mode', route.ping_mode || 'tcp'],
        ['interval', route.report_interval != null ? route.report_interval + ' s' : '–'],
        ['wss_report_interval', route.wss_report_interval != null ? route.wss_report_interval + ' s' : '–'],
        ['collect_interval', route.source_collect_interval != null ? route.source_collect_interval + ' s' : '–'],
        ['映射后实际采集', intervals.collect != null ? intervals.collect + ' s' : '–'],
        ['reset_day', route.reset_day != null ? String(route.reset_day) : '–'],
        ['interface', cfgList(route.interfaces, '""（全部）')],
        ['report_gpu', cfgBool(route.report_gpu) + '（固定）'],
      ]),
      cfgGroup('[reporters.cf] · Ping', [
        ['ct', pingByName.ct || '–'],
        ['cu', pingByName.cu || '–'],
        ['cm', pingByName.cm || '–'],
        ['bd', pingByName.bd || pingByName.bgp || '–'],
      ]),
    );
  } else if (protocol === 'komari') {
    groups.append(
      cfgGroup('[reporters.komari] · 连接', [
        ['endpoint', '******', 'cfg-private'],
        ['token', '******', 'cfg-private'],
      ]),
      cfgGroup('[reporters.komari] · 采集与上报', [
        ['interval', route.report_interval != null ? route.report_interval + ' s' : '–'],
        ['month_rotate', route.reset_day != null ? String(route.reset_day) : '–'],
        ['enable_gpu', cfgBool(route.report_gpu)],
        ['include_nics', cfgList(route.interfaces, '""（全部）')],
        ['include_mountpoints', cfgList(route.disks, '""（全部）')],
      ]),
    );
  } else {
    groups.append(
      cfgGroup('[reporters.probe] · 连接', [
        ['server_id', '******', 'cfg-private'],
        ['secret', '******', 'cfg-private'],
        ['worker_url', '******', 'cfg-private'],
      ]),
      cfgGroup('[reporters.probe]', [
        ['report_interval', route.report_interval != null ? route.report_interval + ' s' : '–'],
        ['reset_day', route.reset_day != null ? String(route.reset_day) : '–'],
        ['report_gpu', cfgBool(route.report_gpu)],
        ['report_errors', cfgBool(route.report_errors)],
        ['report_self', cfgBool(route.report_self)],
        ['interfaces', cfgList(route.interfaces, '[]（全部）')],
        ['disks', cfgList(route.disks, '[]（全部）')],
      ]),
      cfgGroup('[reporters.probe.intervals]', [
        ['collect', intervals.collect != null ? intervals.collect + ' s' : '–'],
        ['ping', intervals.ping != null ? intervals.ping + ' s' : '–'],
        ['slow', intervals.slow != null ? intervals.slow + ' s' : '–'],
        ['gpu', intervals.gpu != null ? intervals.gpu + ' s' : '–'],
        ['ip', intervals.ip != null ? intervals.ip + ' s' : '–'],
        ['diskio', intervals.diskio != null ? intervals.diskio + ' s' : '–'],
      ]),
    );
  }
  var pingRows = pings.map(function (ping, index) {
    return [
      (ping.name || 'ping-' + (index + 1)) + ' · ' + (ping.type || '–'),
      ping.target + ' @ ' + (ping.interval ?? intervals.ping ?? '–') + ' s',
    ];
  });
  if (protocol !== 'cf') {
    groups.append(cfgGroup(
      protocol === 'komari'
        ? '[[reporters.komari.ext.learned_pings]]'
        : '[[reporters.probe.pings]]',
      pingRows.length ? pingRows : [['pings', '[]']],
    ));
  }
  wrap.append(groups, el('div', 'note',
    '该 Reporter 未连接到本 demo；可查看脱敏映射，但连接信息与配置下发均不可用。'));
  return wrap;
}

function cfgPendingView(s) {
  if (!s.pending_config) return null;
  var pending = s.pending_config;
  var rows = [['config_version', fmtVer(s.config_version) + ' → ' + fmtVer(pending.config_version)]];
  if (pending.intervals) rows.push(['intervals', Object.keys(pending.intervals).map(function (key) {
    return key + '=' + pending.intervals[key] + 's';
  }).join(', ')]);
  if (pending.report_interval != null) rows.push(['report_interval', pending.report_interval + 's']);
  if (pending.reset_day != null) rows.push(['reset_day', String(pending.reset_day)]);
  if (pending.interfaces) rows.push(['interfaces', pending.interfaces.join(', ') || '(空)']);
  if (pending.disks) rows.push(['disks', pending.disks.join(', ') || '(全部)']);
  if (pending.pings) rows.push(['pings', pending.pings.length + ' 项']);
  if (pending.report_gpu != null) rows.push(['report_gpu', String(pending.report_gpu)]);
  if (pending.report_errors != null) rows.push(['report_errors', String(pending.report_errors)]);
  if (pending.report_self != null) rows.push(['report_self', String(pending.report_self)]);
  var groups = el('div', 'cfg-groups');
  groups.append(cfgGroup('⚠ 待下发（下次上报生效）', rows));
  return groups;
}

function cfgCollectionPanel(config, route) {
  var panel = el('div', 'cfg-tab-panel');
  var groups = el('div', 'cfg-groups');
  var isGlobal = !route;
  var source = isGlobal ? (config.global || {}) : route;
  var intervals = source.intervals || {};
  groups.append(cfgGroup(isGlobal ? '实际采集周期' : '映射后采集周期', [
    ['collect', intervals.collect != null ? intervals.collect + ' s' : '–'],
    ['ping', intervals.ping != null ? intervals.ping + ' s' : '–'],
    ['slow', intervals.slow != null ? intervals.slow + ' s' : '–'],
    ['gpu', intervals.gpu != null ? intervals.gpu + ' s' : '–'],
    ['ip', intervals.ip != null ? intervals.ip + ' s' : '–'],
    ['diskio', intervals.diskio != null ? intervals.diskio + ' s' : '–'],
  ]));
  var interfaces = isGlobal && source.all_interfaces
    ? '全部接口'
    : cfgList(source.interfaces, isGlobal ? '–' : '默认出口过滤');
  var disks = isGlobal && source.all_disks
    ? '全部卷 / 物理盘'
    : cfgList(source.disks, isGlobal ? '–' : '全部卷 / 物理盘');
  groups.append(cfgGroup(isGlobal ? '实际采集范围' : 'Reporter 采集范围', [
    ['interfaces', interfaces],
    ['disks', disks],
    ['enable_gpu', cfgBool(isGlobal ? source.enable_gpu : source.report_gpu)],
  ]));
  var pings = Array.isArray(source.pings) ? source.pings : [];
  var pingRows = pings.map(function (ping, index) {
    var label = isGlobal ? 'worker ' + (index + 1) : (ping.name || 'ping-' + (index + 1));
    var type = ping.type ? ' · ' + ping.type : '';
    return [label + type, ping.target + ' @ ' + (ping.interval ?? intervals.ping ?? '–') + ' s'];
  });
  groups.append(cfgGroup(isGlobal ? '实际 Ping workers' : 'Reporter Ping 映射',
    pingRows.length ? pingRows : [['pings', '[]']]));
  panel.append(groups);
  return panel;
}

function renderConfig(data) {
  var app = document.getElementById('app');
  app.textContent = '';
  var configured = data.reduce(function (count, server) {
    return count + cfgRoutes(server).length;
  }, 0);
  document.getElementById('summary').textContent = data.length + ' 路接入 · ' + configured + ' 个 Reporter';
  if (!data.length) { app.append(el('div', 'empty', '暂无服务器数据')); return; }

  data.forEach(function (s) {
    var st = s.static || {};
    var config = st.config || {};
    var routes = cfgRoutes(s);
    var card = el('div', 'card' + (s.online ? '' : ' offline'));
    var head = el('div', 'card-head');
    head.append(
      el('span', 'dot ' + (s.online ? 'on' : 'off')),
      el('span', 'name', s.server_id),
      el('span', 'badge', reporterTag(s)),
      el('span', 'meta', 'agent v' + (st.agent_version || s.agent_version || '?')),
      el('span', 'meta', 'cfg ' + fmtVer(s.config_version)));
    if (s.pending_config) head.append(el('span', 'badge', '待下发 ' + fmtVer(s.pending_config.config_version)));
    var spacer = el('span', 'spacer');
    var refreshBtn = el('button', 'btn', '↻ 刷新 static'); refreshBtn.type = 'button';
    refreshBtn.onclick = function () {
      fetch('/api/need-static/' + encodeURIComponent(instanceKey(s)), { method: 'POST' });
      refreshBtn.textContent = '已安排 ✓';
      setTimeout(function () { refreshBtn.textContent = '↻ 刷新 static'; }, 3000);
    };
    head.append(spacer, refreshBtn);
    card.append(head, cfgMachineForm(s, st));

    var stateKey = instanceKey(s);
    var reporterBlock = cfgBlock('Reporter 配置', '每个 [[reporters]] 对应一个 Tab');
    var reporterDefs = routes.map(function (route) {
      var current = cfgIsCurrent(s, route);
      return {
        key: cfgRouteKey(route),
        label: route.id || 'primary',
        tag: (route.protocol || 'probe') + (current ? ' · 当前' : ' · 只读'),
      };
    });
    var currentRoute = routes.find(function (route) { return cfgIsCurrent(s, route); });
    var reporterActive = cfgReporterSelection[stateKey];
    if (!reporterDefs.some(function (def) { return def.key === reporterActive; })) {
      reporterActive = cfgRouteKey(currentRoute || routes[0]);
      cfgReporterSelection[stateKey] = reporterActive;
    }
    reporterBlock.append(cfgTabs(reporterDefs, reporterActive, function (key) {
      cfgReporterSelection[stateKey] = key;
      cfgFormActive = false;
      renderConfig(data);
    }));
    var selectedRoute = routes.find(function (route) { return cfgRouteKey(route) === reporterActive; });
    if (selectedRoute && cfgIsCurrent(s, selectedRoute) && (selectedRoute.protocol || 'probe') === 'probe') {
      var pending = cfgPendingView(s);
      if (pending) reporterBlock.append(pending);
      reporterBlock.append(cfgEditForm(s, st));
    } else if (selectedRoute) {
      reporterBlock.append(cfgReadonlyReporterForm(selectedRoute));
    }
    card.append(reporterBlock);

    var collectionBlock = cfgBlock('采集配置', '全局实际采集与各 Reporter 映射结果');
    var collectionDefs = [{ key: 'global', label: '全局实际采集', tag: 'effective' }].concat(
      routes.map(function (route) {
        return { key: cfgRouteKey(route), label: route.id || 'primary', tag: route.protocol || 'probe' };
      }),
    );
    var collectionActive = cfgCollectionSelection[stateKey] || 'global';
    if (!collectionDefs.some(function (def) { return def.key === collectionActive; })) collectionActive = 'global';
    cfgCollectionSelection[stateKey] = collectionActive;
    collectionBlock.append(cfgTabs(collectionDefs, collectionActive, function (key) {
      cfgCollectionSelection[stateKey] = key;
      cfgFormActive = false;
      renderConfig(data);
    }));
    var collectionRoute = collectionActive === 'global'
      ? null
      : routes.find(function (route) { return cfgRouteKey(route) === collectionActive; });
    collectionBlock.append(cfgCollectionPanel(config, collectionRoute));
    card.append(collectionBlock);
    app.append(card);
  });
}

/* ---- 协议预览视图：每台机器按子 tab 展示 probe / CF / komari 报文 ----
   servers 可传 WS 推送的最新视图（避免每次推送重复拉取） */
var protoSub = 'cf';   // 协议预览的子 tab：probe | cf | komari
function renderCfView(servers) {
  var reportsP = fetch('/api/reports').then(function (r) { return r.json(); });
  var serversP = servers
    ? Promise.resolve(servers)
    : fetch('/api/servers').then(function (r) { return r.json(); });
  Promise.all([serversP, reportsP]).then(function (res) {
    var servers = res[0], reports = res[1];
    var app = document.getElementById('app');
    app.textContent = '';
    document.getElementById('summary').textContent = '三种协议视角（CF/komari 由 probe 报文实时换算）';

    /* 子 tab：probe / CF / komari */
    var sub = el('div', 'seg');
    sub.style.marginBottom = '14px';
    [['probe', 'probe（原生）'], ['cf', 'CF /update'], ['komari', 'komari WS']].forEach(function (def) {
      var b = el('button', null, def[1]);
      b.type = 'button';
      b.dataset.sub = def[0];
      b.classList.toggle('on', protoSub === def[0]);
      b.onclick = function () { protoSub = def[0]; renderCfView(servers); };
      sub.append(b);
    });
    app.append(sub);

    servers.forEach(function (s) {
      if (s.static) serverStatics[instanceKey(s)] = s.static;
      serverLatest[instanceKey(s)] = s;
      var card = el('div', 'card' + (s.online ? '' : ' offline'));
      var head = el('div', 'card-head');
      var endpoint = { probe: 'POST /report', cf: 'POST /update', komari: 'WS v2 JSON-RPC' }[protoSub];
      head.append(
        el('span', 'dot ' + (s.online ? 'on' : 'off')),
        el('span', 'name', s.server_id),
        el('span', 'badge', reporterTag(s)),
        el('span', 'meta', endpoint + ' 预览'));
      card.append(head);

      /* 该机器最近一条上报 */
      var latest = reports.find(function (r) { return instanceKey(r) === instanceKey(s); });
      var g1 = el('div', 'cfg-groups');
      var b1 = el('div', 'cfg-group');
      var title = protoSub === 'probe' ? '上报报文' : protoSub === 'cf' ? '请求体' : 'WS 帧（basicInfo + report）';
      b1.append(el('h3', null, title + '（最近一条上报换算' + (latest ? '，#' + latest.seq : '') + '）'));
      if (!latest) {
        b1.append(el('pre', 'code', '（暂无上报记录）'));
      } else if (protoSub === 'probe') {
        b1.append(el('pre', 'code', JSON.stringify(latest.report, null, 2)));
      } else if (protoSub === 'cf') {
        b1.append(el('pre', 'code', JSON.stringify(cfBody(latest), null, 2)));
      } else {
        var kf = komariFrames(latest);
        b1.append(el('pre', 'code',
          JSON.stringify(kf.basic_info, null, 2) + String.fromCharCode(10) + String.fromCharCode(10)
          + JSON.stringify(kf.report, null, 2)));
        b1.append(el('div', 'note', 'komari 无 ts/批量/配置下发语义：只报最新值；下行方法（exec/terminal 等）一律不实现。'));
      }
      g1.append(b1);
      card.append(g1);

      /* CF 子 tab 额外展示配置下发 */
      if (protoSub === 'cf') {
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
        card.append(g2);
      }
      app.append(card);
    });
    if (!servers.length) app.append(el('div', 'empty', '暂无服务器数据'));
  }).catch(function () {});
}

var EXAMPLE_JSON5 = \`{
  "server_id": "srv-01",                    // 服务器 ID（本地配置，远端不可改）
  "config_version": "2026-08-06T15:30:45.123+08:00",   // 当前配置版本（UTC+8 可读时间戳字符串），服务端按「不等」判断是否下发新配置
  "time": {                                  // 原生服务端时间校准；offset = accurate - local
    "local_ts": 1754300060123,               // 本机墙钟
    "accurate_ts": 1754300060000,            // 单调时钟推算的准确时间；首次校准前为 null
    "offset_ms": -123,                       // 负数 = 本机快，正数 = 本机慢
    "source": "ntp:time.cloudflare.com",     // NTP 优先；UDP/123 不通时为 server
    "round_trip_ms": 18,                     // 最近一次校准请求 RTT
    "sample_age_ms": 29982                   // 最近校准样本到本次上报的年龄
  },

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
    "disks": [                              // 当前 Reporter 选中的逐卷容量
      { "id": "/dev/sda1", "name": "sda1", "mount_point": "/", "file_system": "ext4", "total": 107374182400, "used": 53687091200 }
    ],
    "gpu_name": "NVIDIA A100 80GB",         // 可选；无 GPU 为 null
    "virtualization": "kvm",                // 可选；物理机为 null
    "boot_time": 1754300000000,             // ms 时间戳
    "ipv4": "203.0.113.10",                 // 查询失败保留旧值
    "ipv6": "2001:db8::10",                 // 可选；无 v6 出口为 null
    "agent_version": "0.1.3-beta.5",
    "config": {                             // 当前生效配置（供服务端展示/核对）
      "global": {                           // Agent 全局实际采集；与任何 Reporter 的上报周期无关
        "intervals": {
          "collect": 1, "ping": 30, "slow": 60, "gpu": 60, "ip": 600, "diskio": 10
        },
        "enable_gpu": true,                 // 是否实际启动 GPU worker
        "interfaces": [], "all_interfaces": true, // Reporter 网卡需求并集；true 表示实际采全部接口
        "disks": [], "all_disks": true,     // Reporter 磁盘需求并集；true 表示实际采全部
        "pings": [                           // target URI + 规范化端点去重；全局 worker 没有 Reporter 逻辑 name/type
          { "target": "tcp://gd-ct-dualstack.ip.zstaticcdn.com:80", "interval": 30 },
          { "target": "https://example.com:443", "interval": 60 },
          { "target": "icmp://1.1.1.1", "interval": 60 }
        ]
      },
      "reporters": [                        // 本机全部 Reporter 的脱敏拓扑/输出策略；当前线路是 primary/probe
        {
          "id": "primary", "protocol": "probe", "source_collect_interval": 1, "report_interval": 30, "reset_day": 1,
          "intervals": { "collect": 1, "ping": 30, "slow": 60, "gpu": 60, "ip": 600, "diskio": 10 },
          "interfaces": [], "disks": [], "report_gpu": true, "report_errors": true, "report_self": true,
          "pings": [
            { "name": "ct", "type": "tcp", "target": "gd-ct-dualstack.ip.zstaticcdn.com:80", "interval": 30 },
            { "name": "health", "type": "http", "target": "https://example.com", "interval": 60 },
            { "name": "edge", "type": "icmp", "target": "1.1.1.1", "interval": 60 }
          ]
        },
        {
          "id": "cf-upstream", "protocol": "cf", "source_collect_interval": 0,
          "connection_mode": "auto", "ping_mode": "tcp", "wss_report_interval": 2, "report_interval": 30, "reset_day": 1,
          "intervals": { "collect": 2, "ping": 30, "slow": 60, "gpu": 60, "ip": 600, "diskio": 10 },
          "interfaces": [], "disks": [], "report_gpu": true, "report_errors": false, "report_self": false,
          "pings": [
            { "name": "ct", "type": "tcp", "target": "gd-ct-dualstack.ip.zstaticcdn.com:80", "interval": 30 }
          ]
        },
        {
          "id": "komari", "protocol": "komari", "source_collect_interval": 1, "report_interval": 1, "reset_day": 12,
          "intervals": { "collect": 1, "ping": 30, "slow": 60, "gpu": 60, "ip": 600, "diskio": 10 },
          "interfaces": [], "disks": [], "report_gpu": true, "report_errors": true, "report_self": false,
          "pings": []
        }
      ],                                     // 不含 secret/worker_url/server_id/config_version
      "reset_day": 1,                       // 月流量账期重置日 0-31；0 = 不重置
      "intervals": {                        // 各间隔（秒），完全独立无关系约束
        "collect": 1, "report": 30, "ping": 30, "slow": 60, "gpu": 60, "ip": 600, "diskio": 10
      },
      "interfaces": [],                     // 当前 Reporter 网卡白名单；空 = 默认排除虚拟/隧道网卡
      "disks": [],                          // 当前 Reporter 卷/物理盘 glob；空 = 全部
      "enable_gpu": true,                   // 当前 Reporter 是否输出 GPU（wire 字段沿用旧名）
      "report_errors": true,                // 是否上报 errors 错误事件
      "report_self": true,                  // 是否上报探针自身占用 kind:"self"
      "pings": [                            // 探测目标组
        { "name": "ct", "type": "tcp", "target": "gd-ct-dualstack.ip.zstaticcdn.com:80", "interval": 30 },
        { "name": "health", "type": "http", "target": "https://example.com", "interval": 60 },
        { "name": "edge", "type": "icmp", "target": "1.1.1.1", "interval": 60 }
      ]                                      // probe 没有协议扩展，因此不带 ext.cf
    }
  },

  // 同步采集记录：每个 collect tick 一条；空数组也必报（承担心跳）
  "dynamic": [
    {
      "ts": 1754300060000,                  // 采集时刻（ms），非上报时刻
      "accurate_ts": 1754300059877,         // 可选；采集时保存的校准时间，首次校准前缺席
      "cpu_usage": 12.35,                   // %（0-100）；首轮无前值为 null
      "mem_used": 4294967296,               // 字节，total − MemAvailable
      "swap_used": 134217728,               // 字节
      "load": [0.52, 0.41, 0.30],           // [load1, load5, load15]
      "net_rx": 1073741824,                 // 开机起累计（字节），白名单网卡求和
      "net_tx": 536870912,
      "net_rx_speed": 102400,               // 字节/秒；首轮无前值为 null
      "net_tx_speed": 51200,
      "net_rx_monthly": 858993459200,       // 账期累计（字节），客户端自算
      "net_tx_monthly": 429496729600,
      "net_interfaces": {                   // 逐网卡值；上方兼容字段为选中网卡求和
        "eth0": { "rx": 1073741824, "tx": 536870912, "rx_speed": 102400, "tx_speed": 51200, "rx_monthly": 858993459200, "tx_monthly": 429496729600 }
      }
    }
  ],

  // 异步记录：仅当对应源快照 ts 更新才产生；kind 区分来源，ts 为各自真实测量时刻
  "async": [
    { "kind": "ping", "ts": 1754300058000, "name": "ct", "rtt": 32, "loss": 0 },
    // 探测结果；name = [[pings]] 组 key；rtt = -1 表示探测失败
    { "kind": "slow", "ts": 1754300055000, "disk_used": 53687091200,
      "disks": [{ "id": "/dev/sda1", "name": "sda1", "mount_point": "/", "file_system": "ext4", "total": 107374182400, "used": 53687091200 }],
      "tcp_conn": 120, "udp_conn": 8, "processes": 230 },
    // 系统慢指标（每台机器必有）；disk_used 与 disk_total 同口径；TCP 全状态计数
    { "kind": "gpu", "ts": 1754300050000, "id": "0", "name": "NVIDIA A100 80GB", "usage": 42.5, "mem_total": 85899345920, "mem_used": 10737418240, "temp": 55 },
    // 可选硬件指标（仅部分机器）；多卡时每卡一条；mem/temp 仅 nvidia 路径有，macOS 为 null；无 GPU 时整个 kind 不出现
    { "kind": "self", "ts": 1754300055000, "cpu_usage": 1.2, "mem_rss": 13631488 },
    // 探针自身资源占用；report_self=true 时才有（默认 false）
    { "kind": "diskio", "ts": 1754300056000, "read_bps": 1048576, "write_bps": 524288, "read_iops": 40, "write_iops": 18, "await_ms": 1.8, "usage": 3.2,
      "disks": [{ "name": "sda", "read_bps": 1048576, "write_bps": 524288, "read_iops": 40, "write_iops": 18, "await_ms": 1.8, "usage": 3.2 }] }
    // 磁盘 IO；disks 为逐物理盘，上层字段为当前 Reporter 选中盘的聚合
  ],

  // 错误事件：采集/上报失败记录，空数组 = 无错误；同源同文去重
  "errors": [
    { "ts": 1754300055000, "source": "gpu", "msg": "nvidia-smi exit 1" },
    { "ts": 1754300058000, "source": "ping:health", "msg": "dns resolve failed" },
    { "ts": 1754300059000, "source": "reporter", "msg": "connection refused" }
  ]
}\`;

var CONFIG_EXAMPLE_JSON5 = \`{
  // schema = 1:文件缺省该键视为旧版配置,启动时自动迁移并回写
  "schema": 1,
  // 机器级持久化设置:data_dir 承载 net_static.json 等运行态文件
  "data_dir": "/var/lib/probe-rs",

  // 每条 Reporter 只有 id + 一个协议段(cf/komari/probe),协议由段决定;
  // 段内命名对齐原版 agent,采集需求各自声明后按路聚合
  "reporters": [
    {
      "id": "primary",                          // id 只需在本文件内唯一
      "cf": {                                   // 命名对齐 cfsm-agent
        "server_id": "cf-panel-uuid", "secret": "cf-api-secret",
        "url": "https://cf.example.com/update",
        "connection_mode": "auto",                // auto = WSS + POST 回退；http = 仅 POST
        "ping_mode": "tcp",                       // tcp / icmp，统一控制四条 CF Ping
        "interval": 30,                         // 上报周期(原版 report_interval)
        "collect_interval": 0,                  // 0 时按连接模式映射当前 Reporter 的采集需求
        "wss_report_interval": 2,               // auto + collect_interval=0 时使用
        "reset_day": 1,
        "interface": "",                        // 逗号分隔;空 = Reporter 默认出口过滤
        "ct": "gd-ct-dualstack.ip.zstaticcdn.com:80",
        "cu": "gd-cu-dualstack.ip.zstaticcdn.com:80"
        // cf 线固定:启用 GPU、samples[] 批量；wire 没有 errors/self 落点
        // config_version 由 Agent 回写至 cf.ext,勿手填
      }
    },
    {
      "id": "komari",
      "komari": {                               // 命名对齐 komari-agent
        "endpoint": "https://komari.example.com", "token": "komari-token",
        "interval": 1,                          // 采集周期,komari 按采集周期上报
        "month_rotate": 12,                     // 流量账期重置日(0 = 禁用)
        "enable_gpu": true,
        "include_nics": "", "include_mountpoints": ""
        // learned_pings 是探针自生成状态,落盘在 komari.ext,勿手填
      }
    },
    {
      "id": "local-demo",
      "probe": {                                // probe-rs 原生完整形态
        "server_id": "srv-01", "secret": "change-me",
        "worker_url": "http://127.0.0.1:8080/report",
        "report_interval": 1, "reset_day": 1,
        "report_errors": true, "report_self": true,   // 仅 probe 线可配这两项
        "interfaces": ["Ethernet*"], "disks": ["C:*"], // 空数组 = 全部
        "report_gpu": true,
        "intervals": { "collect": 2, "ping": 60, "slow": 60, "gpu": 60, "ip": 600, "diskio": 10 },
        "pings": [                              // type + 规范化目标去重;周期取最小值
          // 与 primary/cf 的 ct 同目标:只采一次(30s),本路仍用 ct-local 名称接收结果
          { "name": "ct-local", "type": "tcp", "target": "GD-CT-DUALSTACK.IP.ZSTATICCDN.COM", "interval": 60 },
          { "name": "health", "type": "http", "target": "https://example.com", "interval": 60 },
          { "name": "edge", "type": "icmp", "target": "1.1.1.1", "interval": 60 }
        ]
      }
    }
  ]
}\`;

var RESPONSE_EXAMPLE_JSON5 = \`// 无配置变更时：200 OK，body 为 {} 或空
{}

// 有配置变更时（便车下发，随上报响应返回）：
{
  "config": {                       // 配置收在一级；信封后续可扩展其他指令（如动作类）
    "config_version": "2026-08-06T16:00:00.000+08:00",   // 与 agent 当前版本不等才应用（幂等）
    "intervals": {                  // 可选；只改当前 Reporter 的采集需求
      "collect": 2, "ping": 60, "slow": 60, "gpu": 60, "ip": 600, "diskio": 10
    },
    "report_interval": 30,          // 可选；只改当前 Reporter 的上报周期，>= 1
    "reset_day": 15,                // 可选；账期重置日 1-31；0 = 不重置
    "interfaces": ["eth*"],         // 可选；网卡白名单（glob）
    "disks": ["nvme*"],             // 可选；卷/物理盘 glob；空 = 全部
    "pings": [                      // 可选；整组替换；target 只能是端点，不能带 path/query/fragment
      { "name": "edge", "type": "icmp", "target": "1.1.1.1", "interval": 60 }
    ],
    "report_gpu": true,             // 可选；也会参与全局 GPU worker 的 OR 聚合
    "report_errors": false,         // 可选；是否上报 errors 错误事件
    "report_self": true             // 可选；是否上报探针自身资源占用 kind:"self"
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
  document.getElementById('summary').textContent = 'probe 原生协议 · POST /report 请求与响应';
  app.append(
    annotatedCard(EXAMPLE_JSON5, '//', [
      '此例是 primary / probe 原生 Reporter。请求头：X-Secret（认证）、X-Agent-Version、X-Reporter-Id: primary、X-Reporter-Protocol: probe（Reporter id 会百分号编码；旧服务端可忽略新增头）。',
      'static.config.global 是实际采集事实；static.config.reporters 是全线路脱敏摘要；同级 reset_day/intervals/... 仍是当前 Reporter 视角。',
      'CF / Komari 只在 reporters 脱敏拓扑中出现；它们自己的 wire 报文请看「协议预览」，probe 请求不携带 ext.cf。',
      '上报失败时 agent 将 dynamic/async/errors 共享保留在 512 条有界日志中待重发——只覆盖短暂抖动，长断网历史不补发；服务端收到延迟记录按 ts 去重排序即可。',
    ]),
    annotatedCard(RESPONSE_EXAMPLE_JSON5, '//', [
      'agent 行为：整体校验（版本、周期、glob、Ping），全部通过才应用当前 Reporter 并落盘；随后重新计算全局 collect 最大公约数、其他 worker 最小周期、GPU OR、网卡/磁盘/Ping 并集。',
      '🔒 连接身份与 data_dir 只接受本地配置；远端只能修改产生该响应的 Reporter。允许下发 Ping 时，服务端应自行限制目标，避免 SSRF/内网探测。',
    ]));
}

function renderConfigExample() {
  var app = document.getElementById('app');
  app.textContent = '';
  document.getElementById('summary').textContent = 'agent 本地配置（JSON5 注释版）';
  app.append(annotatedCard(CONFIG_EXAMPLE_JSON5, '//', [
    '实际配置文件为 TOML（仓库根目录 config.example.toml），此处以 JSON5 等价展示。旧的根级 intervals / enable_gpu / pings 不再兼容。',
    'reporters[] 等价于多个 [[reporters]]。每路有独立上报周期、游标、ACK、重试和远端配置版本；采集 worker 全局只运行一份，并由全部 Reporter 的需求自动聚合。',
  ]));
}

function loadReports() {
  // 同时拉 servers（CF 视角换算需要各机最新 static）
  fetch('/api/servers').then(function (r) { return r.json(); }).then(function (data) {
    data.forEach(function (s) { if (s.static) serverStatics[instanceKey(s)] = s.static; });
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

// PAGE 是 TypeScript 模板字符串，内嵌 JS 的反斜杠需要双重转义。
// 启动时直接解析实际将要发送的脚本，避免服务正常监听但浏览器静默卡在“连接中”。
const pageScriptStart = PAGE.indexOf("<script>");
const pageScriptEnd = PAGE.indexOf("</script>", pageScriptStart);
if (pageScriptStart < 0 || pageScriptEnd < 0) {
  throw new Error("demo PAGE is missing its inline script");
}
new Function(PAGE.slice(pageScriptStart + "<script>".length, pageScriptEnd));
