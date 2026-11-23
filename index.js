// openclaw-task-reporter — OpenCode V2 plugin
//
// Logs the work of every OpenCode/OpenChamber session linked to a TickTick
// task, at zero LLM-token cost, and gives the agent one explicit tool to
// notify the OpenClaw "task-manager" agent. See openclaw-task-reporter.md
// next to this file for setup on other devices and troubleshooting.
//
// Data flow
//   OpenCode events ──► Core (sessions, links, busy intervals)
//                          │ intents (focus chunk, comment, webhook)
//                          ▼
//                      durable Outbox (one JSON file per item, O_EXCL created)
//                          │ replayed on startup + timer, exp. backoff
//                          ▼
//          TickTick (mcphub MCP)      OpenClaw /hooks/task-progress (v2)
//   The OpenClaw ledger is the official history; TickTick is a projection.
//
// Linking
//   A user message containing https://ticktick.com/webapp/#p/<proj>/tasks/<task>
//   or `ticktick:<taskId>` links that session (latest wins). Sessions that
//   started before the plugin loaded are scanned once (their user messages
//   via ctx.session.context) the first time they become busy. Children and
//   grandchildren inherit the nearest linked ancestor's link (resolved by
//   walking the parent chain at use time). The `link_task` tool links
//   mid-session.
//
// Time
//   Time is tracked per root session tree: the tree is active while any
//   session in it executes (V2 session.execution.started → succeeded|failed|
//   interrupted; V1 session.status/session.idle as fallback). Each busy span
//   becomes an interval; intervals separated by < 2 min are merged. After the
//   tree has been idle for idleMin (default 10), the block is flushed: one
//   TickTick focus record per active interval (≥ 1 min, split at 12 h), one
//   comment, and one ledger webhook carrying the intervals. Idle gaps are
//   never logged as focus time. A block with < 1 min of activity is carried
//   into the next block instead of being dropped. A tree still "busy" with no
//   events for 2 h is closed at its last event (lost end event).
//
// Failure policy
//   Periodic idle flushes expire from the outbox after 24 h (logged, never
//   silent). Milestones — report_progress done/blocked and the final flush on
//   session deletion / plugin unload / recovery from a dead process — retry
//   until they succeed. Before re-creating a focus record or comment whose
//   previous attempt had an unknown outcome, TickTick is queried for the
//   eventId marker (focus note / comment "ref" footer) to avoid duplicates.
//   Webhooks are idempotent on eventId at the OpenClaw side.
//
// Concurrency
//   Several OpenCode processes (TUI + OpenChamber server) can run on one
//   device. State read-merge-write and outbox processing each run under an
//   O_EXCL lockfile (stale when its holder pid is dead or its heartbeat
//   mtime is old). Outbox items are created with link(2) so two processes
//   computing the same deterministic item id enqueue it once. ctx.storage is
//   not used: it has no compare-and-set, so it cannot coordinate processes.
//
// V2 plugin shape (https://opencode.ai/docs/build/plugins/server): default export
// `{ id, setup(ctx) }`; events via ctx.event.subscribe; prompts via
// ctx.session.hook("prompt") (event.sessionID, event.prompt.text); tools via
// ctx.tool.transform; setup returns the cleanup function. setup runs once per
// location, so the core is a per-process singleton on globalThis.
//
// Config: env vars override ~/.config/opencode/openclaw-reporter.json (chmod
// 600) which overrides defaults. See loadConfig() and the README.
// Logs: <stateDir>/openclaw-task-reporter.log. Secrets are never logged.

import fs from "node:fs"
import path from "node:path"
import os from "node:os"
import https from "node:https"
import http from "node:http"
import crypto from "node:crypto"

const PLUGIN_ID = "openclaw-task-reporter"
const VERSION = "2.2.0"
const GLOBAL_KEY = "__openclawTaskReporterCore"
// No URL is hardcoded and there is no loopback fallback: hooksUrl must come
// from the config file / OPENCLAW_HOOKS_URL, or nothing is sent.
const MIN = 60_000
const HOUR = 60 * MIN
const DAY = 24 * HOUR
const HOST = os.hostname()

// OpenCode imports the plugin once per location, so one process can hold
// several module instances of the same file. They share one core, keyed by
// the source text: identical code joins the running core, edited code (hot
// reload) replaces it.
const MODULE_TOKEN = (() => {
  try {
    return `${VERSION}:${crypto.createHash("sha256").update(fs.readFileSync(new URL(import.meta.url))).digest("hex").slice(0, 12)}`
  } catch {
    return VERSION
  }
})()

// ─── Config ─────────────────────────────────────────────────────────────

function sanitizeDevice(value) {
  return (
    String(value || "unknown")
      .replace(/\.local$/i, "")
      .toLowerCase()
      .replace(/[^a-z0-9._-]+/g, "-")
      .slice(0, 40) || "unknown"
  )
}

function loadConfig(env = process.env) {
  const home = os.homedir()
  const fileConfigPath =
    env.OPENCLAW_REPORTER_CONFIG || path.join(home, ".config/opencode/openclaw-reporter.json")
  const file = readJsonFile(fileConfigPath) ?? {}
  const openclawConfig = path.join(home, ".openclaw/openclaw.json")
  // Gateway host = this machine runs OpenClaw AND its config carries a hooks
  // token. The token value is never kept in config or logged.
  const hubToken = readJsonFile(openclawConfig)?.hooks?.token
  const isHub = typeof hubToken === "string" && hubToken.trim() !== ""
  const pick = (name, key) => (env[name] !== undefined && env[name] !== "" ? env[name] : file[key])
  const num = (v, d) => {
    const n = Number(v)
    return Number.isFinite(n) && n > 0 ? n : d
  }
  const bool = (v, d) =>
    v === undefined || v === null || v === "" ? d : v === true || v === 1 || v === "1" || v === "true"
  const stateDir =
    env.OPENCLAW_REPORTER_STATE_DIR ||
    (env.OPENCLAW_REPORTER_STATE ? path.dirname(env.OPENCLAW_REPORTER_STATE) : "") ||
    file.stateDir ||
    path.join(home, ".config/opencode/state")
  return {
    version: VERSION,
    fileConfigPath,
    fileConfigFound: Object.keys(file).length > 0,
    isHub,
    openclawConfig,
    disabled: bool(pick("OPENCLAW_REPORTER_DISABLED", "disabled"), false),
    debug: bool(pick("OPENCLAW_REPORTER_DEBUG", "debug"), false),
    focus: bool(pick("OPENCLAW_REPORTER_FOCUS", "focus"), true),
    idleMs: num(pick("OPENCLAW_REPORTER_IDLE_MIN", "idleMin"), 10) * MIN,
    minBlockMs: num(pick("OPENCLAW_REPORTER_MIN_BLOCK_MIN", "minBlockMin"), 1) * MIN,
    staleBusyMs: num(pick("OPENCLAW_REPORTER_STALE_BUSY_MIN", "staleBusyMin"), 120) * MIN,
    mergeGapMs: 2 * MIN,
    maxChunkMs: 12 * HOUR,
    device: sanitizeDevice(pick("OPENCLAW_REPORTER_DEVICE", "device") || HOST),
    mcpUrl: pick("OPENCLAW_REPORTER_MCP_URL", "mcpUrl") || "",
    mcpServer: pick("OPENCLAW_REPORTER_MCP_SERVER", "mcpServer") || "ticktick",
    mcpAuth: pick("OPENCLAW_REPORTER_MCP_AUTH", "mcpAuth") || "",
    // No default: without hooksUrl (and not on the gateway host) nothing is sent.
    hooksUrl: pick("OPENCLAW_HOOKS_URL", "hooksUrl") || "",
    hooksFallbackUrl: "",
    hooksToken: pick("OPENCLAW_HOOKS_TOKEN", "hooksToken") || "",
    // Cloudflare Access service token; sent only when both parts are set.
    cfAccessClientId: String(pick("OPENCLAW_CF_ACCESS_CLIENT_ID", "cfAccessClientId") || ""),
    cfAccessClientSecret: String(pick("OPENCLAW_CF_ACCESS_CLIENT_SECRET", "cfAccessClientSecret") || ""),
    opencodeConfig: path.join(home, ".config/opencode/opencode.json"),
    mcphubTokenFile: path.join(home, ".config/opencode/secrets/mcphub-token"),
    stateDir,
    statePath: env.OPENCLAW_REPORTER_STATE || path.join(stateDir, `${PLUGIN_ID}.json`),
    outboxDir: path.join(stateDir, `${PLUGIN_ID}-outbox`),
    logPath: path.join(stateDir, `${PLUGIN_ID}.log`),
    fetchTimeoutMs: num(env.OPENCLAW_REPORTER_FETCH_TIMEOUT_MS, 20_000),
    localProbeTimeoutMs: 3_000,
    periodicTtlMs: num(env.OPENCLAW_REPORTER_TTL_MIN, 24 * 60) * MIN,
    backoffBaseMs: num(env.OPENCLAW_REPORTER_BACKOFF_BASE_MS, 30_000),
    backoffCapMs: 30 * MIN,
    depWaitMs: 15 * MIN,
    outboxTickMs: num(env.OPENCLAW_REPORTER_TICK_MS, 30_000),
    sweepMs: num(env.OPENCLAW_REPORTER_SWEEP_MS, MIN),
    retainDoneMs: 7 * DAY,
  }
}

// ─── Small helpers ──────────────────────────────────────────────────────

const chars = (text) => Array.from(String(text ?? ""))

function clip(text, max) {
  const points = chars(text)
  if (points.length <= max) return points.join("")
  return points.slice(0, Math.max(0, max - 1)).join("") + "…"
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const round1 = (n) => Math.round(n * 10) / 10
const hash = (text, len = 16) => crypto.createHash("sha256").update(String(text)).digest("hex").slice(0, len)
const iso = (ms) => new Date(ms).toISOString()

// eventId: deterministic, ≤ 200 chars of [A-Za-z0-9:._-].
function eventIdOf(...parts) {
  const raw = parts.join(":").replace(/[^A-Za-z0-9:._-]+/g, "_")
  return raw.length <= 200 ? raw : `${raw.slice(0, 183)}:${hash(raw)}`
}

function parseLooseJson(text) {
  try {
    return JSON.parse(text)
  } catch {
    const stripped = text
      .replace(/\/\*[\s\S]*?\*\//g, "")
      .replace(/(^|[^:"\\])\/\/.*$/gm, "$1")
      .replace(/,(\s*[}\]])/g, "$1")
    return JSON.parse(stripped)
  }
}

function readJsonFile(file) {
  try {
    return parseLooseJson(fs.readFileSync(file, "utf8"))
  } catch {
    return null
  }
}

function writeFileAtomic(file, data) {
  fs.mkdirSync(path.dirname(file), { recursive: true })
  const tmp = `${file}.${process.pid}.${crypto.randomBytes(4).toString("hex")}.tmp`
  fs.writeFileSync(tmp, data)
  fs.renameSync(tmp, file)
}

function pidAlive(pid) {
  if (!pid) return false
  try {
    process.kill(pid, 0)
    return true
  } catch (err) {
    return err?.code === "EPERM"
  }
}

const eventProps = (event) => event?.data ?? event?.properties ?? {}

function errMsg(err) {
  return clip(String(err?.message ?? err), 300)
}

// ─── Link parsing ───────────────────────────────────────────────────────

const URL_RE = /(?:ticktick|dida365)\.com\/webapp\/#p\/([0-9a-f]{24})\/tasks\/([0-9a-f]{24})/gi
const MARKER_RE = /\bticktick:([0-9a-f]{24})\b/gi
const BARE_RE = /^\s*([0-9a-f]{24})\s*$/i

// Returns the LAST reference in the text (the most recent instruction wins).
function parseTaskRef(text, { allowBare = false } = {}) {
  const value = String(text ?? "")
  let best = null
  for (const m of value.matchAll(URL_RE)) {
    best = { index: m.index, projectId: m[1].toLowerCase(), taskId: m[2].toLowerCase() }
  }
  for (const m of value.matchAll(MARKER_RE)) {
    if (!best || m.index > best.index) best = { index: m.index, projectId: null, taskId: m[1].toLowerCase() }
  }
  if (best) return { projectId: best.projectId, taskId: best.taskId }
  if (allowBare) {
    const bare = value.match(BARE_RE)
    if (bare) return { projectId: null, taskId: bare[1].toLowerCase() }
  }
  return null
}

// ─── Logger ─────────────────────────────────────────────────────────────

function makeLogger(config) {
  let checked = false
  const once = new Set()
  const write = (level, message) => {
    try {
      fs.mkdirSync(path.dirname(config.logPath), { recursive: true })
      if (!checked) {
        checked = true
        const size = fs.statSync(config.logPath, { throwIfNoEntry: false })?.size ?? 0
        if (size > 2_000_000) fs.renameSync(config.logPath, `${config.logPath}.1`)
      }
      fs.appendFileSync(config.logPath, `${new Date().toISOString()} ${level} [${process.pid}] ${message}\n`)
    } catch {
      // logging must never break the plugin
    }
  }
  return {
    info: (m) => write("INFO", m),
    warn: (m) => write("WARN", m),
    error: (m) => write("ERROR", m),
    debug: (m) => config.debug && write("DEBUG", m),
    debugOnce: (key, m) => {
      if (!config.debug || once.has(key)) return
      once.add(key)
      write("DEBUG", m)
    },
  }
}

// ─── O_EXCL lockfile ────────────────────────────────────────────────────

class FileLock {
  constructor(file, { staleMs = 60_000, log } = {}) {
    this.file = file
    this.staleMs = staleMs
    this.log = log
    this.token = null
    this.heartbeat = null
  }

  readHolder() {
    try {
      const stat = fs.statSync(this.file)
      let info = null
      try {
        info = JSON.parse(fs.readFileSync(this.file, "utf8"))
      } catch {}
      return { stat, info }
    } catch {
      return null
    }
  }

  isStale(holder) {
    if (!holder) return false
    const age = Date.now() - holder.stat.mtimeMs
    if (!holder.info) return age > 5_000 // half-written by a crashed process
    if (holder.info.host === HOST && holder.info.pid !== process.pid && !pidAlive(holder.info.pid)) return true
    return age > this.staleMs
  }

  createOnce() {
    const token = crypto.randomUUID()
    let fd
    try {
      fs.mkdirSync(path.dirname(this.file), { recursive: true })
      fd = fs.openSync(this.file, "wx", 0o600)
    } catch (err) {
      if (err?.code === "EEXIST") return false
      throw err
    }
    try {
      fs.writeSync(fd, JSON.stringify({ pid: process.pid, host: HOST, token, at: Date.now() }))
    } finally {
      fs.closeSync(fd)
    }
    this.token = token
    this.heartbeat = setInterval(() => {
      try {
        const now = new Date()
        fs.utimesSync(this.file, now, now)
      } catch {}
    }, Math.max(1000, Math.floor(this.staleMs / 3)))
    this.heartbeat.unref?.()
    return true
  }

  tryAcquire() {
    if (this.token) return false // not re-entrant
    if (this.createOnce()) return true
    const holder = this.readHolder()
    if (!this.isStale(holder)) return false
    // Break the stale lock: move it aside, and put it back if what we moved
    // turns out to be a fresh lock another breaker created meanwhile.
    const aside = `${this.file}.stale.${process.pid}.${crypto.randomBytes(3).toString("hex")}`
    try {
      fs.renameSync(this.file, aside)
      let moved = null
      try {
        moved = JSON.parse(fs.readFileSync(aside, "utf8"))
      } catch {}
      if (holder.info && moved && moved.token !== holder.info.token) {
        try {
          fs.linkSync(aside, this.file)
        } catch {}
        fs.rmSync(aside, { force: true })
        return false
      }
      fs.rmSync(aside, { force: true })
      this.log?.warn(`broke stale lock ${path.basename(this.file)} (holder pid ${holder.info?.pid ?? "?"})`)
    } catch {
      return false
    }
    return this.createOnce()
  }

  async acquire(waitMs) {
    const deadline = Date.now() + waitMs
    for (;;) {
      if (this.tryAcquire()) return true
      if (Date.now() >= deadline) return false
      await sleep(40 + Math.random() * 80)
    }
  }

  release() {
    if (!this.token) return
    clearInterval(this.heartbeat)
    this.heartbeat = null
    try {
      const info = JSON.parse(fs.readFileSync(this.file, "utf8"))
      if (info.token === this.token) fs.rmSync(this.file, { force: true })
    } catch {}
    this.token = null
  }
}

// ─── MCP client (streamable HTTP JSON-RPC) ─────────────────────────────

// `unknown` = the request may have been applied but we got no answer.
class CallError extends Error {
  constructor(message, { unknown = false, status = 0 } = {}) {
    super(message)
    this.unknown = unknown
    this.status = status
  }
}

const DEFINITE_NET = /refused|ENOTFOUND|EAI_AGAIN|ECONNREFUSED|Unable to connect|ConnectionRefused|certificate|FailedToOpenSocket/i

function netError(err, what) {
  const message = `${what}: ${errMsg(err)}`
  const definite = DEFINITE_NET.test(`${err?.code ?? ""} ${err?.message ?? ""}`)
  return new CallError(message, { unknown: !definite })
}

// Cloudflare Access service-token headers; both parts or nothing.
function cfAccessHeaders(config) {
  const id = config.cfAccessClientId
  const secret = config.cfAccessClientSecret
  return id && secret ? { "CF-Access-Client-Id": id, "CF-Access-Client-Secret": secret } : {}
}

function resolveMcpAuth(config) {
  if (config.mcpAuth) return /^\S+\s+\S+/.test(config.mcpAuth) ? config.mcpAuth : `Bearer ${config.mcpAuth}`
  const cfg = readJsonFile(config.opencodeConfig)
  const mcp = cfg?.mcp ?? {}
  const hub = mcp["hub-tunnel"] ?? mcp.servers?.["hub-tunnel"]
  let header = hub?.headers?.Authorization ?? hub?.headers?.authorization ?? ""
  header = header
    .replace(/\{env:([^}]+)\}/g, (_, name) => process.env[name] ?? "")
    .replace(/\{file:([^}]+)\}/g, (_, file) => {
      try {
        return fs.readFileSync(file.replace(/^~/, os.homedir()), "utf8").trim()
      } catch {
        return ""
      }
    })
  if (/^Bearer\s+\S+/.test(header)) return header.trim()
  let token = process.env.MCPHUB_TOKEN ?? ""
  if (!token) {
    try {
      token = fs.readFileSync(config.mcphubTokenFile, "utf8").trim()
    } catch {}
  }
  return token ? `Bearer ${token}` : ""
}

function mcpUrls(config) {
  if (config.mcpUrl) return [config.mcpUrl]
  const urls = [`http://localhost:37373/mcp/${config.mcpServer}`]
  const cfg = readJsonFile(config.opencodeConfig)
  const mcp = cfg?.mcp ?? {}
  const hubUrl = (mcp["hub-tunnel"] ?? mcp.servers?.["hub-tunnel"])?.url
  if (hubUrl && URL.canParse(hubUrl)) urls.push(`${new URL(hubUrl).origin}/mcp/${config.mcpServer}`)
  return [...new Set(urls)]
}

function parseRpcBody(text, contentType, id) {
  if (!String(contentType).includes("text/event-stream")) {
    const parsed = JSON.parse(text)
    return Array.isArray(parsed) ? parsed.find((m) => m.id === id) : parsed
  }
  let found = null
  for (const block of text.split(/\r?\n\r?\n/)) {
    const data = block
      .split(/\r?\n/)
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).replace(/^ /, ""))
      .join("\n")
    if (!data) continue
    try {
      const msg = JSON.parse(data)
      if (msg.id === id || (!found && ("result" in msg || "error" in msg))) found = msg
    } catch {}
  }
  return found
}

class McpClient {
  constructor(config, log) {
    this.config = config
    this.log = log
    this.sessionId = null
    this.url = null
    this.auth = ""
    this.ready = null
    this.nextId = 1
  }

  async post(url, body, timeoutMs) {
    const headers = {
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
      "mcp-protocol-version": "2025-03-26",
    }
    if (this.auth) headers.authorization = this.auth
    if (!/^https?:\/\/(localhost|127\.0\.0\.1|\[::1\])[:/]/.test(url)) Object.assign(headers, cfAccessHeaders(this.config))
    if (this.sessionId) headers["mcp-session-id"] = this.sessionId
    const res = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs ?? this.config.fetchTimeoutMs),
    })
    const sid = res.headers.get("mcp-session-id")
    if (sid) this.sessionId = sid
    const text = await res.text()
    return { status: res.status, contentType: res.headers.get("content-type") ?? "", text }
  }

  async initialize() {
    this.auth = resolveMcpAuth(this.config)
    if (!this.auth) throw new CallError("no MCP auth (set mcpAuth in openclaw-reporter.json)")
    let lastError = null
    for (const url of mcpUrls(this.config)) {
      this.sessionId = null
      const local = /\/\/(localhost|127\.0\.0\.1)[:/]/.test(url)
      try {
        const id = this.nextId++
        const res = await this.post(
          url,
          {
            jsonrpc: "2.0",
            id,
            method: "initialize",
            params: { protocolVersion: "2025-03-26", capabilities: {}, clientInfo: { name: PLUGIN_ID, version: VERSION } },
          },
          local ? this.config.localProbeTimeoutMs : undefined,
        )
        if (res.status !== 200) throw new Error(`HTTP ${res.status}`)
        const msg = parseRpcBody(res.text, res.contentType, id)
        if (!msg?.result) throw new Error(clip(JSON.stringify(msg?.error ?? "no result"), 160))
        await this.post(url, { jsonrpc: "2.0", method: "notifications/initialized" })
        if (this.url !== url) this.log.info(`mcp endpoint ${url}`)
        this.url = url
        return
      } catch (err) {
        lastError = err
        this.log.debug(`mcp init ${url}: ${errMsg(err)}`)
      }
    }
    throw new CallError(`mcp unreachable: ${errMsg(lastError)}`)
  }

  ensure() {
    if (!this.ready) {
      this.ready = this.initialize().catch((err) => {
        this.ready = null
        throw err
      })
    }
    return this.ready
  }

  async call(name, args) {
    for (let attempt = 0; attempt < 2; attempt++) {
      await this.ensure()
      const id = this.nextId++
      let res
      try {
        res = await this.post(this.url, { jsonrpc: "2.0", id, method: "tools/call", params: { name, arguments: args } })
      } catch (err) {
        this.ready = null
        throw netError(err, name)
      }
      // Expired MCP session or auth change: re-initialize once. The call was
      // rejected before the tool ran, so retrying is safe.
      if ([400, 401, 404].includes(res.status) && attempt === 0) {
        this.ready = null
        continue
      }
      if (res.status !== 200) {
        throw new CallError(`${name}: HTTP ${res.status}`, { unknown: res.status >= 500, status: res.status })
      }
      let msg
      try {
        msg = parseRpcBody(res.text, res.contentType, id)
      } catch {
        msg = null
      }
      if (!msg) throw new CallError(`${name}: unparseable response`, { unknown: true })
      if (msg.error) throw new CallError(`${name}: ${clip(msg.error.message ?? JSON.stringify(msg.error), 300)}`)
      const result = msg.result ?? {}
      const text = (result.content ?? [])
        .filter((c) => c?.type === "text")
        .map((c) => c.text)
        .join("\n")
      if (result.isError) throw new CallError(`${name}: ${clip(text, 300)}`)
      if (result.structuredContent !== undefined) return result.structuredContent?.result ?? result.structuredContent
      try {
        return JSON.parse(text)
      } catch {
        return text
      }
    }
    throw new CallError(`${name}: failed after re-initialize`)
  }
}

// ─── TickTick wrapper ───────────────────────────────────────────────────

const asList = (value) => (Array.isArray(value) ? value : Array.isArray(value?.result) ? value.result : [])

function makeTickTick(mcp) {
  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone
  return {
    getTask: (taskId) => mcp.call("get_task_by_id", { task_id: taskId }),
    addComment: (projectId, taskId, text) =>
      mcp.call("add_comment", { project_id: projectId, task_id: taskId, title: clip(text, 1024) }),
    listComments: async (projectId, taskId) => asList(await mcp.call("get_comment", { project_id: projectId, task_id: taskId })),
    createFocus: ({ start, end, taskId, note }) =>
      mcp.call("create_focus", {
        start_time: iso(start),
        end_time: iso(end),
        type: 1,
        task_id: taskId ?? null,
        note: note ? clip(note, 500) : null,
        client_timezone: tz,
      }),
    listFocuses: async (from, to) => asList(await mcp.call("get_focuses_by_time", { from_time: iso(from), to_time: iso(to), type: 1 })),
  }
}

// ─── OpenClaw webhook ───────────────────────────────────────────────────

function resolveHooksToken(config) {
  if (config.hooksToken) return config.hooksToken
  if (config.isHub) return readJsonFile(config.openclawConfig)?.hooks?.token ?? ""
  return ""
}

// Can the webhook be sent at all? Reasons start with "no hooks url" /
// "no hooks token" (the outbox requeue keys on these).
function webhookReadiness(config) {
  if (!config.hooksUrl) {
    return { ok: false, reason: "no hooks url (set hooksUrl in openclaw-reporter.json or OPENCLAW_HOOKS_URL)" }
  }
  if (!URL.canParse(config.hooksUrl)) return { ok: false, reason: "no hooks url (hooksUrl is not a valid URL)" }
  if (!resolveHooksToken(config)) {
    return { ok: false, reason: "no hooks token (set hooksToken in openclaw-reporter.json)" }
  }
  return { ok: true, reason: "" }
}

// Delivered = 2xx (200 manager run, 204 ledgered/duplicate). On the gateway
// host a network failure on the primary URL falls back to loopback.
async function postWebhook(config, payload, idempotencyKey) {
  const ready = webhookReadiness(config)
  if (!ready.ok) return { ok: false, status: 0, error: ready.reason, notConfigured: true }
  const res = await postWebhookTo(config, config.hooksUrl, payload, idempotencyKey)
  if (res.status === 0 && config.hooksFallbackUrl && !res.unknown) {
    const alt = await postWebhookTo(config, config.hooksFallbackUrl, payload, idempotencyKey)
    if (alt.status !== 0 || alt.unknown) return { ...alt, via: "loopback" }
  }
  return res
}

function postWebhookTo(config, target, payload, idempotencyKey) {
  return new Promise((resolve) => {
    let url
    try {
      url = new URL(target)
    } catch {
      return resolve({ ok: false, status: 0, error: "bad hooksUrl" })
    }
    const token = resolveHooksToken(config)
    if (!token) return resolve({ ok: false, status: 0, error: "no hooks token (set hooksToken in openclaw-reporter.json)" })
    const body = JSON.stringify(payload)
    const loopback = ["127.0.0.1", "localhost", "::1", "[::1]"].includes(url.hostname)
    const lib = url.protocol === "https:" ? https : http
    let sent = false
    const req = lib.request(
      url,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "content-length": Buffer.byteLength(body),
          authorization: `Bearer ${token}`,
          "idempotency-key": idempotencyKey,
          ...cfAccessHeaders(config),
        },
        // The gateway's loopback listener is self-signed; the public hostname has
        // a valid certificate, so verification stays on everywhere else.
        rejectUnauthorized: !loopback,
        timeout: config.fetchTimeoutMs,
      },
      (res) => {
        let data = ""
        res.setEncoding("utf8")
        res.on("data", (chunk) => {
          if (data.length < 4000) data += chunk
        })
        res.on("end", () => {
          let json = null
          try {
            json = data ? JSON.parse(data) : null
          } catch {}
          resolve({ ok: res.statusCode >= 200 && res.statusCode < 300, status: res.statusCode, json, text: data })
        })
      },
    )
    req.on("finish", () => (sent = true))
    req.on("timeout", () => req.destroy(new Error("timeout")))
    // Bun emits "finish" before connecting, so a refused/unresolvable
    // connection is classified by its error code, not by "sent".
    req.on("error", (err) =>
      resolve({ ok: false, status: 0, error: errMsg(err), unknown: sent && !DEFINITE_NET.test(`${err?.code ?? ""} ${err?.message ?? ""}`) }),
    )
    req.end(body)
  })
}

// ─── Durable outbox ─────────────────────────────────────────────────────
//
// One JSON file per item: { id, kind, payload, deps, milestone, state,
// attempts, unknown, nextAttemptAt, expiresAt, createdAt, lastError,
// result }. Created atomically with link(2) (EEXIST = already queued, by us
// or another process). Processed only under the outbox lock; each item is
// re-read from disk before it is sent.

const MISSING_CONFIG_ERR = /^(webhook HTTP )?no hooks (token|url)|bad hooksUrl/

class Outbox {
  constructor({ config, log, handlers, now }) {
    this.config = config
    this.log = log
    this.handlers = handlers
    this.now = now ?? (() => Date.now())
    this.dir = config.outboxDir
    this.lock = new FileLock(path.join(config.stateDir, `${PLUGIN_ID}-outbox.lock`), { staleMs: 2 * MIN, log })
    this.running = null
    this.timer = null
    this.kickTimer = null
    this.lastPrune = 0
  }

  file(id) {
    const safe = id.replace(/[^A-Za-z0-9._-]+/g, "_")
    return path.join(this.dir, `${safe.length > 150 ? safe.slice(0, 133) + "_" + hash(id) : safe}.json`)
  }

  read(id) {
    return readJsonFile(this.file(id))
  }

  write(item) {
    writeFileAtomic(this.file(item.id), JSON.stringify(item))
  }

  // Returns true if newly queued, false if the id already exists.
  enqueue({ id, kind, payload, deps = [], milestone = false, delayMs = 0 }) {
    const now = this.now()
    const item = {
      id,
      kind,
      payload,
      deps,
      milestone,
      state: "pending",
      attempts: 0,
      unknown: false,
      createdAt: now,
      nextAttemptAt: now + delayMs,
      expiresAt: milestone ? null : now + this.config.periodicTtlMs,
      lastError: null,
      result: null,
    }
    fs.mkdirSync(this.dir, { recursive: true })
    const target = this.file(id)
    const tmp = `${target}.${process.pid}.${crypto.randomBytes(4).toString("hex")}.tmp`
    fs.writeFileSync(tmp, JSON.stringify(item))
    try {
      fs.linkSync(tmp, target)
      return true
    } catch (err) {
      if (err?.code === "EEXIST") return false
      throw err
    } finally {
      fs.rmSync(tmp, { force: true })
    }
  }

  list() {
    let names = []
    try {
      names = fs.readdirSync(this.dir).filter((n) => n.endsWith(".json"))
    } catch {
      return []
    }
    return names.map((n) => readJsonFile(path.join(this.dir, n))).filter((x) => x?.id)
  }

  backoff(attempts) {
    const base = this.config.backoffBaseMs * 2 ** Math.min(attempts - 1, 20)
    return Math.min(this.config.backoffCapMs, base) * (0.8 + Math.random() * 0.4)
  }

  // One-time, idempotent revival of webhook items that were rejected or
  // expired ONLY because no hooks token/URL was configured. Never touches
  // delivered ("done") items; each item is revived at most once (marker
  // `requeuedAt`). Does nothing until delivery is actually configured, and
  // nothing is deleted. Returns the number of items requeued.
  requeueMissingConfig() {
    if (!webhookReadiness(this.config).ok) return 0
    if (!this.lock.tryAcquire()) return 0 // another process holds it; next start retries
    let count = 0
    try {
      const now = this.now()
      for (const listed of this.list()) {
        const item = this.read(listed.id)
        if (!item || item.kind !== "webhook" || item.requeuedAt) continue
        if (!MISSING_CONFIG_ERR.test(String(item.lastError ?? ""))) continue
        if (item.state === "pending") {
          // parked while unconfigured: make it due now instead of after the park delay
          if (item.nextAttemptAt > now) {
            item.nextAttemptAt = now
            item.lastError = null
            this.write(item)
            count++
          }
          continue
        }
        if (item.state !== "rejected" && item.state !== "expired") continue
        item.requeuedFrom = item.state
        item.requeuedAt = now
        item.state = "pending"
        item.nextAttemptAt = now
        item.expiresAt = item.milestone ? null : now + this.config.periodicTtlMs
        item.finishedAt = null
        this.write(item)
        count++
      }
    } finally {
      this.lock.release()
    }
    if (count) this.log.info(`outbox: requeued ${count} webhook item(s) that failed only for a missing hooks token/url`)
    return count
  }

  start() {
    if (this.timer) return
    try {
      this.requeueMissingConfig()
    } catch (err) {
      this.log.warn(`outbox requeue failed: ${errMsg(err)}`)
    }
    this.timer = setInterval(() => void this.process().catch(() => {}), this.config.outboxTickMs)
    this.timer.unref?.()
    void this.process().catch(() => {})
  }

  stop() {
    clearInterval(this.timer)
    clearTimeout(this.kickTimer)
    this.timer = null
  }

  kick() {
    clearTimeout(this.kickTimer)
    this.kickTimer = setTimeout(() => void this.process().catch(() => {}), 50)
    this.kickTimer.unref?.()
  }

  // Process due items. `only` limits to some ids (still in dependency order);
  // `waitMs` waits that long for the lock (another process may hold it).
  process({ only = null, waitMs = 0 } = {}) {
    if (this.running && !only) return this.running
    const run = (async () => {
      if (this.running && only) await this.running.catch(() => {})
      const got = waitMs ? await this.lock.acquire(waitMs) : this.lock.tryAcquire()
      if (!got) return false
      try {
        await this.processLocked(only ? new Set(only) : null)
      } finally {
        this.lock.release()
      }
      return true
    })()
    const tracked = run.finally(() => {
      if (this.running === tracked) this.running = null
    })
    this.running = tracked
    return tracked
  }

  async processLocked(only) {
    const now = this.now()
    const items = this.list()
      .filter((i) => i.state === "pending" && (!only || only.has(i.id)))
      .sort(
        (a, b) =>
          a.createdAt - b.createdAt ||
          (a.kind === "webhook") - (b.kind === "webhook") ||
          a.id.localeCompare(b.id, "en", { numeric: true }),
      )
    for (const listed of items) {
      const item = this.read(listed.id) // fresh copy: another process may have finished it
      if (!item || item.state !== "pending") continue
      if (!only && item.nextAttemptAt > this.now()) continue
      if (item.expiresAt && this.now() > item.expiresAt) {
        item.state = "expired"
        item.finishedAt = this.now()
        this.write(item)
        this.log.error(`outbox EXPIRED ${item.kind} ${item.id} after ${item.attempts} attempts: ${item.lastError ?? "never attempted"}`)
        continue
      }
      const handler = this.handlers[item.kind]
      if (!handler) {
        this.log.error(`outbox: unknown kind ${item.kind} for ${item.id}`)
        continue
      }
      let outcome
      try {
        outcome = await handler(item, this)
      } catch (err) {
        outcome = { error: err }
      }
      if (outcome?.reject) {
        item.attempts++
        item.state = "rejected"
        item.lastError = outcome.reject
        item.finishedAt = this.now()
        this.write(item)
        this.log.error(`outbox REJECTED ${item.kind} ${item.id} (not retried): ${outcome.reject}`)
        continue
      }
      if (outcome?.defer) {
        item.nextAttemptAt = this.now() + Math.min(outcome.defer, this.config.backoffCapMs)
        if (outcome.deferReason) item.lastError = outcome.deferReason
        this.write(item)
        continue
      }
      item.attempts++
      if (outcome?.error) {
        const err = outcome.error
        item.unknown = item.unknown || err?.unknown === true
        item.lastError = errMsg(err)
        item.nextAttemptAt = this.now() + this.backoff(item.attempts)
        this.write(item)
        const level = item.attempts === 1 || item.attempts % 5 === 0 ? "warn" : "debug"
        this.log[level](
          `outbox ${item.kind} ${item.id} attempt ${item.attempts} failed${item.unknown ? " (outcome unknown)" : ""}: ${item.lastError}`,
        )
        continue
      }
      item.state = "done"
      item.result = outcome?.result ?? null
      item.finishedAt = this.now()
      item.lastError = null
      this.write(item)
      this.log.info(`outbox done ${item.kind} ${item.id}${outcome?.note ? ` (${outcome.note})` : ""}`)
    }
    if (now - this.lastPrune > HOUR) {
      this.lastPrune = now
      for (const item of this.list()) {
        if (item.state !== "pending" && now - (item.finishedAt ?? item.createdAt) > this.config.retainDoneMs) {
          fs.rmSync(this.file(item.id), { force: true })
        }
      }
    }
  }
}

const PERMANENT_HTTP = new Set([400, 401, 403, 404, 405, 413, 415, 422])

function makeHandlers({ ticktick, webhook, config, now }) {
  return {
    async focus(item) {
      const p = item.payload
      if (item.attempts > 0 && item.unknown) {
        const existing = await ticktick.listFocuses(p.start - 5 * MIN, p.end + 5 * MIN)
        const sameSpan = (f) => {
          const fStart = Date.parse(f?.startTime ?? f?.tasks?.[0]?.startTime ?? "")
          const fEnd = Date.parse(f?.endTime ?? f?.tasks?.[0]?.endTime ?? "")
          // TickTick keeps second precision
          return Math.abs(fStart - p.start) < 1500 && Math.abs(fEnd - p.end) < 1500
        }
        const hit = existing.find((f) => String(f?.note ?? "").includes(p.eventId) || sameSpan(f))
        if (hit) return { result: { focusId: hit.id }, note: "found existing" }
      }
      const res = await ticktick.createFocus(p)
      return { result: { focusId: res?.id ?? null } }
    },
    async comment(item) {
      const p = item.payload
      if (item.attempts > 0 && item.unknown) {
        const existing = await ticktick.listComments(p.projectId, p.taskId)
        const hit = existing.find((c) => String(c?.title ?? "").includes(`ref ${p.ref}`))
        if (hit) return { result: { commentId: hit.id }, note: "found existing" }
      }
      const res = await ticktick.addComment(p.projectId, p.taskId, p.text)
      return { result: { commentId: res?.id ?? null } }
    },
    async webhook(item, outbox) {
      const deps = item.deps.map((id) => outbox.read(id)).filter(Boolean)
      const pending = deps.filter((d) => d.state === "pending")
      // Wait for the TickTick projection so the ledger gets its ids, but the
      // ledger is the source of truth: never hold it back for long.
      if (pending.length && now() - item.createdAt < config.depWaitMs) {
        return { defer: Math.max(5_000, Math.min(...pending.map((d) => d.nextAttemptAt)) - now() + 1_000) }
      }
      const focusDeps = deps.filter((d) => d.kind === "focus")
      const commentDep = deps.find((d) => d.kind === "comment")
      const payload = { ...item.payload }
      const focusIds = focusDeps.filter((d) => d.state === "done" && d.result?.focusId).map((d) => d.result.focusId).slice(-50)
      if (payload.intervals) {
        payload.intervals = payload.intervals.map((iv) => {
          const dep = focusDeps.find((d) => d.payload.start === iv._start && d.payload.end === iv._end)
          const out = { start: iv.start, end: iv.end, minutes: iv.minutes }
          if (dep?.state === "done" && dep.result?.focusId) out.focusId = dep.result.focusId
          return out
        })
      }
      payload.ticktick = {
        ...(focusIds.length ? { focusIds } : {}),
        ...(commentDep?.result?.commentId ? { commentId: commentDep.result.commentId } : {}),
        ok: deps.length > 0 && deps.every((d) => d.state === "done"),
      }
      const res = await webhook(payload, payload.eventId)
      // Not configured (no URL/token): not an attempt and not an error. Park
      // the item; it is requeued/sent once the config exists and the plugin
      // restarts. The reason is stored as lastError for the requeue.
      if (res.notConfigured) return { defer: config.backoffCapMs, deferReason: res.error }
      if (!res.ok) {
        const detail = `webhook HTTP ${res.status || res.error}${res.text ? " " + clip(res.text, 120) : ""}`
        // The transform rejected the request itself (auth/shape): resending
        // the same body cannot succeed, so don't retry it.
        if (PERMANENT_HTTP.has(res.status)) return { reject: detail }
        throw new CallError(detail, { status: res.status, unknown: res.unknown === true })
      }
      return { result: { status: res.status, runId: res.json?.runId ?? null, ...(res.via ? { via: res.via } : {}) } }
    },
  }
}

// ─── Core ───────────────────────────────────────────────────────────────

const EXEC_START = new Set(["session.execution.started"])
const EXEC_END = new Set(["session.execution.succeeded", "session.execution.failed", "session.execution.interrupted"])
const HANDLED = new Set([
  ...EXEC_START,
  ...EXEC_END,
  "session.status",
  "session.idle",
  "session.created",
  "session.updated",
  "session.renamed",
  "session.deleted",
  "session.text.ended",
  "session.inbox.enqueued",
  "message.updated",
  "message.part.updated",
])

class Core {
  constructor({ config, log, api, ticktick, webhook, now }) {
    this.config = config
    this.log = log
    this.api = api // { getSession(id), messages(id) }
    this.ticktick = ticktick
    this.now = now ?? (() => Date.now())
    this.webhook = webhook ?? ((payload, key) => postWebhook(config, payload, key))
    this.outbox = new Outbox({
      config,
      log,
      now: this.now,
      handlers: makeHandlers({ ticktick, webhook: this.webhook, config, now: this.now }),
    })
    this.stateLock = new FileLock(`${config.statePath}.lock`, { staleMs: 30_000, log })
    this.state = { version: 2, sessions: {}, tasks: {} }
    this.dirty = new Set()
    this.dirtyTasks = new Set()
    this.timers = new Map()
    this.queue = Promise.resolve()
    this.saveTimer = null
    this.sweepTimer = null
    this.saving = null
    this.disposed = false
    this.refs = 0
    this.seen = new Set()
    this.scanned = new Set()
    this.pendingLinks = new Set()
    this.ready = Promise.resolve()
    this.lastSeen = new Map() // sessionID → last event time (sync, every event)
    this.sweeps = 0
  }

  get me() {
    return { pid: process.pid, host: HOST }
  }

  mine(block) {
    return block?.owner?.pid === process.pid && block?.owner?.host === HOST
  }

  // ── persistence ──
  load() {
    const data = readJsonFile(this.config.statePath)
    if (data?.sessions) this.state = { version: 2, tasks: {}, ...data }
    for (const [id, s] of Object.entries(this.state.sessions)) {
      if (s.link?.inherited) delete s.link // v1 stored copies; links now resolve via the parent chain
      const block = s.block
      if (!block) continue
      if (!block.intervals) {
        // v1 block shape: one span
        block.intervals = block.activeMs > 0 ? [[block.start, block.start + block.activeMs]] : []
        block.lastEventAt ??= s.updatedAt ?? block.end
      }
      if (typeof block.owner === "number") block.owner = { pid: block.owner, host: HOST }
      const owner = block.owner ?? {}
      if (owner.host && owner.host !== HOST) continue
      if (owner.pid !== process.pid && !this.foreignStale(s)) continue // its owner still tracks it
      if (owner.pid !== process.pid) {
        // Its process died: the session's work there has ended. Final flush.
        this.closeInterval(block, block.lastEventAt ?? block.end)
        block.busy = []
        block.owner = this.me
        block.final = true
        this.touch(id)
        this.log.info(`recovered stale block of ${id} from pid ${owner.pid ?? "?"}`)
        this.scheduleFlush(id, 0)
        continue
      }
      if (!block.busy?.length) this.scheduleFlush(id, Math.max(0, (block.end ?? 0) + this.config.idleMs - this.now()))
    }
  }

  touch(id) {
    const s = this.state.sessions[id]
    if (s) s.updatedAt = this.now()
    this.dirty.add(id)
    this.scheduleSave()
  }

  scheduleSave(delay = 1500) {
    if (this.saveTimer || this.disposed) return
    this.saveTimer = setTimeout(() => {
      this.saveTimer = null
      void this.save()
    }, delay)
    this.saveTimer.unref?.()
  }

  // Read-merge-write under the state lock: each process only overwrites the
  // sessions it changed, so concurrent processes don't erase each other.
  async save() {
    if (this.saving) return this.saving
    if (!this.dirty.size && !this.dirtyTasks.size) return
    this.saving = (async () => {
      const dirty = new Set(this.dirty)
      const dirtyTasks = new Set(this.dirtyTasks)
      this.dirty.clear()
      this.dirtyTasks.clear()
      if (!(await this.stateLock.acquire(5_000))) {
        dirty.forEach((id) => this.dirty.add(id))
        dirtyTasks.forEach((id) => this.dirtyTasks.add(id))
        this.log.warn("state lock busy; save postponed")
        this.scheduleSave(5_000)
        return
      }
      try {
        const disk = readJsonFile(this.config.statePath) ?? {}
        disk.version = 2
        disk.sessions ??= {}
        disk.tasks ??= {}
        for (const id of dirty) {
          if (this.state.sessions[id]) disk.sessions[id] = this.state.sessions[id]
          else delete disk.sessions[id]
        }
        for (const id of dirtyTasks) disk.tasks[id] = this.state.tasks[id]
        const cutoff = this.now() - 30 * DAY
        for (const [id, s] of Object.entries(disk.sessions)) {
          if (!s.block && (s.updatedAt ?? 0) < cutoff) delete disk.sessions[id]
        }
        disk.savedAt = this.now()
        writeFileAtomic(this.config.statePath, JSON.stringify(disk, null, 1))
      } catch (err) {
        dirty.forEach((id) => this.dirty.add(id))
        this.log.error(`save state: ${errMsg(err)}`)
      } finally {
        this.stateLock.release()
      }
    })().finally(() => {
      this.saving = null
      if (this.dirty.size || this.dirtyTasks.size) this.scheduleSave()
    })
    return this.saving
  }

  session(id) {
    return (this.state.sessions[id] ??= { totalMs: 0 })
  }

  start() {
    this.outbox.start()
    this.sweepTimer = setInterval(() => {
      this.enqueue(() => this.sweep())
      if (++this.sweeps % 10 === 0) this.enqueue(() => this.adoptStale())
    }, this.config.sweepMs)
    this.sweepTimer.unref?.()
  }

  // ── event intake (serialized) ──
  enqueue(fn) {
    const next = this.queue.then(fn)
    this.queue = next.catch((err) => this.log.error(`event: ${err?.stack ?? err}`))
    return next
  }

  handleEvent(event) {
    if (this.disposed || !event?.type) return
    if (event.id) {
      if (this.seen.has(event.id)) return
      this.seen.add(event.id)
      if (this.seen.size > 5000) {
        for (const id of this.seen) {
          this.seen.delete(id)
          if (this.seen.size <= 4000) break
        }
      }
    }
    const type = event.type
    const p = eventProps(event)
    const at = Number(event.created) || Number(event.time) || this.now()
    const sid = p.sessionID ?? p.info?.id ?? p.part?.sessionID

    if (sid) this.noteActivity(sid, at)
    if (!HANDLED.has(type)) {
      this.log.debugOnce(`type:${type}`, `unhandled event type ${type}`)
      return
    }
    if (EXEC_START.has(type)) return this.enqueue(() => this.onBusy(sid, at))
    if (EXEC_END.has(type)) return this.enqueue(() => this.onIdle(sid, at))
    if (type === "session.status") {
      const status = p.status?.type ?? p.status
      if (status === "busy" || status === "retry") return this.enqueue(() => this.onBusy(sid, at))
      if (status === "idle") return this.enqueue(() => this.onIdle(sid, at))
      return
    }
    if (type === "session.idle") return this.enqueue(() => this.onIdle(sid, at))
    if (type === "session.created" || type === "session.updated") return this.enqueue(() => this.onSessionInfo(p.info ?? p))
    if (type === "session.renamed") {
      return this.enqueue(() => {
        if (sid && typeof p.title === "string") {
          this.session(sid).title = p.title
          this.touch(sid)
        }
      })
    }
    if (type === "session.deleted") return this.enqueue(() => this.onDeleted(sid, at))
    if (type === "session.text.ended") {
      if (sid && typeof p.text === "string" && p.text.trim()) {
        const s = this.session(sid)
        if (s.lastTextMsg === p.assistantMessageID) s.lastText = clip(`${s.lastText ?? ""}\n${p.text}`.trim(), 4000)
        else s.lastText = clip(p.text.trim(), 4000)
        s.lastTextMsg = p.assistantMessageID
        this.touch(sid)
      }
      return
    }
    if (type === "session.inbox.enqueued") {
      const item = p.item ?? {}
      if (item.type !== "synthetic") {
        const text = item.payload?.text ?? item.payload?.prompt?.text ?? item.text
        if (text) this.onUserText(sid, text)
      }
      return
    }
    // V1 shapes: only text explicitly marked as the user's can link.
    const part = p.part
    const role = p.role ?? p.message?.role ?? p.info?.role
    if (part?.type === "text" && part.sessionID && !part.synthetic && role === "user") {
      this.onUserText(part.sessionID, part.text)
    }
  }

  // Cheap, synchronous: keeps the stale-busy detector fed by every event.
  noteActivity(sid, at) {
    if (at > (this.lastSeen.get(sid) ?? 0)) this.lastSeen.set(sid, at)
    const root = this.state.sessions[sid]?.rootID
    const block = root && this.state.sessions[root]?.block
    if (block && at > (block.lastEventAt ?? 0)) block.lastEventAt = at
  }

  // ── session tree ──
  async onSessionInfo(info) {
    if (!info?.id) return
    const s = this.session(info.id)
    if (info.title) s.title = info.title
    const dir = info.directory ?? info.location?.directory
    if (dir) s.directory = dir
    if (info.parentID && s.parentID !== info.parentID) {
      s.parentID = info.parentID
      s.rootID = await this.rootOf(info.parentID)
    } else if (!info.parentID && s.parentID === undefined) {
      s.parentID = null
      s.rootID = info.id
    }
    this.touch(info.id)
  }

  async fetchInfo(id) {
    try {
      return await this.api.getSession(id)
    } catch (err) {
      this.log.debug(`getSession ${id}: ${errMsg(err)}`)
      return null
    }
  }

  async rootOf(id) {
    const chain = []
    let current = id
    for (let depth = 0; depth < 16 && current; depth++) {
      const s = this.session(current)
      if (s.rootID) {
        chain.forEach((c) => (this.session(c).rootID = s.rootID))
        return s.rootID
      }
      if (s.parentID === undefined) {
        const info = await this.fetchInfo(current)
        if (!info) return current // unknown for now; don't persist a guess
        s.parentID = info.parentID ?? null
        if (info.title) s.title = info.title
        const dir = info.directory ?? info.location?.directory
        if (dir) s.directory = dir
        this.touch(current)
      }
      chain.push(current)
      if (!s.parentID) {
        chain.forEach((c) => {
          this.session(c).rootID = current
          this.touch(c)
        })
        return current
      }
      current = s.parentID
    }
    return id
  }

  // Nearest linked ancestor (self, parent, grandparent, …).
  linkOf(id) {
    let current = id
    for (let depth = 0; depth < 16 && current; depth++) {
      const s = this.state.sessions[current]
      if (!s) return null
      if (s.link?.taskId) return s.link
      current = s.parentID
    }
    return null
  }

  async resolveLink(id) {
    await this.rootOf(id)
    return this.linkOf(id)
  }

  // Link used for a root tree's time: the root's, else the most recently
  // linked session inside the tree.
  treeLink(root) {
    const own = this.linkOf(root)
    if (own) return own
    let best = null
    for (const s of Object.values(this.state.sessions)) {
      if (s.rootID === root && s.link?.taskId && (!best || s.link.linkedAt > best.linkedAt)) best = s.link
    }
    return best
  }

  // ── time tracking ──
  closeInterval(block, at) {
    if (!block.busySince) return
    const start = block.busySince
    const end = Math.max(at ?? start, start)
    block.busySince = null
    const last = block.intervals[block.intervals.length - 1]
    if (last && start - last[1] < this.config.mergeGapMs) last[1] = Math.max(last[1], end)
    else block.intervals.push([start, end])
    block.activeMs = block.intervals.reduce((sum, [s, e]) => sum + (e - s), 0)
    block.end = Math.max(block.end ?? end, end)
  }

  async onBusy(sid, at) {
    if (!sid) return
    const root = await this.rootOf(sid)
    const r = this.session(root)
    this.clearFlush(root)
    if (!r.block) {
      const carry = r.carry ?? []
      delete r.carry
      r.block = {
        start: carry[0]?.[0] ?? at,
        end: at,
        intervals: carry,
        activeMs: carry.reduce((sum, [s, e]) => sum + (e - s), 0),
        busy: [],
        busySince: null,
        lastEventAt: at,
        owner: this.me,
      }
    }
    const block = r.block
    block.owner = this.me
    block.lastEventAt = Math.max(block.lastEventAt ?? at, at)
    if (!block.busy.includes(sid)) block.busy.push(sid)
    if (!block.busySince) block.busySince = at
    this.touch(root)
    void this.maybeScan(sid, root)
  }

  async onIdle(sid, at) {
    if (!sid) return
    const root = await this.rootOf(sid)
    const r = this.state.sessions[root]
    const block = r?.block
    if (!block) return
    block.busy = block.busy.filter((id) => id !== sid)
    block.lastEventAt = Math.max(block.lastEventAt ?? at, at)
    if (block.busy.length) return this.touch(root)
    this.closeInterval(block, at)
    this.touch(root)
    this.scheduleFlush(root, this.config.idleMs)
  }

  async onDeleted(sid, at) {
    if (!sid) return
    const s = this.state.sessions[sid]
    if (!s) return
    if (s.rootID && s.rootID !== sid) await this.onIdle(sid, at)
    if (s.block) {
      this.closeInterval(s.block, at)
      s.block.busy = []
      s.block.final = true
      await this.flush(sid)
    } else if (s.carry?.length) {
      await this.flush(sid, { carryOnly: true })
    }
  }

  // A tree marked busy with no events for staleBusyMs lost its end event.
  // A block belongs to the process that recorded it. Another process takes
  // it over only when it is stale: owner pid dead on this host, or no update
  // for staleBusy + idle + 30 min (the owner would have flushed it by then).
  foreignStale(s) {
    const block = s?.block
    if (!block || this.mine(block)) return false
    const owner = block.owner ?? {}
    if (owner.host && owner.host !== HOST) return false
    if (owner.pid && !pidAlive(owner.pid)) return true
    const last = Math.max(s.updatedAt ?? 0, block.lastEventAt ?? 0, block.end ?? 0)
    return this.now() - last > this.config.staleBusyMs + this.config.idleMs + 30 * MIN
  }

  async adoptStale() {
    const disk = readJsonFile(this.config.statePath)
    const candidates = Object.entries(disk?.sessions ?? {}).filter(([, s]) => this.foreignStale(s))
    if (!candidates.length) return
    if (!(await this.stateLock.acquire(5_000))) return
    const adopted = []
    try {
      const fresh = readJsonFile(this.config.statePath) ?? { sessions: {} }
      for (const [id] of candidates) {
        const s = fresh.sessions?.[id]
        if (!this.foreignStale(s)) continue // its owner came back meanwhile
        const block = s.block
        this.log.info(`adopting stale block of ${id} from pid ${block.owner?.pid ?? "?"}`)
        if (!block.intervals) block.intervals = []
        this.closeInterval(block, block.lastEventAt ?? block.end)
        block.busy = []
        block.final = true
        block.owner = this.me
        this.state.sessions[id] = s
        fresh.sessions[id] = s
        adopted.push(id)
      }
      if (adopted.length) writeFileAtomic(this.config.statePath, JSON.stringify(fresh, null, 1))
    } finally {
      this.stateLock.release()
    }
    for (const id of adopted) await this.flush(id)
  }

  sweep() {
    const now = this.now()
    for (const [id, s] of Object.entries(this.state.sessions)) {
      const block = s.block
      if (!block || !this.mine(block) || !block.busy?.length) continue
      const last = Math.max(block.lastEventAt ?? block.busySince ?? now, ...block.busy.map((sid) => this.lastSeen.get(sid) ?? 0))
      block.lastEventAt = last
      if (now - last <= this.config.staleBusyMs) continue
      this.log.warn(`stale busy tree ${id} (no events for ${Math.round((now - last) / MIN)} min); closing at last event`)
      this.closeInterval(block, last)
      block.busy = []
      this.touch(id)
      this.scheduleFlush(id, 0)
    }
    for (const [sid, at] of this.lastSeen) if (now - at > 2 * this.config.staleBusyMs) this.lastSeen.delete(sid)
    if (this.dirty.size) this.scheduleSave()
  }

  clearFlush(root) {
    clearTimeout(this.timers.get(root))
    this.timers.delete(root)
  }

  scheduleFlush(root, delay) {
    this.clearFlush(root)
    if (this.disposed) return
    const timer = setTimeout(() => {
      this.timers.delete(root)
      this.enqueue(() => this.flush(root))
    }, Math.max(0, delay))
    timer.unref?.()
    this.timers.set(root, timer)
  }

  treeActiveMs(root) {
    const r = this.state.sessions[root]
    if (!r) return 0
    let ms = r.totalMs ?? 0
    for (const [s, e] of r.carry ?? []) ms += e - s
    if (r.block) {
      ms += r.block.activeMs ?? 0
      if (r.block.busySince) ms += Math.max(0, this.now() - r.block.busySince)
    }
    return ms
  }

  async summaryFor(id) {
    const s = this.state.sessions[id]
    if (s?.lastText) return s.lastText
    try {
      const messages = await this.api.messages(id)
      for (let i = messages.length - 1; i >= 0; i--) {
        if (messageRole(messages[i]) !== "assistant") continue
        const text = messageText(messages[i]).trim()
        if (text) return text
      }
    } catch {}
    return ""
  }

  async titleFor(id) {
    const info = await this.fetchInfo(id)
    const s = this.session(id)
    if (info?.title) s.title = info.title
    const dir = info?.directory ?? info?.location?.directory
    if (dir) s.directory = dir
    const title = (s.title || "").replace(/^\s*(?:🚧|✅|📣|⏸️?|🎫)\s*/u, "").trim()
    return title || id
  }

  splitChunks(intervals) {
    const chunks = []
    for (const [s, e] of intervals) {
      for (let from = s; from < e; from += this.config.maxChunkMs) {
        chunks.push([from, Math.min(e, from + this.config.maxChunkMs)])
      }
    }
    return chunks
  }

  // Turn a closed block into outbox intents. `final` blocks (session ended,
  // process died, plugin unloaded) are milestones; idle flushes are periodic.
  async flush(root, { carryOnly = false } = {}) {
    const r = this.state.sessions[root]
    if (!r) return
    let block = r.block
    if (carryOnly) block = { intervals: r.carry ?? [], final: true, busy: [] }
    if (!block || block.busy?.length || block.busySince) return
    const intervals = block.intervals ?? []
    const activeMs = intervals.reduce((sum, [s, e]) => sum + (e - s), 0)
    const final = block.final === true || this.disposed
    const link = this.treeLink(root)

    const finish = (why) => {
      r.totalMs = (r.totalMs ?? 0) + activeMs
      r.block = null
      if (carryOnly) delete r.carry
      this.touch(root)
      if (why) this.log.debug(`flush ${root}: ${why}`)
    }
    if (!intervals.length) return finish("empty block")
    if (!link?.taskId || !link.projectId) return finish(`no linked task (${Math.round(activeMs / 1000)}s untracked)`)
    if (activeMs < this.config.minBlockMs && !final) {
      // Too small to log on its own: fold into the next block of this tree.
      r.carry = intervals
      r.block = null
      this.touch(root)
      this.log.debug(`flush ${root}: ${Math.round(activeMs / 1000)}s carried into next block`)
      return
    }

    const title = await this.titleFor(root)
    const base = eventIdOf(this.config.device, root, intervals[0][0])
    const minutes = round1(activeMs / MIN)
    // minutesSpent is per ledger event: the part of this block not already
    // claimed by a report_progress call earlier in it.
    const treeMsAfter = (r.totalMs ?? 0) + activeMs
    const claimed = Math.min(r.claimedMs ?? 0, treeMsAfter)
    const ledgerMinutes = round1(Math.max(0, treeMsAfter - Math.max(claimed, r.totalMs ?? 0)) / MIN)
    const chunks = this.splitChunks(intervals)
    const fresh = !r.lastTextMsg || r.lastTextMsg !== r.flushedTextMsg
    const summary = fresh ? (await this.summaryFor(root)).trim() : ""
    const milestone = final
    const ids = []

    const focusIds = []
    if (this.config.focus) {
      chunks.forEach(([s, e], i) => {
        if (e - s < MIN) return // TickTick focus has minute granularity
        const eventId = eventIdOf(base, `f${i}`)
        const id = `${eventId}`
        this.outbox.enqueue({
          id,
          kind: "focus",
          milestone,
          payload: {
            eventId,
            start: s,
            end: e,
            taskId: link.taskId,
            note: `[agent] OpenCode · ${clip(title, 80)} · session ${root} · device ${this.config.device} · event ${eventId}`,
          },
        })
        focusIds.push(id)
      })
    }
    const commentRef = eventIdOf(base, "c")
    const head = `🤖 OpenCode · ${clip(title, 120)} · ${Math.max(1, Math.round(minutes))} min`
    const footer = `\nref ${commentRef}`
    const room = 1024 - chars(head).length - chars(footer).length - 1
    const commentText = summary ? `${head}\n${clip(summary, room)}${footer}` : `${head}${footer}`
    this.outbox.enqueue({
      id: commentRef,
      kind: "comment",
      milestone,
      payload: { projectId: link.projectId, taskId: link.taskId, ref: commentRef, text: commentText },
    })
    const eventId = eventIdOf(base, final ? "final" : "flush")
    this.outbox.enqueue({
      id: `${eventId}:webhook`,
      kind: "webhook",
      milestone,
      deps: [...focusIds, commentRef],
      payload: {
        v: 2,
        eventId,
        source: "opencode",
        device: this.config.device,
        sessionId: root,
        sessionTitle: title,
        directory: r.directory ?? "",
        taskId: link.taskId,
        projectId: link.projectId,
        ...(link.taskTitle ? { taskTitle: link.taskTitle } : {}),
        status: "progress",
        summary: clip(summary || `${minutes} min of agent work`, 600),
        minutesSpent: ledgerMinutes,
        // The ledger keeps at most 50; minutesSpent still covers every chunk.
        intervals: chunks.slice(-50).map(([s, e]) => ({ start: iso(s), end: iso(e), minutes: round1((e - s) / MIN), _start: s, _end: e })),
        at: iso(block.end ?? intervals[intervals.length - 1][1]),
      },
    })
    ids.push(...focusIds, commentRef, `${eventId}:webhook`)
    r.flushedTextMsg = r.lastTextMsg
    r.claimedMs = treeMsAfter
    finish()
    this.log.info(
      `flush ${root} → task ${link.taskId}: ${minutes} min in ${intervals.length} interval(s)${milestone ? " [milestone]" : ""}`,
    )
    await this.save()
    this.outbox.kick()
    return ids
  }

  // ── linking ──
  onUserText(sid, text) {
    if (!sid || !text) return
    const ref = parseTaskRef(text)
    if (!ref) return
    const current = this.state.sessions[sid]?.link
    if (current?.taskId === ref.taskId) return
    const key = `${sid}|${ref.taskId}`
    if (this.pendingLinks.has(key)) return
    this.pendingLinks.add(key)
    this.enqueue(() => this.link(sid, ref)).finally(() => this.pendingLinks.delete(key))
  }

  // Sessions that predate the plugin (or a restart) are scanned once for a
  // task reference in their user messages.
  async maybeScan(sid, root) {
    for (const id of new Set([root, sid])) {
      if (this.scanned.has(id) || this.linkOf(id)) continue
      this.scanned.add(id)
      const s = this.session(id)
      if (s.scannedAt) continue
      s.scannedAt = this.now()
      this.touch(id)
      try {
        const messages = await this.api.messages(id)
        let ref = null
        for (const m of messages) {
          if (messageRole(m) !== "user") continue
          ref = parseTaskRef(messageText(m)) ?? ref
        }
        if (ref) {
          this.log.info(`scan found task ${ref.taskId} in ${id}`)
          await this.enqueue(() => this.link(id, ref))
        }
      } catch (err) {
        this.log.debug(`scan ${id}: ${errMsg(err)}`)
      }
    }
  }

  async link(sid, ref) {
    const s = this.session(sid)
    const cached = this.state.tasks[ref.taskId]
    s.link = {
      taskId: ref.taskId,
      projectId: ref.projectId ?? cached?.projectId ?? null,
      taskTitle: cached?.title ?? null,
      linkedAt: new Date(this.now()).toISOString(),
    }
    this.touch(sid)
    if (!s.link.taskTitle || !s.link.projectId) {
      try {
        const task = await this.ticktick.getTask(ref.taskId)
        if (task?.id) {
          this.state.tasks[ref.taskId] = { projectId: task.projectId, title: task.title }
          this.dirtyTasks.add(ref.taskId)
          s.link.projectId ??= task.projectId ?? null
          s.link.taskTitle = task.title ?? null
          this.touch(sid)
        }
      } catch (err) {
        this.log.warn(`get_task_by_id ${ref.taskId}: ${errMsg(err)}`)
      }
    }
    this.log.info(`linked ${sid} → task ${s.link.taskId} (${s.link.taskTitle ?? "title unknown"})`)
    return s.link
  }

  // ── tools ──
  async linkTool(sid, value) {
    const ref = parseTaskRef(value, { allowBare: true })
    if (!ref) return "error: pass a TickTick task URL, ticktick:<taskId>, or a 24-hex task id"
    const link = await this.enqueue(() => this.link(sid, ref))
    await this.save()
    if (!link?.projectId) return `linked ${ref.taskId}, but projectId unknown — pass the full task URL`
    return `linked: ${link.taskTitle ?? link.taskId}`
  }

  async reportProgress(sid, { status, summary, taskId }, { waitMs = 12_000 } = {}) {
    const statuses = ["started", "progress", "blocked", "done"]
    if (!statuses.includes(status)) return `error: status must be one of ${statuses.join("|")}`
    let link = await this.resolveLink(sid)
    if (taskId) {
      const ref = parseTaskRef(taskId, { allowBare: true })
      if (!ref) return "error: taskId must be a TickTick task URL or 24-hex id"
      if (ref.taskId !== link?.taskId) link = await this.enqueue(() => this.link(sid, ref))
    }
    if (!link?.taskId) return "error: no TickTick task linked — ask the user for the TickTick task URL, then call link_task"
    if (!link.projectId) return "error: task projectId unknown — call link_task with the full TickTick task URL"

    const text = clip(String(summary ?? "").trim(), 600)
    const root = await this.rootOf(sid)
    const title = await this.titleFor(sid)
    const s = this.session(sid)
    const active = this.treeActiveMs(root)
    const r = this.session(root)
    const since = round1(Math.max(0, active - (r.claimedMs ?? 0)) / MIN)
    const minute = Math.floor(this.now() / MIN)
    const eventId = eventIdOf(this.config.device, sid, "rp", status, hash(`${sid}|${status}|${text}|${minute}`, 12))
    const milestone = status === "done" || status === "blocked"
    const commentRef = eventIdOf(eventId, "c")
    const head = `🤖 OpenCode · ${status} · ${clip(title, 120)} · ${since} min since last report: `
    const footer = `\nref ${commentRef}`
    this.outbox.enqueue({
      id: commentRef,
      kind: "comment",
      milestone,
      payload: {
        projectId: link.projectId,
        taskId: link.taskId,
        ref: commentRef,
        text: head + clip(text, 1024 - chars(head).length - chars(footer).length) + footer,
      },
    })
    const webhookId = `${eventId}:webhook`
    this.outbox.enqueue({
      id: webhookId,
      kind: "webhook",
      milestone,
      deps: [commentRef],
      payload: {
        v: 2,
        eventId,
        source: "opencode",
        device: this.config.device,
        sessionId: sid,
        sessionTitle: title,
        directory: s.directory ?? this.session(root).directory ?? "",
        taskId: link.taskId,
        projectId: link.projectId,
        ...(link.taskTitle ? { taskTitle: link.taskTitle } : {}),
        status,
        summary: text,
        minutesSpent: since,
        at: new Date(this.now()).toISOString(),
      },
    })
    s.lastReport = { at: this.now(), activeMs: active }
    r.claimedMs = Math.max(r.claimedMs ?? 0, active)
    this.touch(root)
    this.touch(sid)
    await this.save()
    try {
      await this.outbox.process({ only: [commentRef, webhookId], waitMs })
    } catch (err) {
      this.log.error(`report_progress processing: ${errMsg(err)}`)
    }
    const c = this.outbox.read(commentRef)
    const w = this.outbox.read(webhookId)
    const tt = c?.state === "done" ? "ticktick ok" : `ticktick queued${c?.lastError ? ` (${clip(c.lastError, 60)})` : ""}`
    const oc =
      w?.state === "done"
        ? `openclaw ${w.result?.status}${w.result?.runId ? " run " + w.result.runId : ""}`
        : `openclaw queued, will retry${milestone ? " until delivered" : " for 24h"}${w?.lastError ? ` (${clip(w.lastError, 60)})` : ""}`
    return `${tt}; ${oc}`
  }

  // ── lifecycle ──
  async dispose({ flush = true } = {}) {
    if (this.disposed) return
    clearInterval(this.sweepTimer)
    if (flush) {
      await this.queue
      const roots = []
      for (const [id, s] of Object.entries(this.state.sessions)) {
        if (!s.block || !this.mine(s.block)) continue
        this.closeInterval(s.block, this.now())
        s.block.busy = []
        s.block.final = true
        this.touch(id)
        roots.push(id)
      }
      for (const id of roots) await this.enqueue(() => this.flush(id)).catch(() => {})
      await Promise.race([this.outbox.process({ waitMs: 2_000 }).catch(() => {}), sleep(6_000)])
    }
    this.disposed = true
    for (const timer of this.timers.values()) clearTimeout(timer)
    this.timers.clear()
    clearTimeout(this.saveTimer)
    this.saveTimer = null
    this.outbox.stop()
    await this.save()
  }
}

// ─── V2 context adapters ────────────────────────────────────────────────

function messageRole(msg) {
  return msg?.info?.role ?? msg?.role ?? msg?.type
}

// V2 context: user {type:"user", text}; assistant {type:"assistant",
// content:[{type:"text", text}, …]}. V1: {info:{role}, parts:[…]}.
function messageText(msg) {
  const parts = msg?.parts ?? msg?.content ?? []
  if (typeof parts === "string") return parts
  const text = (Array.isArray(parts) ? parts : [])
    .filter((p) => p?.type === "text" && typeof p.text === "string" && !p.synthetic)
    .map((p) => p.text)
    .join("\n")
  return text || (typeof msg?.text === "string" ? msg.text : "")
}

const unwrap = (value) =>
  value && typeof value === "object" && !Array.isArray(value) && "data" in value && !("id" in value) ? value.data : value

// Any loaded instance's context can read any session; use whichever is live.
function makeApi(holder) {
  const ctxs = () => [...holder.ctxs]
  return {
    async getSession(id) {
      let lastError = null
      for (const ctx of ctxs()) {
        try {
          if (ctx.session?.get) return unwrap(await ctx.session.get({ sessionID: id }))
          if (ctx.client?.session?.get) return unwrap(await ctx.client.session.get({ path: { id } }))
        } catch (err) {
          lastError = err
        }
      }
      if (lastError) throw lastError
      return null
    },
    async messages(id) {
      for (const ctx of ctxs()) {
        try {
          let messages = null
          if (ctx.session?.context) messages = unwrap(await ctx.session.context({ sessionID: id }))
          else if (ctx.client?.session?.messages) messages = unwrap(await ctx.client.session.messages({ path: { id } }))
          if (Array.isArray(messages)) return messages
        } catch {}
      }
      return []
    },
  }
}

function getCore(config, log) {
  const existing = globalThis[GLOBAL_KEY]
  if (existing && existing.token === MODULE_TOKEN && !existing.core.disposed) return existing
  // Hot reload of a new module version: the old core persists its state
  // (no flush) and this one loads it. Loading is the first item on the
  // event queue, so no event or tool call can run against empty state.
  const handover = existing && !existing.core.disposed ? existing.core.dispose({ flush: false }) : null
  // Contexts of instances still loaded stay usable for session reads.
  const holder = { token: MODULE_TOKEN, ctxs: new Set(existing?.ctxs ?? []), core: null, refs: existing?.refs ?? 0 }
  const mcp = new McpClient(config, log)
  const core = new Core({ config, log, api: makeApi(holder), ticktick: makeTickTick(mcp) })
  holder.core = core
  globalThis[GLOBAL_KEY] = holder
  core.ready = core.enqueue(async () => {
    await Promise.resolve(handover).catch(() => {})
    checkConfigFile(config, log)
    core.load()
    core.start()
    const ready = webhookReadiness(config)
    const hooksHost = URL.canParse(config.hooksUrl || "") ? new URL(config.hooksUrl).host : "(none)"
    log.info(
      `v${VERSION} started: device ${config.device}, idle ${config.idleMs / MIN} min, focus ${config.focus}, ` +
        `hooks ${hooksHost}, cf-access ${Object.keys(cfAccessHeaders(config)).length ? "yes" : "no"}, hub ${config.isHub}, ` +
        `config file ${config.fileConfigFound ? "yes" : `no (${config.fileConfigPath})`}`,
    )
    if (!ready.ok) {
      log.error(`webhook delivery disabled: ${ready.reason}; OpenClaw events stay queued until configured (config file ${config.fileConfigPath})`)
    }
  })
  return holder
}

// The core this process currently runs (may change on hot reload).
const currentCore = () => {
  const h = globalThis[GLOBAL_KEY]
  return h && !h.core.disposed ? h.core : null
}

function subscribe(ctx, signal, log) {
  if (!ctx.event?.subscribe) {
    log.error("ctx.event.subscribe unavailable; time tracking disabled for this location")
    return
  }
  void (async () => {
    let backoff = 1000
    while (!signal.aborted) {
      try {
        for await (const event of ctx.event.subscribe({ signal })) {
          backoff = 1000
          currentCore()?.handleEvent(event)
        }
      } catch (err) {
        if (signal.aborted) break
        log.warn(`event stream: ${errMsg(err)}`)
      }
      if (signal.aborted) break
      await sleep(backoff)
      backoff = Math.min(backoff * 2, 60_000)
    }
  })()
}

const REPORT_DESCRIPTION =
  "Notify the OpenClaw task-manager about this session's linked TickTick task. Call ONLY at real milestones, " +
  "when blocked, or when done — never for routine progress: time and periodic comments are logged to TickTick " +
  "automatically. Posts a TickTick comment and records the event in the OpenClaw ledger; done/blocked are " +
  "retried until delivered."

const LINK_DESCRIPTION =
  "Link this session to a TickTick task (URL like ticktick.com/webapp/#p/<project>/tasks/<task>, or the task id) " +
  "so its time and progress are logged there. Only needed if the user gives the task after the first message."

function checkConfigFile(config, log) {
  try {
    const st = fs.statSync(config.fileConfigPath, { throwIfNoEntry: false })
    if (st && (st.mode & 0o077) !== 0) log.warn(`${config.fileConfigPath} is readable by others; run chmod 600 on it`)
  } catch {}
}

const plugin = {
  id: PLUGIN_ID,
  async setup(ctx) {
    const config = loadConfig()
    const log = makeLogger(config)
    if (config.disabled) {
      log.info("disabled (OPENCLAW_REPORTER_DISABLED / disabled:true)")
      return
    }
    let holder
    try {
      holder = getCore(config, log)
    } catch (err) {
      log.error(`setup: ${err?.stack ?? err}`)
      return
    }
    holder.refs++
    holder.ctxs.add(ctx)
    const abort = new AbortController()
    subscribe(ctx, abort.signal, log)
    // Resolve the live core at call time: a hot reload may have replaced it.
    const live = async () => {
      const core = currentCore()
      if (!core) throw new Error("reporter core not running")
      await core.ready
      return core
    }
    const registrations = []

    try {
      if (ctx.session?.hook) {
        registrations.push(
          await ctx.session.hook("prompt", (event) => {
            try {
              currentCore()?.onUserText(event?.sessionID, event?.prompt?.text)
            } catch (err) {
              log.error(`prompt hook: ${errMsg(err)}`)
            }
          }),
        )
      }
    } catch (err) {
      log.warn(`prompt hook unavailable: ${errMsg(err)}`)
    }

    try {
      registrations.push(
        await ctx.tool.transform((editor) => {
          editor.add({
            name: "report_progress",
            description: REPORT_DESCRIPTION,
            options: { codemode: false },
            input: {
              type: "object",
              properties: {
                status: { type: "string", enum: ["started", "progress", "blocked", "done"] },
                summary: { type: "string", description: "What was achieved / what blocks you (≤600 chars)" },
                taskId: { type: "string", description: "Optional TickTick task URL or id override" },
              },
              required: ["status", "summary"],
              additionalProperties: false,
            },
            async execute(input, context) {
              try {
                const sid = context?.sessionID
                if (!sid) return { content: "error: no session id" }
                return { content: await (await live()).reportProgress(sid, input ?? {}) }
              } catch (err) {
                log.error(`report_progress: ${err?.stack ?? err}`)
                return { content: `error: ${clip(errMsg(err), 160)}` }
              }
            },
          })
          editor.add({
            name: "link_task",
            description: LINK_DESCRIPTION,
            options: { codemode: false },
            input: {
              type: "object",
              properties: { taskUrlOrId: { type: "string" } },
              required: ["taskUrlOrId"],
              additionalProperties: false,
            },
            async execute(input, context) {
              try {
                const sid = context?.sessionID
                if (!sid) return { content: "error: no session id" }
                return { content: await (await live()).linkTool(sid, input?.taskUrlOrId) }
              } catch (err) {
                log.error(`link_task: ${err?.stack ?? err}`)
                return { content: `error: ${clip(errMsg(err), 160)}` }
              }
            },
          })
        }),
      )
    } catch (err) {
      log.error(`tool registration: ${errMsg(err)}`)
    }

    return async () => {
      abort.abort()
      const current = globalThis[GLOBAL_KEY]
      holder.ctxs.delete(ctx)
      current?.ctxs.delete(ctx)
      for (const reg of registrations) {
        try {
          await reg?.dispose?.()
        } catch {}
      }
      // refs count loaded instances in this process; the last one to unload
      // flushes. Handed-over holders carry their count to the new holder.
      if (current) current.refs--
      if (current && current.refs <= 0) {
        const core = current.core
        delete globalThis[GLOBAL_KEY]
        await core.dispose({ flush: true }).catch((err) => log.error(`dispose: ${errMsg(err)}`))
      }
    }
  },
}

// Internals for standalone tests; non-enumerable so the loader never sees it.
Object.defineProperty(plugin, "__test", {
  value: {
    Core,
    Outbox,
    FileLock,
    McpClient,
    CallError,
    makeTickTick,
    makeHandlers,
    postWebhook,
    parseTaskRef,
    eventIdOf,
    loadConfig,
    makeLogger,
  },
})

export default plugin
