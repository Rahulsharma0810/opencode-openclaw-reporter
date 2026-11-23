import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import http from "node:http"
import assert from "node:assert/strict"
import { fileURLToPath } from "node:url"

// Usage: npm test   (fake values only; scratch files live in a temp dir)
const T = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-reporter-test-"))
const HOME = path.join(T, "home")
fs.mkdirSync(path.join(HOME, ".openclaw"), { recursive: true })
process.env.HOME = HOME
const PLUGIN = process.argv[2] || fileURLToPath(new URL("../index.js", import.meta.url))
const { default: plugin } = await import(PLUGIN)
const { loadConfig, makeLogger, Outbox, makeHandlers, postWebhook, McpClient } = plugin.__test

const FAKE = { token: "FAKE-hooks-token-123", id: "FAKE-cf-id-456", secret: "FAKE-cf-secret-789" }
let hits = []
const mk = () => {
  hits = []
  return new Promise((res) => {
    const srv = http.createServer((req, rsp) => {
      req.resume()
      req.on("end", () => {
        hits.push(req.headers)
        rsp.statusCode = 204
        rsp.end()
      })
    })
    srv.listen(0, "0.0.0.0", () => res(srv))
  })
}
let n = 0
const ok = (name) => console.log(`ok ${++n} ${name}`)
const cfgFor = (sub, env = {}) => {
  const dir = path.join(T, "run-" + sub)
  fs.mkdirSync(dir, { recursive: true })
  return loadConfig({
    OPENCLAW_REPORTER_CONFIG: path.join(T, "none.json"),
    OPENCLAW_REPORTER_STATE_DIR: dir,
    OPENCLAW_REPORTER_BACKOFF_BASE_MS: "10",
    ...env,
  })
}

const srv = await mk()
const port = srv.address().port
const url = `http://127.0.0.1:${port}/hooks/task-progress`

// 1 headers
let c = cfgFor("t1", {
  OPENCLAW_HOOKS_URL: url,
  OPENCLAW_HOOKS_TOKEN: FAKE.token,
  OPENCLAW_CF_ACCESS_CLIENT_ID: FAKE.id,
  OPENCLAW_CF_ACCESS_CLIENT_SECRET: FAKE.secret,
})
let r = await postWebhook(c, { a: 1 }, "k1")
assert.equal(r.ok, true)
assert.equal(hits[0]["cf-access-client-id"], FAKE.id)
assert.equal(hits[0]["cf-access-client-secret"], FAKE.secret)
assert.equal(hits[0].authorization, `Bearer ${FAKE.token}`)
assert.equal(hits[0]["idempotency-key"], "k1")
ok("CF + Authorization headers sent")

// 2 only one CF part -> none
hits = []
c = cfgFor("t2", { OPENCLAW_HOOKS_URL: url, OPENCLAW_HOOKS_TOKEN: FAKE.token, OPENCLAW_CF_ACCESS_CLIENT_ID: FAKE.id })
await postWebhook(c, {}, "k2")
assert.equal(hits.length, 1)
assert.equal(hits[0]["cf-access-client-id"], undefined)
assert.equal(hits[0]["cf-access-client-secret"], undefined)
ok("CF headers omitted unless both set")

// config-file keys
const f = path.join(T, "reporter.json")
fs.writeFileSync(
  f,
  JSON.stringify({ hooksUrl: url, hooksToken: FAKE.token, cfAccessClientId: FAKE.id, cfAccessClientSecret: FAKE.secret }),
  { mode: 0o600 },
)
c = loadConfig({ OPENCLAW_REPORTER_CONFIG: f, OPENCLAW_REPORTER_STATE_DIR: path.join(T, "run-t3") })
assert.equal(c.hooksUrl, url)
assert.equal(c.cfAccessClientId, FAKE.id)
assert.equal(c.cfAccessClientSecret, FAKE.secret)
assert.equal(c.fileConfigFound, true)
ok("config file keys parsed")

// 3 missing hooksUrl -> nothing sent
hits = []
c = cfgFor("t4", { OPENCLAW_HOOKS_TOKEN: FAKE.token })
assert.equal(c.hooksUrl, "")
assert.equal(c.isHub, false)
r = await postWebhook(c, {}, "k3")
assert.equal(r.notConfigured, true)
assert.equal(hits.length, 0)
ok("nothing sent when hooksUrl missing")

// isHub requires hooks.token
fs.writeFileSync(path.join(HOME, ".openclaw/openclaw.json"), JSON.stringify({ gateway: {} }))
assert.equal(cfgFor("t5").isHub, false)
fs.writeFileSync(path.join(HOME, ".openclaw/openclaw.json"), JSON.stringify({ hooks: { token: FAKE.token } }))
assert.equal(cfgFor("t5").isHub, true)
fs.rmSync(path.join(HOME, ".openclaw/openclaw.json"))
ok("isHub needs hooks.token")

// 4 outbox: park when unconfigured, requeue when configured
const mkOutbox = (cfg) => {
  const log = makeLogger(cfg)
  const handlers = makeHandlers({ ticktick: {}, webhook: (p, k) => postWebhook(cfg, p, k), config: cfg, now: () => Date.now() })
  return new Outbox({ config: cfg, log, handlers })
}
c = cfgFor("t6", { OPENCLAW_HOOKS_TOKEN: FAKE.token }) // no url
let ob = mkOutbox(c)
ob.enqueue({ id: "wh-new", kind: "webhook", payload: { eventId: "wh-new" }, milestone: false })
hits = []
await ob.process()
const it = ob.read("wh-new")
assert.equal(it.state, "pending")
assert.equal(it.attempts, 0)
assert.match(it.lastError, /^no hooks url/)
assert.equal(hits.length, 0)
ok("unconfigured webhook parked, no attempt, nothing sent")

fs.mkdirSync(ob.dir, { recursive: true })
const base = { kind: "webhook", payload: {}, deps: [], milestone: false, unknown: false, attempts: 39, createdAt: 1, nextAttemptAt: 1, result: null }
const seed = (id, o) =>
  fs.writeFileSync(ob.file(id), JSON.stringify({ ...base, finishedAt: Date.now(), id, payload: { eventId: id }, ...o }))
seed("exp-token", { state: "expired", expiresAt: 2, lastError: "webhook HTTP no hooks token (set hooksToken in openclaw-reporter.json)" })
seed("rej-token", { state: "rejected", lastError: "webhook HTTP no hooks token (x)" })
seed("rej-401", { state: "rejected", lastError: "webhook HTTP 401 nope" })
seed("exp-other", { state: "expired", expiresAt: 2, lastError: "webhook HTTP 500" })
seed("done-1", { state: "done", lastError: null, result: { status: 204 } })
seed("done-tok", { state: "done", lastError: "webhook HTTP no hooks token" })
assert.equal(ob.requeueMissingConfig(), 0) // still unconfigured
ok("no requeue while unconfigured")

c = cfgFor("t6", {
  OPENCLAW_HOOKS_URL: url,
  OPENCLAW_HOOKS_TOKEN: FAKE.token,
  OPENCLAW_CF_ACCESS_CLIENT_ID: FAKE.id,
  OPENCLAW_CF_ACCESS_CLIENT_SECRET: FAKE.secret,
})
ob = mkOutbox(c)
assert.equal(ob.requeueMissingConfig(), 3) // 2 revived + 1 parked made due
assert.equal(ob.requeueMissingConfig(), 0) // idempotent
assert.equal(ob.read("exp-token").state, "pending")
assert.equal(ob.read("rej-token").state, "pending")
assert.equal(ob.read("rej-401").state, "rejected")
assert.equal(ob.read("exp-other").state, "expired")
assert.equal(ob.read("done-1").state, "done")
assert.equal(ob.read("done-tok").state, "done")
ok("requeue only token/url failures, once, done untouched")

hits = []
await ob.process()
assert.equal(hits.length, 3)
for (const id of ["exp-token", "rej-token", "wh-new"]) assert.equal(ob.read(id).state, "done", id)
assert.equal(ob.read("done-1").attempts, 39)
hits = []
assert.equal(ob.requeueMissingConfig(), 0)
await ob.process()
assert.equal(hits.length, 0)
ok("requeued items delivered once; delivered items not resent")

// 5 MCP client CF headers on a non-loopback address
const lan = Object.values(os.networkInterfaces()).flat().find((i) => i && i.family === "IPv4" && !i.internal)
if (lan) {
  hits = []
  const mc = new McpClient(c, makeLogger(c))
  mc.auth = "Bearer FAKE-mcp"
  await mc.post(`http://${lan.address}:${port}/mcp/ticktick`, { x: 1 }, 3000)
  assert.equal(hits[0]["cf-access-client-id"], FAKE.id)
  assert.equal(hits[0]["cf-access-client-secret"], FAKE.secret)
  hits = []
  await mc.post(`http://127.0.0.1:${port}/mcp/ticktick`, { x: 1 }, 3000)
  assert.equal(hits[0]["cf-access-client-id"], undefined)
  ok("MCP: CF headers on remote, not on loopback")
} else console.log("skip MCP LAN test (no non-internal IPv4)")

srv.close()
// 6 no secret in any log
let logs = ""
for (const d of fs.readdirSync(T)) {
  const p = path.join(T, d)
  if (d.startsWith("run-") && fs.statSync(p).isDirectory())
    for (const x of fs.readdirSync(p)) if (x.endsWith(".log")) logs += fs.readFileSync(path.join(p, x), "utf8")
}
assert.ok(logs.length > 0, "log empty?")
for (const v of Object.values(FAKE)) assert.ok(!logs.includes(v), "secret leaked: " + v.slice(0, 8))
ok(`no secret value in logs (${logs.length} bytes)`)
console.log("ALL PASSED")
fs.rmSync(T, { recursive: true, force: true })
process.exit(0)
