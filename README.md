# opencode-openclaw-reporter (OpenCode V2 plugin)

Logs the work of every OpenCode / OpenChamber session that is linked to a TickTick task. It runs without the model, so it costs no LLM tokens. It also gives the agent a tool to notify the OpenClaw **task-manager** agent.

- **Official history:** the OpenClaw ledger (`/hooks/task-progress`, payload v2).
- **Projection:** TickTick focus records (time) and task comments (progress).

Package: `opencode-openclaw-reporter` (a single `index.js` with no runtime dependencies). Requires an OpenCode release with the V2 plugin API (OpenCode >= 2.4).

## What it does

| Behaviour | How |
|---|---|
| **Link a session to a task** | Paste `https://ticktick.com/webapp/#p/<project>/tasks/<task>` or `ticktick:<taskId>` in a message. The latest reference wins. Sessions started before the plugin loaded are scanned once for a reference. Subagent children and grandchildren inherit the link. The agent can also call `link_task`. |
| **Track time** | Busy time is tracked per root session tree. A parent and child running at the same time count once. Busy spans less than 2 min apart merge into one interval. Idle gaps are never counted. |
| **Automatic updates** | After 10 min idle (`idleMin`), it creates one TickTick focus record per active interval (only intervals of 1 min or more, split into ≤12 h pieces). It adds one comment with the last assistant message and sends one `progress` event to the ledger with the intervals. A block under 1 min carries into the next block. |
| **Agent time tag** | Each focus note starts with `[agent]` and contains `session <id>`, `device <name>` and `event <eventId>`. Search for `[agent]` to find, filter or delete agent time. |
| **`report_progress` tool** | For milestones, blockers and finished work only. It posts a TickTick comment ("N min since last report") and a ledger event. `done` and `blocked` start a task-manager run. |
| **Delivery** | Everything goes through a durable outbox and is retried with backoff (30 s doubling, up to 30 min). Network errors and 5xx retry with the same eventId. A 200 or 204 reply means delivered. 400/401/403/404/413/422 are not retried: the item is marked `rejected` and logged as an ERROR. Periodic updates expire after 24 h, and each expiry is logged as an error. Milestones (`done`/`blocked`, and the final flush when a session is deleted, the plugin unloads, or its process died) retry until they succeed. |
| **Minutes** | `minutesSpent` covers only that report. A `report_progress` call claims the minutes so far, and the next flush reports only the remainder, so ledger minutes add up without overlap. `intervals` (at most 50) describe the focus time of the block. |
| **Ownership** | A block belongs to the process that recorded it. Another process on the same device adopts it, as a final flush, only when the owner pid is dead or the block has had no updates for 2 h 40 min. |
| **Dedupe** | Every intent has a deterministic `eventId`, which is also sent as the `Idempotency-Key` header. If an earlier attempt's outcome is unknown, the plugin first looks for the eventId in TickTick: in focus notes, and in the comment's `ref <eventId>` footer. Only then does it create the record again. |
| **Stuck sessions** | If a tree is marked busy but has had no events for 2 h, its block is closed at the last event. |
| **Several processes** | TUI and OpenChamber server processes on one device share the state and outbox through `O_EXCL` lockfiles. A lock is stale if its holder pid is dead or its heartbeat is old. The same item is never sent twice. |

## Install

1. Add the package to the `plugins` array in `opencode.json` (project root or `~/.config/opencode/opencode.json`). See [Building plugins](https://opencode.ai/docs/build/plugins) and the [server plugin docs](https://opencode.ai/docs/build/plugins/server):
   ```json
   {
     "plugins": ["opencode-openclaw-reporter"]
   }
   ```
   OpenCode installs the package itself. To use a local checkout instead, point `plugins` at the directory, e.g. `"./path/to/opencode-openclaw-reporter"`.
2. Create the config file. **Never paste tokens into a chat or an agent session.** Create the file in your own terminal, or copy it from another machine you control. Start from `openclaw-reporter.example.json` (fake placeholder values) and fill in your own values:
   ```sh
   umask 077
   mkdir -p ~/.config/opencode
   cp openclaw-reporter.example.json ~/.config/opencode/openclaw-reporter.json
   $EDITOR ~/.config/opencode/openclaw-reporter.json
   chmod 600 ~/.config/opencode/openclaw-reporter.json
   ```
   To reuse the same config on another device, copy the file over an authenticated channel you control, without printing it:
   ```sh
   scp <gateway-host>:~/.config/opencode/openclaw-reporter.json ~/.config/opencode/openclaw-reporter.json
   chmod 600 ~/.config/opencode/openclaw-reporter.json
   ```
   Tokens come from your own OpenClaw gateway configuration (`hooks.token`) and MCP hub setup; read them in your own shell, never in an agent session.
   If OpenClaw sits behind Cloudflare Access, also set `cfAccessClientId` / `cfAccessClientSecret` (see the table below).
   **Config file path:** `~/.config/opencode/openclaw-reporter.json` (override with env `OPENCLAW_REPORTER_CONFIG`). It is plain JSON (comments and trailing commas are tolerated). Expect mode `600`: if group/world can read it, the log warns `… is readable by others; run chmod 600 on it`. If it is missing, the start line says `config file no (<path>)`.

   JSON keys (all optional, but `hooksUrl` and `hooksToken` are needed to deliver to OpenClaw):

   | Field | Meaning / default |
   |---|---|
   | `hooksUrl` | Full OpenClaw webhook URL, e.g. `https://<host>/hooks/task-progress`. **No default.** Missing → one `ERROR webhook delivery disabled: no hooks url …` line per start; nothing is sent and events stay queued. |
   | `hooksToken` | Bearer token for the webhook. On the gateway host it is read from `hooks.token` in `~/.openclaw/openclaw.json`. **Required elsewhere.** |
   | `cfAccessClientId`, `cfAccessClientSecret` | Cloudflare Access service token. Sent as `CF-Access-Client-Id` / `CF-Access-Client-Secret` on the webhook and on remote (non-localhost) MCP hub calls, **only when both are set**. |
   | `mcpUrl` | TickTick MCP endpoint. Default: `http://localhost:37373/mcp/ticktick`, then the `hub-tunnel` origin from `opencode.json` + `/mcp/ticktick`. |
   | `mcpAuth` | The `hub-tunnel` Authorization header in `opencode.json`, then `$MCPHUB_TOKEN`, then `~/.config/opencode/secrets/mcphub-token` |
   | `mcpServer` | `ticktick` |
   | `idleMin` | `10` |
   | `focus` | `true`. Set `false` to stop creating focus records; comments and the ledger continue. |
   | `device` | The short hostname |
   | `disabled` | `false` |
   | `stateDir` | `~/.config/opencode/state` |

   Example (fake values; create it with `umask 077`, never paste real tokens into a chat):
   ```json
   {"hooksUrl": "https://openclaw.example.com/hooks/task-progress", "hooksToken": "…",
    "cfAccessClientId": "…", "cfAccessClientSecret": "…"}
   ```

   Env vars take precedence over the file: `OPENCLAW_HOOKS_URL`, `OPENCLAW_HOOKS_TOKEN`, `OPENCLAW_CF_ACCESS_CLIENT_ID`, `OPENCLAW_CF_ACCESS_CLIENT_SECRET`, `OPENCLAW_REPORTER_MCP_URL`, `OPENCLAW_REPORTER_MCP_AUTH`, `OPENCLAW_REPORTER_IDLE_MIN`, `OPENCLAW_REPORTER_FOCUS`, `OPENCLAW_REPORTER_DEVICE`, `OPENCLAW_REPORTER_DISABLED`, `OPENCLAW_REPORTER_DEBUG`.

   A machine counts as the gateway host only if `~/.openclaw/openclaw.json` exists **and** contains `hooks.token` (the value is never logged).
3. Restart OpenCode / OpenChamber once.

## Verify

```sh
tail -n 20 ~/.config/opencode/state/openclaw-task-reporter.log
```
- On load you should see `v2.2.0 started: device <name>, … hooks <host>, cf-access <yes|no>, hub <bool>, config file yes`.
- In a session, paste a task URL. The log shows `linked ses_… → task …`.
- About 10 min after the session goes idle, the log shows `flush …` and then `outbox done focus|comment|webhook …`.

## Kill switch

Use either:
- `OPENCLAW_REPORTER_DISABLED=1` in the environment of the OpenCode process
- `"disabled": true` in `openclaw-reporter.json`

Either one takes effect on the next load. You can also remove the package from `plugins`.

## Files

All files live in `~/.config/opencode/state/` (override with `stateDir`). Do not sync that folder between devices.

| File | Purpose |
|---|---|
| `openclaw-task-reporter.log` | Log (rotates at 2 MB to `.log.1`). Never contains tokens. |
| `openclaw-task-reporter.json` | Sessions, links, open blocks and the task cache |
| `openclaw-task-reporter-outbox/*.json` | One file per intent. `state` is `pending`, `done`, `expired` or `rejected`. `done` and `expired` items are pruned after 7 days. |
| `*.lock` | Lockfiles. They are deleted on release, and stale ones are broken automatically. |

## Troubleshooting

- **`webhook delivery disabled: no hooks url|token` in the log:** fill `hooksUrl` / `hooksToken` in the config file and restart OpenCode. Until then webhook items are parked (no attempt, no expiry spam). On start, webhook items that were `rejected`/`expired` only because of a missing token/url are requeued once (marker `requeuedAt`), and parked ones are made due; delivered items are never resent. Items already pruned after 7 days cannot be recovered.
- **`no MCP auth` in the log:** fill `mcpAuth` in the config file.
- **`… is readable by others`:** run `chmod 600 ~/.config/opencode/openclaw-reporter.json`.
- **`outbox … failed`:** look at `lastError` in the item file. The item retries on its own. `outbox EXPIRED` means a periodic update was dropped after 24 h.
- **`outbox REJECTED webhook … HTTP 401`:** the hooks token is wrong. Rejected items are not retried. Fix the token, then re-queue each one: set `"state":"pending"` in its outbox file. It is resent with the same eventId.
- **5xx / network errors:** they are retried on their own. Milestones never expire.
- **Duplicate TickTick comment:** this is possible only if a comment reached TickTick but TickTick's comment list did not show it yet when the retry checked. Delete it by the `ref` footer.
- **Remove agent time from TickTick:** list focus records whose note starts with `[agent]` (`get_focuses_by_time`), then call `delete_focus`.
- **Debug:** set `OPENCLAW_REPORTER_DEBUG=1`. Unknown event types are logged once.

## Development

```sh
npm test   # unit tests, fake values only, no network access beyond localhost
```

## License

MIT. See [LICENSE](LICENSE).
