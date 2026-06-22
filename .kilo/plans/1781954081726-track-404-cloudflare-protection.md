# Track 404 Responses to Prevent Cloudflare IP Bans

## Problem

Prism's PATCH (edit) and DELETE requests to Discord webhooks return 404/10008 ("message not found — deleted") frequently. These 404 responses are **not tracked** by the `InvalidRequestTracker` sliding-window counter, which only monitors 401, 403, and non-shared 429 responses. As a result:

1. Each 404 wastes a Cloudflare rate-limit token (Discord explicitly states: *"If a webhook returns a 404 status you should not attempt to use it again — repeated attempts to do so will result in a temporary restriction."*)
2. The existing backpressure mechanism (`unhealthy?()`) never activates because the aggregate 404 volume is invisible to `InvalidRequestTracker`
3. Eventually, the combined volume of legitimate 429s, 401s, 403s, and **unmonitored 404s** triggers a Cloudflare IP-level ban

### Log Evidence

```
11:01:25.965 [info] Webhook_id=1516793691644366849 returned 10008 on patch. Target message not found (deleted).
11:01:25.967 [info] Webhook_id=1516793724535832676 returned 10008 on patch. Target message not found (deleted).
11:01:25.979 [info] Webhook_id=1514825643605622928 returned 10008 on patch. Target message not found (deleted).
... (many more unique webhooks)
```

Each PATCH request is to a different webhook/message — no single message is retried (the current code correctly treats 10008 as permanent). The aggregate volume across all unique requests causes the Cloudflare restriction.

### Root Cause (Upstream)

The Python bot publishes PATCH batches for messages that users have already deleted. Prism cannot know a message is deleted without making the HTTP request. The solution must make Prism resilient to this upstream behavior.

---

## Solution: Three-Layer Defense

### Layer 1: Track 404 in InvalidRequestTracker (Prevent Cloudflare Ban)

Make the existing sliding-window backpressure mechanism aware of 404 wasted requests. When the 404 count pushes the aggregate past the threshold (9,500/10,000 per 10 minutes), `unhealthy?()` returns `true` and all outbound HTTP is gated — preventing the Cloudflare IP ban.

**Files**: `lib/prism/discord_worker.ex`, `lib/prism/rate_limit.ex`

**Changes:**

1. **In `do_http_request_internal`** (discord_worker.ex), add `Prism.RateLimit.InvalidRequestTracker.record_invalid()` calls in the 404 handler (lines ~706-733):

   - **10008 on PATCH** (line 716-720): Call `record_invalid()` before returning `{:error, :message_not_found}`
   - **10008 on DELETE** (line 709-714): Call `record_invalid()` before returning `{:ok, nil}` — the HTTP request is still wasted even though the goal (message deleted) is achieved
   - **10003 / 10015** (line 723-728): Call `record_invalid()` before returning `{:error, :invalid_webhook}` — these are permanently invalid webhooks
   - **Other/unrecognized 404** (line 731-732): Call `record_invalid()` before returning `{:error, :network_error}`

2. **Optionally**, add a 404 clause to `Prism.RateLimit.handle_response/5` (rate_limit.ex) and have the 404 handler delegate to it. This keeps rate-limit tracking centralized. The 404 handler currently pattern-matches only `body`, not `headers` — either capture headers or accept passing `[]` for headers in the 404 case.

---

### Layer 2: Dead Message Cache (Eliminate Duplicate Wasted Requests)

When a PATCH or DELETE returns 404/10008, cache the `{webhook_id, message_id}` combination in Redis. Before making subsequent PATCH/DELETE requests, check the cache — if the message is known-dead, skip the HTTP request entirely.

**Files**: `lib/prism/discord_worker.ex`

**New Redis keys**: `dead_msg:{webhook_id}:{message_id}` with 30-minute TTL

**Changes:**

1. **New private helper** `dead_message_cached?(webhook_id, message_id)`:
   - Redis: `"EXISTS dead_msg:#{webhook_id}:#{message_id}"` 
   - Returns boolean

2. **New private helper** `cache_dead_message(webhook_id, message_id, ttl_seconds \\ 1800)`:
   - Redis: `"SETEX dead_msg:#{webhook_id}:#{message_id} #{ttl_seconds} 1"`

3. **In `process_target`**, for `"edit"` and `"delete"` actions, before `build_request`:
   - If `message_id` is present and `dead_message_cached?(webhook_id, message_id)` returns true:
     - For `"delete"`: return `{:ok, nil}` (message already gone, goal achieved)
     - For `"edit"`: return `{:error, :message_not_found}` (cannot edit deleted message)
     - Log at debug level: `"Skipping PATCH/DELETE for webhook_id=#{webhook_id} message_id=#{message_id} — known dead"`

4. **In `do_http_request_internal`**, for 404/10008 responses:
   - After the existing log line, call `cache_dead_message(webhook_id, message_id)`

**Impact note**: From the current logs, each webhook_id appears to receive only one 10008 (unique messages). Layer 2 prevents a *second* wasted request to the same message+webhook. It protects against future scenarios where retries or duplicate batches target the same message.

---

### Layer 3 (Deferred): Per-Webhook 10008 Circuit Breaker

If a webhook receives N consecutive 10008 errors on PATCH within a time window, temporarily skip all PATCH requests for that webhook. This handles the case where the upstream bot publishes many stale edits for the same webhook in rapid succession.

**Implementation deferred** — deploy Layers 1 and 2 first, monitor impact, then assess whether Layer 3 is needed.

**Design sketch** (for future reference):
- New module `lib/prism/rate_limit/webhook_error_counter.ex` (ETS-based GenServer)
- Threshold: 10 consecutive 10008s within 60 seconds → 5-minute PATCH cooldown for that webhook
- Check in `process_target` before PATCH: skip if webhook is in the penalty box
- Periodic pruning of expired entries via `:timer.send_interval`

---

## Implementation Order

1. **Layer 1** — Track 404 in `InvalidRequestTracker` (highest impact, simplest change)
2. **Layer 2** — Dead message cache (adds preventive capability)
3. **Deploy and monitor** — Observe Cloudflare ban frequency, `InvalidRequestTracker` log lines, and cache hit rates
4. **Assess Layer 3** — Only if Layers 1+2 are insufficient

---

## Files Changed

| File | Layer | Change |
|------|-------|--------|
| `lib/prism/discord_worker.ex` | 1 | Add `record_invalid()` calls in 404 handler |
| `lib/prism/rate_limit.ex` | 1 (optional) | Add 404 clause to `handle_response/5` |
| `lib/prism/discord_worker.ex` | 2 | Add `dead_message_cached?/2`, `cache_dead_message/3` helpers; check cache before PATCH/DELETE |
| `lib/prism/discord_worker.ex` | 2 | Call `cache_dead_message/2` on 404/10008 |

---

## Validation

1. **Layer 1**: Check production logs for `[InvalidRequestTracker]` warning/error lines showing increased counts after deployment. Verify that `unhealthy?()` transitions are triggered by 404 accumulation as well as 429.
2. **Layer 2**: Add optional debug log or metric counting cache hits (`"Skipping ... — known dead"` log lines) vs cache misses.
3. **End-to-end**: Compare Cloudflare block event frequency before vs after deployment. Target: significant reduction or elimination of Cloudflare IP bans caused by 404 accumulation.

---

## Risks

| Risk | Mitigation |
|------|-----------|
| `InvalidRequestTracker` threshold (9,500) triggers too aggressively when 404s are added | Threshold is a module constant `@backpressure_threshold`, tunable without code changes. Monitor real-world 404 rates first |
| Dead message cache adds Redis `EXISTS` call per PATCH/DELETE | Single O(1) Redis call; negligible compared to the Finch HTTP call it replaces (typically 50-200ms) |
| False negatives: message deleted after cache TTL expires (30 min) | After 30 min, the message is stale enough that a new edit attempt is unlikely. If it does occur, the request goes through and gets a fresh 10008 (1 wasted request, not a pattern) |
| 10008 on DELETE is tracked as "invalid" but the operation was technically successful | The Cloudflare limit doesn't distinguish between "successful" and "unsuccessful" HTTP requests — every request counts. Tracking it protects the IP from ban |
