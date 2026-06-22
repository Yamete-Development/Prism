# Redis Stream Contract

Prism acts as a consumer of Redis Streams to process webhook dispatch requests, and a producer of Redis Streams to emit callbacks once processing is complete (or fails). All stream keys are configurable via environment variables — see `.env.example` for the full list.

## Enqueueing Work (Input Stream)

To dispatch messages, push JSON payloads to your configured Redis stream (default: `prism:stream:fast`).

### Payload Schema

```json
{
  "batch_id": "string (unique identifier for this batch)",
  "action": "execute | edit | delete",
  "message_id": "string (optional, used as parent_message_id in callbacks for execute actions)",
  "metadata": {
    "any_key": "any_value"
  },
  "payload": {
    "content": "Hello World",
    "embeds": [],
    "components": [],
    "allowed_mentions": { "parse": [] }
  },
  "targets": [
    {
      "webhook_id": "1234567890",
      "webhook_token": "abc_xyz...",
      "thread_id": "string (optional)",
      "message_id": "string (required if action is edit/delete)",
      "overrides": {
        "content": "Hello <@123456>!",
        "allowed_mentions": { "users": ["123456"] }
      },
      "channel_id": "string",
      "guild_id": "string",
      "connection_id": "string",
      "hub_id": "string"
    }
  ]
}
```

### Supported Actions
- `execute`: Creates a new webhook message (POST)
- `edit`: Edits an existing webhook message (PATCH). Requires `message_id` on each target.
- `delete`: Deletes an existing webhook message (DELETE). Requires `message_id` on each target.

### Overrides Mechanism

Prism takes the base `payload` and does a shallow merge with a target's `overrides` dictionary before executing the HTTP request. This allows you to efficiently send identical messages to most targets while sending slight variations (e.g., custom pings or components) to specific targets within the same batch.

### Wire Format (Key Minification)

To reduce Redis stream memory usage, publishers may minify JSON keys to 1–2 character codes. Prism automatically expands them back to full names before processing. The key mapping is controlled by the `@key_map` in `Prism.FanoutBroadway.KeyExpansion`. If you do not use key minification, set `PRISM_KEY_EXPANSION_ENABLED=false` to skip this step.

| Long key | Short key | Level |
|---|---|---|
| `action` | `a` | root |
| `batch_id` | `b` | root |
| `message_id` | `m` | root / target |
| `shard_index` | `s` | root |
| `hub_id` | `h` | root / target |
| `hub_name` | `n` | root |
| `payload` | `p` | root |
| `targets` | `t` | root |
| `metadata` | `d` | root |
| `trace_headers` | `r` | root |
| `channel_id` | `c` | target |
| `webhook_id` | `w` | target |
| `webhook_token` | `k` | target |
| `guild_id` | `g` | target / metadata |
| `thread_id` | `f` | target |
| `overrides` | `o` | target |
| `connection_id` | `ci` | target |
| `username` | `u` | payload |
| `avatar_url` | `v` | payload |
| `content` | `x` | payload |
| `embeds` | `e` | payload |
| `components` | `q` | payload |
| `allowed_mentions` | `l` | payload |
| `flags` | `fl` | payload |
| `author_id` | `ai` | metadata |
| `guild_name` | `gn` | metadata |
| `badges` | `bg` | metadata |

Short keys that appear at multiple nesting levels (e.g. `m` for `message_id` at root and target level) are safe because JSON keys are scoped to their parent object.

---

## Callbacks (Output Stream)

Once a batch is fully processed (or hits maximum retries), Prism writes a callback event to the configured callback stream (default: `prism:stream:callbacks`).

### Callback Schema

```json
{
  "batch_id": "string (matches input)",
  "action": "execute | edit | delete",
  "status": "success | partial_retry",
  "parent_message_id": "string (matches the message_id passed in the root of the input payload)",
  "message_ids": [
    {
      "webhook_id": "1234567890",
      "message_id": "9876543210",
      "channel_id": "string",
      "guild_id": "string",
      "connection_id": "string",
      "hub_id": "string"
    }
  ],
  "failures": [
    {
      "webhook_id": "1234567890",
      "error": "rate_limited | invalid_webhook | message_not_found | bad_request | server_error | network_error | permanent_error",
      "error_type": "transient | permanent",
      "channel_id": "string",
      "guild_id": "string"
    }
  ]
}
```

### Error Types

| Error | Type | Meaning |
|---|---|---|
| `rate_limited` | transient | Discord returned 429. Prism retries automatically after the `retry_after` delay. |
| `server_error` | transient | Discord returned 5xx. Prism retries with exponential backoff up to the configured max attempts. |
| `network_error` | transient | TCP connection failure, DNS error, or unrecognized 404. Prism retries. |
| `message_not_found` | permanent | Discord returned 10008 (unknown message). The target message was deleted. |
| `invalid_webhook` | permanent | Discord returned 10003 or 10015. The webhook URL is invalid or deleted. |
| `permanent_error` | permanent | Discord returned 401 or 403. The token is invalid or missing permissions. |
| `bad_request` | transient | Discord returned 400. Typically a malformed embed or payload; not retried by default. |

---

## Configurable Stream Keys

All stream keys and Redis identifiers are configurable. See `.env.example` for the full list of environment variables and their defaults.

| Config Key | Env Var | Default |
|---|---|---|
| Fast lane stream | `REDIS_STREAM_FAST` | `prism:stream:fast` |
| Slow lane stream | `REDIS_STREAM_SLOW` | `prism:stream:slow` |
| Retry stream | `REDIS_RETRY_STREAM` | `prism:stream:retries` |
| Callback stream | `REDIS_CALLBACK_STREAM` | `prism:stream:callbacks` |
| Consumer group | `REDIS_GROUP` | `prism:cg:fanout` |
| Delayed queue ZSET | `PRISM_DELAYED_ZSET_KEY` | `prism:delayed` |
| PubSub wakeup channel | `PRISM_PUBSUB_CHANNEL` | `prism:wakeup` |
