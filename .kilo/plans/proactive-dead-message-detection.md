# Proactive Dead Message Detection via `MESSAGE_DELETE`

## Goal

When a broadcast copy is deleted on a target Discord server, proactively set the dead message cache (`dead_msg:{webhook_id}:{message_id}`) so Prism skips future edit/delete HTTP calls instead of hitting 404 first.

Currently, Prism only discovers a broadcast copy is gone when it tries to edit/delete and gets a 404 (code 10008). The dead message cache is populated reactively. This plan makes it proactive.

## Approach

1. **Store reverse mapping** at callback time: `p:d:wh:{broadcast_msg_id} → webhook_id`
2. **Listen for deletions** via `on_raw_message_delete` and `on_raw_bulk_message_delete`
3. **On deletion**: look up the webhook_id → set `dead_msg:{webhook_id}:{msg_id}` with 1800s TTL

No Elixir changes — Prism already checks `dead_msg` keys in `dead_message_cached?`.

## Data Flow

```
Discord target server deletes broadcast copy
        │
        ▼
on_raw_message_delete(message_id=123)
        │
        ▼
Redis GET p:d:wh:123  →  "abc-webhook-id"
        │
        ▼
Redis SETEX dead_msg:abc-webhook-id:123  1800  1
        │
        ▼
Prism: edit/delete for broadcast 123 on webhook abc-webhook-id
sees dead_msg key → skips HTTP call (no 404)
```

## Redis Key Design

| Key | Value | TTL | Set by | Read by |
|-----|-------|-----|--------|---------|
| `p:d:wh:{broadcast_msg_id}` | `webhook_id` | `prism_reply_index_ttl_seconds` (default 604800 = 7d) | Python callback handler | Python delete listener |
| `dead_msg:{webhook_id}:{message_id}` | `"1"` | 1800 (30min) | Python delete listener (new) and Elixir `cache_dead_message` (existing) | Elixir `dead_message_cached?` |

The `p:d:wh:` prefix fits the existing `p:d:` (reply index) namespace. The reverse mapping TTL is long enough to catch late deletions; the dead_msg TTL stays short (30min) as a belt-and-suspenders that self-refreshes via the existing 404 path if needed.

## Changes — Python Bot (`interchat.py`)

### 1. `apps/bot/services/broadcast/prismCallback.py` — Store reverse mapping

In `_handle_execute_callback`, after `_store_reply_index` (line 277), add a step to extract `webhook_id → broadcast_msg_id` mappings from the raw `message_ids` payload and store the reverse:

```python
# Store reverse lookup: broadcast_msg_id → webhook_id for proactive dead message detection
message_ids = payload.get('message_ids', [])
if isinstance(message_ids, list):
    pipe = redis_client.pipeline()
    ttl = constants.prism_reply_index_ttl_seconds
    for entry in message_ids:
        if isinstance(entry, dict):
            msg_id = str(entry.get('message_id', ''))
            webhook_id = str(entry.get('webhook_id', ''))
            if msg_id and msg_id != 'None' and webhook_id and webhook_id != 'unknown':
                pipe.setex(f'p:d:wh:{msg_id}', ttl, webhook_id)
    await pipe.execute()
```

The `message_ids` array in the callback payload already contains `webhook_id` per entry (set by Prism's `FanoutBroadway.process_batch`). `_extract_broadcasts()` drops it — we extract from the raw payload instead.

### 2. `apps/bot/cogs/events/discord/onMessageDelete.py` — Add raw delete listeners

Add two listeners to the existing `OnMessageDelete` cog:

```python
import discord
from discord.ext import commands
from utils.constants import redis_client
from utils.logger import logger

DEAD_MSG_TTL = 1800  # 30 minutes, matches Elixir cache_dead_message default

class OnMessageDelete(CogBase):
    # ... existing on_message_delete for source message auto-delete ...

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        """Proactively cache dead broadcast copies when deleted on target servers."""
        await self._handle_raw_delete(str(payload.message_id))

    @commands.Cog.listener()
    async def on_raw_bulk_message_delete(self, payload: discord.RawBulkMessageDeleteEvent):
        """Handle bulk deletions of broadcast copies."""
        for msg_id in payload.message_ids:
            await self._handle_raw_delete(str(msg_id))

    @staticmethod
    async def _handle_raw_delete(msg_id: str) -> None:
        try:
            webhook_id = await redis_client.get(f'p:d:wh:{msg_id}')
            if webhook_id:
                await redis_client.setex(
                    f'dead_msg:{webhook_id}:{msg_id}',
                    DEAD_MSG_TTL,
                    '1',
                )
                logger.debug(
                    f'Proactively cached dead broadcast copy {msg_id} for webhook {webhook_id}'
                )
        except Exception:
            logger.exception(f'Failed to handle raw delete for message {msg_id}')
```

**Why `raw` events, not `on_message_delete`:**
- Webhook messages may not be in discord.py's internal message cache (they were sent by Prism, not by the bot process)
- `on_raw_message_delete` always fires with just `(message_id, channel_id, guild_id)` — which is all we need
- The existing `on_message_delete` in this cog already skips webhook messages (`if message.webhook_id: return`) because it handles source auto-deletion

## What We Do NOT Change

- **Elixir Prism**: `dead_message_cached?` and `cache_dead_message` already handle the key format. No changes needed.
- **Existing `on_message_delete` listener**: Still handles source message auto-deletion. The webhook guard (`if message.webhook_id: return`) stays — the new raw listeners handle webhook message deletions.
- **Reply index system**: `_store_reply_index` signature unchanged. The reverse mapping is extracted from raw payload separately.
- **Lobby messages**: Out of scope. Lobby is fire-and-forget (no edits). Can add `_handle_lobby_relay_callback` coverage later if needed.

## Edge Cases

| Scenario | Handling |
|---|---|
| Message deleted before callback arrives | No `p:d:wh:{msg_id}` exists → listener does nothing → existing 404 path catches it |
| Channel has two webhooks (primary + secondary) | Each webhook POST returns a unique message_id → no conflict |
| Bulk delete | `on_raw_bulk_message_delete` iterates each message_id |
| Redis unavailable during listener | Try/except logs warning and continues → existing 404 path is the fallback |
| `dead_msg` expires after 30min, user tries edit again | Prism hits 404, re-caches `dead_msg` for another 30min — unchanged behavior |
| `p:d:wh:{msg_id}` TTL shorter than actual deletion | Set to 7 days matching reply index. If someone deletes a year-old message, the 404 path handles it |
| Bot restart | All keys are in Redis — no in-memory state lost |

## Implementation Order

1. **`prismCallback.py`** — Store reverse mapping in `_handle_execute_callback`
2. **`onMessageDelete.py`** — Add `on_raw_message_delete` + `on_raw_bulk_message_delete` + `_handle_raw_delete`

## Validation

- **Unit**: Mock Redis, verify `_handle_raw_delete` sets `dead_msg` key when `p:d:wh:{msg_id}` exists, no-ops when absent
- **Integration**: Send a message through a hub, delete the broadcast copy on the target server, verify `dead_msg:{webhook_id}:{msg_id}` appears in Redis within seconds
- **End-to-end**: After dead_msg is set by listener, trigger an edit of the source message, verify Prism skips the HTTP call for that target (check logs for "known dead in cache")
