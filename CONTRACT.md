# Redis Stream Contract

Prism acts as a consumer of Redis Streams to process webhook dispatch requests, and a producer of Redis Streams to emit callbacks once processing is complete (or partially fails).

## Enqueueing Work (Input Stream)

To dispatch messages, push JSON payloads to your configured Redis stream (e.g., `discord:fanout:stream:fast`). 

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
    // The base Discord Webhook API payload. 
    // This is passed directly to Discord.
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
      
      // Target-specific overrides (Optional)
      // These will be deep merged into the base `payload` before sending.
      // Use this to customize content per-webhook (e.g., injecting mentions)
      "overrides": {
        "content": "Hello <@123456>!",
        "allowed_mentions": { "users": ["123456"] }
      },
      
      // Passthrough fields (Optional)
      // Prism ignores these fields, but they are returned in the callback.
      // Useful for correlating results back to your database.
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
Prism acts as a generic router. It takes the base `payload` and does a shallow merge with a target's `overrides` dictionary before executing the HTTP request. This allows you to efficiently send identical messages to 99% of targets while sending slight variations (e.g., custom pings or components) to specific targets within the same batch.

---

## Callbacks (Output Stream)

Once a batch is fully processed (or hits maximum retries), Prism writes a callback event to the configured callback stream (e.g., `discord:fanout:callbacks`).

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
      "message_id": "9876543210 (The Discord message ID created/edited)",
      
      // Passthrough fields returned from the target
      "channel_id": "string",
      "guild_id": "string",
      "connection_id": "string",
      "hub_id": "string"
    }
  ],
  "failures": [
    {
      "webhook_id": "1234567890",
      "error": "rate_limited | invalid_webhook | message_not_found | bad_request | server_error | network_error",
      "error_type": "transient | permanent",
      
      // Passthrough fields returned from the target
      "channel_id": "string",
      "guild_id": "string"
    }
  ]
}
```

### Error Types
- **permanent**: The webhook is deleted, the channel doesn't exist, or the payload is invalid. You should disable or delete this webhook from your database.
- **transient**: Network timeout, Discord API outage, etc. Prism automatically retries transient errors before finally emitting a failure callback.
