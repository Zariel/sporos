# Notifications

Sporos sends JSON webhook notifications for startup validation, test messages,
and completed save or inject results when `notification_webhook_urls` is set.

Result notifications default to redacted payloads:

```toml
notification_payload_detail = "redacted"
```

Redacted result payloads include the event, workflow source, result, decision,
redaction marker, searchee length, and searchee source type. They omit candidate
names, info hashes, tracker names, client hosts, client categories or tags, and
local filesystem paths.

Use full payloads only for trusted webhook receivers:

```toml
notification_payload_detail = "full"
```

Full result payloads include candidate names, candidate info hashes, tracker
names, searchee client host, searchee info hash, category, tags, and path.
