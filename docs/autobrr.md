# autobrr Integration

Use an autobrr external webhook to submit accepted releases to the Sporos
queued announce endpoint. Prefer an external webhook over a webhook action
because external webhooks can check the HTTP status returned by Sporos.

## Sporos endpoint

- Endpoint: `http://sporos:PORT/api/announce`
- Method: `POST`
- Headers: `X-Api-Key=SPOROS_API_KEY`
- Expected status: `202`
- Retry statuses: `500,502,503,504`

If your Sporos build returns `429` when the announce queue is full, include
`429` in the retry statuses.

## Payload

Use this payload in the external webhook data field:

```json
{
  "name": {{ .TorrentName | toJson }},
  "guid": {{ .TorrentUrl | toJson }},
  "link": {{ .TorrentUrl | toJson }},
  "tracker": {{ .IndexerIdentifier | toJson }}
}
```

`guid` is Sporos' stable candidate identity. autobrr does not expose the
original feed or IRC GUID as a simple macro, so use `.TorrentUrl`: it is the
download URL Sporos will fetch and is already the strongest universally
available identity autobrr can provide.

Do not use `.TorrentHash` for this webhook. In autobrr, referencing
`TorrentHash` can force autobrr to download the torrent before sending the
webhook, which defeats the purpose of fast announce handoff.

## Actions

Do not also add a torrent-client action in autobrr for the same filter unless
you intentionally want autobrr and Sporos to act on the same release
independently. Let Sporos own matching, dedupe, download, and injection.

If autobrr requires an action after the external webhook passes, use a no-op or
test action.
