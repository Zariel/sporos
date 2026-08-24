# Autobrr integration

If `auth.api_key` (or `SPOROS__AUTH__API_KEY`) is configured, use it for both
requests. Authentication is checked before the request body is admitted. If it
is unset, omit the `Authorization` header; the API is intentionally
unauthenticated.

## External filter

Send `POST /api/v1/autobrr/check` with
`Authorization: Bearer <API key>` (when configured) and this template:

```json
{
  "torrentName": {{ toRawJson .TorrentName }},
  "size": {{ .Size }},
  "indexer": {{ toRawJson .Indexer }}
}
```

`200` accepts a complete or downloading plausible source. `404` rejects the
release. `503` means the qBittorrent inventory has not completed a baseline or
is stale; configure autobrr to retry `503` and `429` when supported.

## Torrent action

Send `POST /api/v1/autobrr/torrents` with the same optional bearer key:

```json
{
  "torrentData": "{{ .TorrentDataRawBytes | toString | b64enc }}",
  "torrentName": {{ toRawJson .TorrentName }},
  "indexer": {{ toRawJson .Indexer }},
  "category": {{ toRawJson .Category }},
  "tags": []
}
```

An HTTP `202` means the candidate and its versioned workflow start were committed
durably. `200` returns the existing task for an identical request. Retrying after
a timeout is safe. Structural torrent failures return `422`, admission bounds
return `413` or `429`, and a durable-write failure returns `507`.

Announcement metadata is never treated as a torrent. Complete matching and all
filesystem or qBittorrent effects happen asynchronously after the torrent bytes
have passed bounded v1/v2/hybrid parsing.
