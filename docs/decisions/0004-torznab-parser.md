# Streaming Torznab parser

Status: Accepted for Phase 0.

Sporos uses the exactly pinned `quick-xml` pull reader for Torznab responses.
The adapter supplies a byte-limited reader, and the parser maintains only its
event buffer and the current item before emitting a result to the caller.

The prototype enforces a depth limit of 32, a 16 KiB limit for each retained
field, and a caller-supplied result cap. It rejects DTDs, processing
instructions, and non-predefined entity references, so neither external nor
custom entity expansion is available. Character references and XML's five
predefined entities remain supported.

Items without a title or download URL, with an invalid size, or with an
oversized field are skipped independently. Malformed XML outside an item
rejects the response. The production search activity must stage emitted items
under its search-attempt identity so a later structural error cannot publish a
partial result set.
