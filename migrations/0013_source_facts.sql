CREATE TABLE sporos_source_title (
    source_id BLOB NOT NULL CHECK (length(source_id) = 16),
    source_type TEXT NOT NULL CHECK (source_type IN ('qbittorrent', 'data')),
    normalized_title TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('primary', 'alternate')),
    PRIMARY KEY (source_id, source_type, normalized_title)
) STRICT;

CREATE INDEX sporos_source_title_lookup_idx
    ON sporos_source_title (normalized_title, source_type, source_id);

CREATE TABLE sporos_source_external_id (
    source_id BLOB NOT NULL CHECK (length(source_id) = 16),
    source_type TEXT NOT NULL CHECK (source_type IN ('qbittorrent', 'data')),
    namespace TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (source_id, source_type, namespace, value)
) STRICT;

CREATE INDEX sporos_source_external_id_lookup_idx
    ON sporos_source_external_id (namespace, value, source_type, source_id);

INSERT INTO sporos_source_title (source_id, source_type, normalized_title, kind)
SELECT id, 'qbittorrent', normalized_title, 'primary'
FROM sporos_qbit_torrent
WHERE normalized_title IS NOT NULL;

INSERT INTO sporos_source_title (source_id, source_type, normalized_title, kind)
SELECT id, 'data', normalized_title, 'primary'
FROM sporos_data_source
WHERE normalized_title IS NOT NULL;
