ALTER TABLE sporos_qbit_torrent ADD COLUMN normalized_title TEXT;
ALTER TABLE sporos_qbit_torrent ADD COLUMN video_kind TEXT;
ALTER TABLE sporos_qbit_torrent ADD COLUMN release_year INTEGER;
ALTER TABLE sporos_qbit_torrent ADD COLUMN season INTEGER;
ALTER TABLE sporos_qbit_torrent ADD COLUMN episode INTEGER;
ALTER TABLE sporos_qbit_torrent ADD COLUMN episode_end INTEGER;
ALTER TABLE sporos_qbit_torrent ADD COLUMN absolute_episode INTEGER;
ALTER TABLE sporos_qbit_torrent ADD COLUMN air_date TEXT;

CREATE INDEX sporos_qbit_torrent_preflight_idx
    ON sporos_qbit_torrent (
        normalized_title, video_kind, season, episode, available, is_complete, total_size, id
    );
CREATE INDEX sporos_qbit_torrent_preflight_movie_idx
    ON sporos_qbit_torrent (
        normalized_title, release_year, available, is_complete, total_size, id
    );
