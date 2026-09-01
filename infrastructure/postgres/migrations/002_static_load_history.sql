CREATE TABLE IF NOT EXISTS raw.static_load_history (
    load_id BIGSERIAL PRIMARY KEY,
    activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum_sha256 TEXT NOT NULL,
    archive_path TEXT NOT NULL,
    row_counts JSONB NOT NULL
);
