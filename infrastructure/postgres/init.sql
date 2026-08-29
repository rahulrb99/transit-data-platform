CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.ingestion_metadata (
    ingestion_id BIGSERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL,
    file_name TEXT NOT NULL,
    record_count BIGINT,
    checksum_sha256 TEXT NOT NULL,
    raw_path TEXT NOT NULL
);

