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

CREATE TABLE IF NOT EXISTS realtime_vehicle_positions (
    id BIGSERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL,
    feed_timestamp TIMESTAMPTZ,
    entity_id TEXT,
    vehicle_id TEXT,
    vehicle_label TEXT,
    trip_id TEXT,
    route_id TEXT,
    direction_id INTEGER,
    start_time TEXT,
    start_date TEXT,
    stop_id TEXT,
    current_stop_sequence INTEGER,
    current_status TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    bearing DOUBLE PRECISION,
    speed DOUBLE PRECISION,
    occupancy_status TEXT,
    source TEXT NOT NULL
);
