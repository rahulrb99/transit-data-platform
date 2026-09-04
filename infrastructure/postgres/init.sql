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
    event_id TEXT,
    ingested_at TIMESTAMPTZ NOT NULL,
    feed_timestamp TIMESTAMPTZ,
    vehicle_timestamp TIMESTAMPTZ,
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_realtime_vehicle_positions_event_id
    ON realtime_vehicle_positions (event_id);

CREATE INDEX IF NOT EXISTS idx_realtime_vehicle_positions_ingested_at
    ON realtime_vehicle_positions (ingested_at);

CREATE INDEX IF NOT EXISTS idx_realtime_vehicle_positions_trip_sequence_time
    ON realtime_vehicle_positions (trip_id, current_stop_sequence, vehicle_timestamp, feed_timestamp, ingested_at);

CREATE INDEX IF NOT EXISTS idx_realtime_vehicle_positions_vehicle_trip_sequence
    ON realtime_vehicle_positions (vehicle_id, trip_id, current_stop_sequence);

CREATE TABLE IF NOT EXISTS realtime_vehicle_position_dead_letters (
    dead_letter_id BIGSERIAL PRIMARY KEY,
    original_event_id TEXT,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    error_type TEXT NOT NULL,
    error_reason TEXT NOT NULL,
    raw_payload BYTEA NOT NULL,
    vehicle_id TEXT,
    trip_id TEXT,
    CONSTRAINT uq_realtime_vehicle_position_dead_letter_record
        UNIQUE (topic, partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_realtime_vehicle_position_dead_letters_processed_at
    ON realtime_vehicle_position_dead_letters (processed_at DESC);

CREATE TABLE IF NOT EXISTS realtime_pipeline_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    metric_timestamp TIMESTAMPTZ NOT NULL,
    metric_kind TEXT NOT NULL,
    topic TEXT NOT NULL,
    batch_size INTEGER NOT NULL,
    events_received INTEGER NOT NULL,
    events_persisted INTEGER NOT NULL,
    duplicate_events INTEGER NOT NULL,
    dead_letter_events INTEGER NOT NULL,
    error_count INTEGER NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    failed_batch_count INTEGER NOT NULL DEFAULT 0,
    processing_duration_seconds DOUBLE PRECISION NOT NULL,
    latest_source_event_timestamp TIMESTAMPTZ,
    latest_ingestion_timestamp TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_realtime_pipeline_metrics_timestamp
    ON realtime_pipeline_metrics (metric_timestamp DESC);
