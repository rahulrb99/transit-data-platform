# Realtime Ingestion

This module will contain GTFS-Realtime polling for trip updates, vehicle positions, and service alerts.

Initial scope:

- Preserve raw protobuf responses under `data/raw/realtime/YYYY-MM-DD/`.
- Record feed metadata and checksums.
- Decode selected entities into PostgreSQL raw tables.
- Add Kafka/Redpanda only after the direct ingestion path works.

