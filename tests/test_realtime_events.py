from datetime import UTC, datetime

from ingestion.realtime.consumer import validate_event
from ingestion.realtime.vehicle_positions import (
    VehiclePositionRecord,
    event_to_record,
    record_to_event,
)


def test_vehicle_position_event_round_trip() -> None:
    record = VehiclePositionRecord(
        event_id="event-1",
        ingested_at=datetime(2026, 8, 29, 17, 0, tzinfo=UTC),
        feed_timestamp=datetime(2026, 8, 29, 16, 59, 58, tzinfo=UTC),
        vehicle_timestamp=datetime(2026, 8, 29, 16, 59, 59, tzinfo=UTC),
        entity_id="y1234",
        vehicle_id="y1234",
        vehicle_label="1234",
        trip_id="trip-1",
        route_id="7",
        direction_id=1,
        start_time="13:00:00",
        start_date="20260829",
        stop_id="stop-1",
        current_stop_sequence=4,
        current_status="IN_TRANSIT_TO",
        latitude=42.351,
        longitude=-71.067,
        bearing=90.0,
        speed=8.5,
        occupancy_status="MANY_SEATS_AVAILABLE",
        source="https://cdn.mbta.com/realtime/VehiclePositions.pb",
    )

    event = record_to_event(record)
    validate_event(event)
    round_tripped = event_to_record(event)

    assert round_tripped == record


def test_validate_event_rejects_missing_required_fields() -> None:
    try:
        validate_event({"event_id": "event-1"})
    except ValueError as error:
        assert "ingested_at" in str(error)
    else:
        raise AssertionError("validate_event should reject incomplete events")
