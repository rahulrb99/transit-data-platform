CREATE TABLE raw.routes (
    route_id TEXT PRIMARY KEY,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type TEXT NOT NULL
);

CREATE TABLE raw.stops (
    stop_id TEXT PRIMARY KEY,
    stop_code TEXT,
    stop_name TEXT NOT NULL,
    stop_lat TEXT,
    stop_lon TEXT,
    location_type TEXT
);

CREATE TABLE raw.trips (
    route_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    trip_id TEXT PRIMARY KEY,
    trip_headsign TEXT,
    direction_id TEXT,
    shape_id TEXT
);

CREATE TABLE raw.stop_times (
    trip_id TEXT NOT NULL,
    arrival_time TEXT NOT NULL,
    departure_time TEXT NOT NULL,
    stop_id TEXT NOT NULL,
    stop_sequence TEXT NOT NULL
);

INSERT INTO raw.routes VALUES ('CI_ROUTE', 'CI_AGENCY', 'CI', 'CI Route', '3');

INSERT INTO raw.stops VALUES
    ('CI_STOP_1', '1', 'CI Stop 1', '42.3501', '-71.0601', '0'),
    ('CI_STOP_2', '2', 'CI Stop 2', '42.3502', '-71.0602', '0'),
    ('CI_STOP_3', '3', 'CI Stop 3', '42.3503', '-71.0603', '0'),
    ('CI_STOP_4', '4', 'CI Stop 4', '42.3504', '-71.0604', '0'),
    ('CI_STOP_5', '5', 'CI Stop 5', '42.3505', '-71.0605', '0');

INSERT INTO raw.trips VALUES
    ('CI_ROUTE', 'CI_SERVICE', 'CI_TRIP', 'CI Destination', '0', 'CI_SHAPE');

INSERT INTO raw.stop_times VALUES
    ('CI_TRIP', '08:00:00', '08:00:30', 'CI_STOP_1', '1'),
    ('CI_TRIP', '08:05:00', '08:05:30', 'CI_STOP_2', '2'),
    ('CI_TRIP', '08:10:00', '08:10:30', 'CI_STOP_3', '3'),
    ('CI_TRIP', '08:15:00', '08:15:30', 'CI_STOP_4', '4'),
    ('CI_TRIP', '08:20:00', '08:20:30', 'CI_STOP_5', '5');

INSERT INTO realtime_vehicle_positions (
    event_id,
    ingested_at,
    feed_timestamp,
    vehicle_timestamp,
    entity_id,
    vehicle_id,
    vehicle_label,
    trip_id,
    route_id,
    direction_id,
    start_date,
    stop_id,
    current_stop_sequence,
    current_status,
    latitude,
    longitude,
    source
) VALUES
    (
        'ci-event-current',
        '2026-08-30 12:01:02+00',
        '2026-08-30 12:01:01+00',
        '2026-08-30 12:01:00+00',
        'ci-entity-current',
        'CI_VEHICLE',
        'CI Vehicle',
        'CI_TRIP',
        'CI_ROUTE',
        0,
        '20260830',
        'CI_STOP_1',
        1,
        'STOPPED_AT',
        42.3501,
        -71.0601,
        'ci-fixture'
    ),
    (
        'ci-event-target',
        '2026-08-30 12:22:02+00',
        '2026-08-30 12:22:01+00',
        '2026-08-30 12:22:00+00',
        'ci-entity-target',
        'CI_VEHICLE',
        'CI Vehicle',
        'CI_TRIP',
        'CI_ROUTE',
        0,
        '20260830',
        'CI_STOP_5',
        5,
        'STOPPED_AT',
        42.3505,
        -71.0605,
        'ci-fixture'
    );
