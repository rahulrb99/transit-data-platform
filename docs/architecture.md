# Architecture

```mermaid
flowchart TD
    A[MBTA GTFS Static ZIP] --> B[Python Static Ingestion]
    C[MBTA GTFS-Realtime API] --> D[Python Realtime Ingestion]
    B --> E[Raw File Storage]
    D --> E
    E --> F[Ingestion Metadata]
    B --> G[(PostgreSQL Raw Schema)]
    D --> G
    D --> M[Redpanda vehicle_positions Topic]
    M --> N[Python Consumer]
    N --> G
    G --> H[dbt Staging Models]
    H --> I[dbt Intermediate Models]
    I --> J[dbt Mart Models]
    I --> O[ML Feature Dataset]
    O --> P[Delay Training Mart]
    J --> K[Streamlit Dashboard]
    L[Airflow DAGs] --> B
    L --> D
    L --> H
```

## Design Notes

- Raw GTFS files are preserved before transformation so pipeline outputs can be reproduced.
- PostgreSQL is used as the local warehouse for raw and modeled data.
- dbt owns analytical transformations, tests, and documentation.
- Airflow coordinates scheduled ingestion and transformation work.
- Realtime ingestion starts as direct polling; Kafka or Redpanda can be added after the core data path works.
- The ML training mart is produced in dbt from realtime observations joined to static GTFS schedule data.
- Weather is excluded from the first ML dataset until the transit-only baseline is validated.
