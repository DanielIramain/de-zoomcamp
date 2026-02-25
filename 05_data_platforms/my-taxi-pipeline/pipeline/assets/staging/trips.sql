/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  strategy: time_interval
  # TODO: set incremental_key to your event time column (DATE or TIMESTAMP).
  incremental_key: pickup_datetime
  # TODO: choose `date` vs `timestamp` based on the incremental_key type.
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    primary_key: true
  - name: vendor_id
    type: integer
    primary_key: true
  - name: passenger_count
    type: integer
  - name: trip_distance
    type: float
  - name: fare_amount
    type: float
  - name: payment_type_description
    type: string
  - name: taxi_type
    type: string

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: row_count_positive
    description: ensures table is not empty after processing
    query: SELECT COUNT(*) > 0 FROM staging.trips
    value: 1

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)

-- 1. Limpieza y Normalización
-- Manejamos la diferencia de nombres de columnas entre Yellow y Green taxis
WITH normalized_trips AS (
    SELECT
        lpep_pickup_datetime AS pickup_datetime,
        lpep_dropoff_datetime AS dropoff_datetime,
        vendor_id,
        passenger_count,
        trip_distance,
        fare_amount,
        taxi_type
    FROM ingestion.trips
    WHERE 
        -- Filtramos registros con fechas inválidas para el intervalo actual
        pickup_datetime BETWEEN '{{ start_date }}' AND '{{ end_date }}'
        AND vendor_id IS NOT NULL
),

-- 2. Deduplicación mediante Clave Compuesta
-- Dado que no hay ID único, usamos pickup, dropoff, vendor y distance
deduplicated_trips AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY pickup_datetime, dropoff_datetime, vendor_id, trip_distance 
            ORDER BY pickup_datetime
        ) as row_num
    FROM normalized_trips
    QUALIFY row_num = 1
)

-- 3. Enriquecimiento con tabla de búsqueda (JOIN)
-- Unimos con la tabla de semillas para obtener la descripción del pago
SELECT
    t.pickup_datetime,
    t.dropoff_datetime,
    t.vendor_id,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    p.payment_type_name AS payment_type_description,
    t.taxi_type
FROM deduplicated_trips t
LEFT JOIN ingestion.payment_lookup p 
ON t.vendor_id = p.payment_type_id; -- Ajusta según la columna real de tu CSV de pagos