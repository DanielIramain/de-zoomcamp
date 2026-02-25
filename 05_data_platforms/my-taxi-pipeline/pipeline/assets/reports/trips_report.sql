/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: time_interval
  # TODO: set to your report's date column
  incremental_key: pickup_date
  # TODO: set to `date` or `timestamp`
  time_granularity: timestamp

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: pickup_date
    type: date
    description: "Fecha en la que ocurrió el viaje"
    primary_key: true
  - name: taxi_type
    type: string
    description: "Tipo de taxi (green o yellow)"
    primary_key: true
  - name: payment_type_description
    type: string
    description: "Descripción del método de pago utilizado"
    primary_key: true
  - name: total_trips
    type: bigint
    description: "Número total de viajes realizados"
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: float
    description: "Suma total de las tarifas de los viajes"
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: float
    description: "Distancia promedio recorrida por viaje"

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

-- Propósito: Agregar datos de staging para dashboards y analítica
-- Filtramos por el intervalo de tiempo para permitir ejecuciones incrementales
SELECT
    -- Truncamos la fecha para agrupar por día
    CAST(pickup_datetime AS DATE) AS pickup_date,
    taxi_type,
    payment_type_description,
    
    -- Métricas agregadas
    COUNT(*) AS total_trips,
    SUM(fare_amount) AS total_fare_amount,
    AVG(trip_distance) AS avg_trip_distance
FROM staging.trips

WHERE 
    -- Filtros dinámicos de Bruin para procesamiento incremental
    pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'

GROUP BY 
    pickup_date,
    taxi_type,
    payment_type_description