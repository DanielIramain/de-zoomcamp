"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

#columns:
#  - name: vendor_id
#    type: BIGINT
#  - name: lpep_pickup_datetime
#    type: TIMESTAMP
#  - name: lpep_dropoff_datetime
#    type: TIMESTAMP
#  - name: store_and_fwd_flag
#    type: VARCHAR
#  - name: ratecode_id
#    type: DOUBLE
#  - name: pu_location_id
#    type: BIGINT
#  - name: do_location_id
#    type: BIGINT
#  - name: passenger_count
#    type: DOUBLE
#  - name: trip_distance
#    type: DOUBLE
#  - name: fare_amount
#    type: DOUBLE
#  - name: extra
#    type: DOUBLE
#  - name: mta_tax
#    type: DOUBLE
#  - name: tip_amount
#    type: DOUBLE
#  - name: tolls_amount
#    type: DOUBLE
#  - name: ehail_fee
#    type: DOUBLE
#  - name: improvement_surcharge
#    type: DOUBLE
#  - name: total_amount
#    type: DOUBLE
#  - name: payment_type
#    type: DOUBLE
#  - name: trip_type
#    type: DOUBLE
#  - name: congestion_surcharge
#    type: DOUBLE
#  - name: taxi_type
#    type: VARCHAR

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import pandas as pd
import requests
import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.

def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    # return final_dataframe
    
    # 1. Get Bruin variables
    # BRUIN_START_DATE y BRUIN_END_DATE are automatically inyected by the CLI
    start_date_str = os.getenv("BRUIN_START_DATE")
    end_date_str = os.getenv("BRUIN_END_DATE")
    
    # taxi_types defined in pipeline.yml
    # i.e ["green", "yellow"] or ["green"]]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    all_data = []
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    # 2. Iterate for taxi type and by month inside the range
    current_date = start_date
    while current_date <= end_date:
        year_month = current_date.strftime("%Y-%m")
        
        for taxi in taxi_types:
            file_name = f"{taxi}_tripdata_{year_month}.parquet"
            url = f"{base_url}{file_name}"
            
            print(f"Downloading...: {url}")
            try:
                # Download .parquet directly to a DataFrame
                df = pd.read_parquet(url)
                
                # Add metadata column for taxi type
                df['taxi_type'] = taxi
                
                # add data
                all_data.append(df)
                
            except Exception as e:
                print(f"error downloading {file_name}: {e}")

        # Next month
        current_date += relativedelta(months=1)

    # 3. Consolidate and return the data for Bruin ingestion
    if not all_data:
        print(f"Data not found in range {start_date_str} to {end_date_str}.")
        return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)
    return final_df

if __name__ == "__main__":
    materialize()
