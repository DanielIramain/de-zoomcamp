"""Pipeline for ingesting NYC taxi data from REST API."""
import os

import dlt
import requests

## database path for duckdb, make sure it exists
db_path = "/workspaces/de-zoomcamp/workshop/taxi-pipeline/"
os.makedirs(db_path, exist_ok=True)

@dlt.resource(name="taxi_data", write_disposition="replace")
def taxi_data_resource():
    url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
    page = 1
    while True:
        # get data
        response = requests.get(url, params={"page": page, "page_size": 1000})
        response.raise_for_status()
        data = response.json()

        if not data: # if page empty
            break

        yield data
        page += 1

pipeline = dlt.pipeline(
    pipeline_name='taxi_pipeline',
    destination='duckdb',
    dataset_name='taxi_data',
)

if __name__ == "__main__":
    load_info = pipeline.run(taxi_data_resource())
    
    print(load_info)
