import time
import json
import dataclasses

from src.models import ride_from_row_green_taxi

import pandas as pd
from kafka import KafkaProducer

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
df = pd.read_parquet(url, columns=columns)
df.head()

server = 'localhost:9092'
topic_name = 'green-trips' # The broker auto-creates the green-trips topic on first use

# A serializer that handles dataclasses directly
def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')

# The bootstrap_servers is where the broker accepts connections - localhost:9092 
# because we're running this from our laptop (outside Docker). 
# In production with multiple brokers, you'd list several for redundancy - if one is down, the client connects through another.
# Next, connect to Kafka. - we can pass Ride objects directly without converting them to dicts first:
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)
# Send one ride to verify
## ride = ride_from_row(df.iloc[0])
## producer.send(topic_name, value=ride)
## producer.flush()

# Send all 1000 rides (rows) in a loop
t0 = time.time()
count = 0

for _, row in df.iterrows():
    ride = ride_from_row_green_taxi(row)
    producer.send(topic_name, value=ride)
    print(f"Sent: {ride}")
    count += 1
    time.sleep(0.01)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds. Total rides sended: {count}')