import time
import json
import dataclasses

from src.models import ride_from_row

import pandas as pd
from kafka import KafkaProducer

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
columns = ['PULocationID', 'DOLocationID', 'trip_distance', 'total_amount', 'tpep_pickup_datetime']
df = pd.read_parquet(url, columns=columns).head(1000)
df.head()

server = 'localhost:9092'
topic_name = 'rides' # The broker auto-creates the rides topic on first use:

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
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    print(f"Sent: {ride}")
    count += 1
    time.sleep(0.01)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds. Total rides sended: {count}')