# Set up the Kafka consumer (postgres). 
# We reuse the same ride_deserializer from the previous step. 
# The group_id is different: each consumer group tracks its offsets independently, so the console consumer and the PostgreSQL consumer each read all messages.

from datetime import datetime

import psycopg2
from kafka import KafkaConsumer

from src.models import green_taxi_ride_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-rides-to-postgres',
    value_deserializer=green_taxi_ride_deserializer
)

# Connect to Postgre database
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password='postgres'
)
conn.autocommit = True # means each INSERT is committed immediately - no need to call conn.commit() after every row.
cur = conn.cursor()

# Read messages and insert into PostgreSQL
print(f"Listening to {topic_name} and writing to PostgreSQL...")

count = 0
for message in consumer:
    ride = message.value
    cur.execute(
        """INSERT INTO green_processed_events
           (lpep_pickup_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID, passenger_count, trip_distance, tip_amount, total_amount)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (ride.lpep_pickup_datetime,
         ride.lpep_dropoff_datetime,
         ride.PULocationID,
         ride.DOLocationID,
         ride.passenger_count,
         ride.trip_distance,
         ride.tip_amount,
         ride.total_amount)
    )
    count += 1
    if count % 100 == 0:
        print(f"Inserted {count} rows...")

consumer.close()
cur.close()
conn.close()
