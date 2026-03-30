import json
from datetime import datetime

from src.models import ride_deserializer

from kafka import KafkaConsumer

server = 'localhost:9092'
topic_name = 'rides' # Must match the topic name used by the producer

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='rides-console',
    value_deserializer=ride_deserializer
)

# Read messages and print them. 
# Since value_deserializer returns a Ride, message.value is already a Ride object - no extra conversion needed.
print(f"Listening to {topic_name}...")

count = 0
for message in consumer:
    ride = message.value
    pickup_dt = datetime.fromtimestamp(ride.tpep_pickup_datetime / 1000)
    print(f"Received: PU={ride.PULocationID}, DO={ride.DOLocationID}, "
          f"distance={ride.trip_distance}, amount=${ride.total_amount:.2f}, "
          f"pickup={pickup_dt}")
    count += 1
    if count >= 30:
        print(f"\n... received {count} messages so far (stopping after 30 for demo)")
        break

consumer.close()