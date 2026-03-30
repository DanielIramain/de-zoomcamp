import json
import math
from dataclasses import dataclass

# Define a dataclass for our message. This gives us a clear schema for each taxi trip:
@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tpep_pickup_datetime: int  # epoch milliseconds

@dataclass
class RideGreenTaxi:
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float

def clean_val(val, default=0, is_int=False):
        try:
            # Verificamos si es un valor nulo/NaN
            if val is None or (isinstance(val, float) and math.isnan(val)):
                return default
            return int(val) if is_int else float(val)
        except (ValueError, TypeError):
            return default

# Write a function to convert a DataFrame row into a Ride. 
# We convert the pandas Timestamp to epoch milliseconds
# that's the format Flink expects later
def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        tpep_pickup_datetime=int(row['tpep_pickup_datetime'].timestamp() * 1000),
    )

#columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']

def ride_from_row_green_taxi(row):
    return RideGreenTaxi(
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime']),
        PULocationID=clean_val(row['PULocationID'], is_int=True),
        DOLocationID=clean_val(row['DOLocationID'], is_int=True),
        passenger_count=clean_val(row['passenger_count'], default=1, is_int=True),
        trip_distance=clean_val(row['trip_distance']),
        tip_amount=clean_val(row['tip_amount']),
        total_amount=clean_val(row['total_amount'])
    )

def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)

def green_taxi_ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return RideGreenTaxi(**ride_dict)