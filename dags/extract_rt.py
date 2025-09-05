import requests
from google.transit import gtfs_realtime_pb2
import pandas as pd
from datetime import datetime
from pathlib import Path
from collections import Counter

# Répertoires
DATA_DIR = Path(__file__).parent / "data"  # /opt/airflow/dags/data
DATA_DIR.mkdir(exist_ok=True)  # crée le dossier si jamais il n'existe pas

# URLs GTFS-RT
URL_VEHICLE = "https://ara-api.enroute.mobi/rla/gtfs/vehicle-positions"
URL_TRIPUP = "https://ara-api.enroute.mobi/rla/gtfs/trip-updates"


def extract_vehicle_positions():
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(URL_VEHICLE)
    feed.ParseFromString(response.content)

    rows = []
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v = entity.vehicle
            rows.append(
                {
                    "vehicle_id": v.vehicle.id,
                    "trip_id": v.trip.trip_id,
                    "route_id": v.trip.route_id,
                    "latitude": v.position.latitude,
                    "longitude": v.position.longitude,
                    "bearing": v.position.bearing,
                    "speed": v.position.speed,
                    "timestamp": datetime.fromtimestamp(v.timestamp),
                }
            )

    df = pd.DataFrame(rows)
    df.to_csv(DATA_DIR / "rt_vehicle.csv", index=False)
    print(f" Saved {len(df)} vehicle positions to data/rt_vehicle.csv")


def extract_trip_updates():
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(URL_TRIPUP)
    feed.ParseFromString(response.content)

    rows = []
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            t = entity.trip_update
            for stop in t.stop_time_update:
                rows.append(
                    {
                        "trip_id": t.trip.trip_id,
                        "route_id": t.trip.route_id,
                        "stop_id": stop.stop_id,
                        "arrival_delay": (
                            stop.arrival.delay if stop.HasField("arrival") else None
                        ),
                        "departure_delay": (
                            stop.departure.delay if stop.HasField("departure") else None
                        ),
                        "event_ts": datetime.fromtimestamp(t.timestamp),
                    }
                )

    df = pd.DataFrame(rows)
    df.to_csv(DATA_DIR / "rt_trips.csv", index=False)
    print(f" Saved {len(df)} trip updates to data/rt_trips.csv")


def count_late_trips() -> int:

    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(URL_TRIPUP)
    feed.ParseFromString(response.content)

    late_count = 0
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            for stop_time_update in entity.trip_update.stop_time_update:
                if stop_time_update.HasField(
                    "arrival"
                ) and stop_time_update.arrival.HasField("delay"):
                    if stop_time_update.arrival.delay > 0:  # strictly positive = late
                        late_count += 1
                # Check departure delay
                if stop_time_update.HasField(
                    "departure"
                ) and stop_time_update.departure.HasField("delay"):
                    if stop_time_update.departure.delay > 0:
                        late_count += 1

    return late_count


def top_busiest_stops(gtfs_url=URL_TRIPUP, top_n=5):
    # Count how many times each stop_id appears in trip updates,
    # return the top N busiest stops.

    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(gtfs_url)
    feed.ParseFromString(response.content)

    stop_ids = []
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            for stop in entity.trip_update.stop_time_update:
                stop_ids.append(stop.stop_id)

    if not stop_ids:
        print("No stops found in this snapshot.")
        return []

    counter = Counter(stop_ids)
    top_stops = counter.most_common(top_n)

    # Turn into DataFrame for possible export
    df = pd.DataFrame(top_stops, columns=["stop_id", "count"])
    df.to_csv("data/top_stops.csv", index=False)

    print(f"Top {top_n} busiest stops:")
    for stop, count in top_stops:
        print(f"  {stop}: {count} updates")

    return top_stops


if __name__ == "__main__":  # This is the entry point.
    extract_vehicle_positions()
    extract_trip_updates()
    late_trips = count_late_trips()
    top_busiest_stops()
