import requests
from google.transit import gtfs_realtime_pb2
import pandas as pd
from datetime import datetime
from pathlib import Path

# Répertoires
DATA_DIR = Path(
    "/Users/pernebayarailym/Documents/Portfolio_Projects_AP/Simplon_DE_Projects/Python_Projects/Airflow_ETL_Nice_lignes_DAazure/data"
)
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


if __name__ == "__main__":  # This is the entry point.
    extract_vehicle_positions()
    extract_trip_updates()
