import pandas as pd

for f in ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt"]:
    try:
        df = pd.read_csv("data/gtfs_static/" + f)
        print(f, "-> columns:", df.columns.tolist(), "rows:", len(df))
        print(df.head(1).to_dict(orient="records"))
    except Exception as e:
        print("could not open", f, e)
