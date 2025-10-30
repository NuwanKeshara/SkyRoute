import pandas as pd
from config import DATA_PATH



def load_airports(path):

    cols = [
        "airport_id", "name", "city", "country", "iata", "icao",
        "latitude", "longitude", "altitude", "timezone",
        "dst", "tz_database_timezone", "type", "source"
    ]
    df = pd.read_csv(path, header=None, names=cols)

    # filter invalid IATA codes
    df = df[df["iata"].notna() & (df["iata"] != "\\N")]
    df = df.drop_duplicates(subset=["iata"])

    return df[["iata", "name", "city", "country", "latitude", "longitude"]]




def load_airlines(path):

    cols = ["airline_id", "name", "alias", "iata", "icao", "callsign", "country", "active"]
    df = pd.read_csv(path, header=None, names=cols)
    df = df[df["iata"].notna() & (df["iata"] != "\\N")]

    # keep only active airlines
    df = df[df["active"] == "Y"]

    return df[["iata", "name", "country"]]




def load_routes(path):

    cols = ["airline", "airline_id", "source_airport", "source_airport_id",
            "destination_airport", "destination_airport_id", "codeshare", "stops", "equipment"]
    df = pd.read_csv(path, header=None, names=cols)

    # keep only valid airport data
    df = df[(df["source_airport"] != "\\N") & (df["destination_airport"] != "\\N")]

    return df[["airline", "source_airport", "destination_airport"]]




if __name__ == "__main__":
    airports = load_airports(f"{DATA_PATH}/airports.dat")
    airlines = load_airlines(f"{DATA_PATH}/airlines.dat")
    routes = load_routes(f"{DATA_PATH}/routes.dat")

    print("Airports:", len(airports))
    print("Airlines:", len(airlines))
    print("Routes:", len(routes))
