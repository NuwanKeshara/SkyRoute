import logging
import sys
import pandas as pd
from config import DATA_PATH

# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("./logs/data_quality.log"),
        logging.StreamHandler(sys.stdout)
    ]
)


# Load and clean airports data
def load_airports(path):
    cols = [
        "airport_id", "name", "city", "country", "iata", "icao",
        "latitude", "longitude", "altitude", "timezone",
        "dst", "tz_database_timezone", "type", "source"
    ]
    try:
        df = pd.read_csv(path, header=None, names=cols)
    except Exception as e:
        logging.error(f"Error loading airports data: {e}")
        raise
    else:
        logging.info(f"Airports data loaded successfully from {path}")

        # Basic cleanup
        df["iata"] = df["iata"].astype(str).str.strip()
        df = df[df["iata"].notna() & (df["iata"] != "\\N") & (df["iata"] != "")]
        df = df.drop_duplicates(subset=["iata"])

        # Keep only valid latitude/longitude
        df = df[pd.to_numeric(df["latitude"], errors="coerce").notna()]
        df = df[pd.to_numeric(df["longitude"], errors="coerce").notna()]

        # Final clean dataset
        df = df[["iata", "name", "city", "country", "latitude", "longitude"]]

        return df



# Load and clean airlines data
def load_airlines(path):
    cols = ["airline_id", "name", "alias", "iata", "icao", "callsign", "country", "active"]

    try:
        df = pd.read_csv(path, header=None, names=cols)
    except Exception as e:
        logging.error(f"Error loading airlines data: {e}")
        raise
    else:
        logging.info(f"Airlines data loaded successfully from {path}")

        # Cleanup
        df["iata"] = df["iata"].astype(str).str.strip()
        df = df[df["iata"].notna() & (df["iata"] != "\\N") & (df["iata"] != "")]
        df = df[df["active"] == "Y"]

        # Keep only valid IATA (mostly 2-character codes)
        df = df[df["iata"].str.len().between(2, 3)]

        # Final clean dataset
        df = df[["iata", "name", "country"]]

        return df



# Load and clean routes data
def load_routes(path):
    cols = [
        "airline", "airline_id", "source_airport", "source_airport_id",
        "destination_airport", "destination_airport_id", "codeshare", "stops", "equipment"
    ]

    try:
        df = pd.read_csv(path, header=None, names=cols)
    except Exception as e:
        logging.error(f"Error loading routes data: {e}")
        raise   
    else:
        logging.info(f"Routes data loaded successfully from {path}")

        # Cleanup
        df["airline"] = df["airline"].astype(str).str.strip()
        df["source_airport"] = df["source_airport"].astype(str).str.strip()
        df["destination_airport"] = df["destination_airport"].astype(str).str.strip()

        # Remove rows with missing or invalid IATA codes
        df = df[
            (df["source_airport"].notna()) &
            (df["destination_airport"].notna()) &
            (df["source_airport"] != "\\N") &
            (df["destination_airport"] != "\\N") &
            (df["source_airport"] != "") &
            (df["destination_airport"] != "")
        ]

        # Final clean dataset
        df = df[["airline", "source_airport", "destination_airport"]]

        return df



if __name__ == "__main__":

    # Load datasets
    airports = load_airports(f"{DATA_PATH}/airports.dat")
    airlines = load_airlines(f"{DATA_PATH}/airlines.dat")
    routes = load_routes(f"{DATA_PATH}/routes.dat")

    # data checks
    print("Data Summary:")
    print(f"Airports: {len(airports)}")
    print(f"Airlines: {len(airlines)}")
    print(f"Routes: {len(routes)}")

    logging.info(f"Airports loaded: {len(airports)}")
    logging.info(f"Airlines loaded: {len(airlines)}")
    logging.info(f"Routes loaded: {len(routes)}")
