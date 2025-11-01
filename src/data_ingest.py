import logging
import sys
from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATA_PATH
from data_quality import load_airports, load_airlines, load_routes


# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("./logs/data_quality.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Create constraints in Neo4j
def create_constraints(session):
    print("Creating constraints...")
    logging.info("Creating constraints in Neo4j database.")

    try:
        # create uniqueness constraints
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Airport) REQUIRE a.iata IS UNIQUE;")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (al:Airline) REQUIRE al.iata IS UNIQUE;")
    except Exception as e:
        logging.error(f"Error creating constraints: {e}")
        raise
    else:
        logging.info("Constraints created successfully.")



# Ingest airports data into Neo4j
def ingest_airports(session, airports_df):
    print("Ingesting airports...")
    logging.info("Ingesting airports data into Neo4j database.")

    # Cypher query for bulk insertion
    query = """
                UNWIND $rows AS row
                MERGE (a:Airport {iata: row.iata})
                SET a.name = row.name,
                    a.city = row.city,
                    a.country = row.country,
                    a.latitude = toFloat(row.latitude),
                    a.longitude = toFloat(row.longitude),
                    a.location = point({longitude: toFloat(row.longitude), latitude: toFloat(row.latitude)})
            """
    # Execute the query with the DataFrame records
    try:
        session.run(query, rows=airports_df.to_dict("records"))
    except Exception as e:
        logging.error(f"Error ingesting airports data: {e}")
        raise
    else:
        logging.info("Airports data ingested successfully.")
        print(f" Loaded {len(airports_df)} airports.")


# Ingest airlines data into Neo4j
def ingest_airlines(session, airlines_df):
    print("Ingesting airlines...")
    logging.info("Ingesting airlines data into Neo4j database.")

    # Cypher query for bulk insertion
    query = """
                UNWIND $rows AS row
                MERGE (al:Airline {iata: row.iata})
                SET al.name = row.name,
                    al.country = row.country
            """
    
    try:
        # Execute the query with the DataFrame records
        session.run(query, rows=airlines_df.to_dict("records"))
    except Exception as e:
        logging.error(f"Error ingesting airlines data: {e}")
        raise
    else:
        logging.info("Airlines data ingested successfully.")
        print(f" Loaded {len(airlines_df)} airlines.")



# Ingest routes data into Neo4j
def ingest_routes(session, routes_df):
    print("Ingesting routes...")
    logging.info("Ingesting routes data into Neo4j database.")

    # Cypher query for bulk insertion
    query = """
                UNWIND $rows AS row
                MATCH (src:Airport {iata: row.source_airport})
                MATCH (dst:Airport {iata: row.destination_airport})
                MATCH (al:Airline {iata: row.airline})
                MERGE (src)-[r:ROUTE {airline: row.airline}]->(dst)
                SET r.updated = datetime()
            """
    
    try:
        # Execute the query with the DataFrame records
        session.run(query, rows=routes_df.to_dict("records"))
    except Exception as e:
        logging.error(f"Error ingesting routes data: {e}")
        raise
    else:
        logging.info("Routes data ingested successfully.")
        print(f" Loaded {len(routes_df)} routes.")




def data_ingestion():
    print("Connecting to Neo4j Aura...")
    logging.info("Connecting to Neo4j Aura database.")

    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    except Exception as e:
        logging.error(f"Error connecting to Neo4j: {e}")
        raise
    else:
        with driver.session() as session:
            create_constraints(session)

            airports = load_airports(f"{DATA_PATH}/airports.dat")
            airlines = load_airlines(f"{DATA_PATH}/airlines.dat")
            routes = load_routes(f"{DATA_PATH}/routes.dat")

            ingest_airports(session, airports)
            ingest_airlines(session, airlines)
            ingest_routes(session, routes)

            logging.info("Data ingestion completed successfully.")
            print(" Ingestion complete!")
    finally:
        driver.close()
        logging.info("Neo4j connection closed.")
    


if __name__ == "__main__":
    data_ingestion()
