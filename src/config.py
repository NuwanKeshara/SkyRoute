import logging
import sys
from dotenv import load_dotenv
import os


# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("./logs/config.log"),
        logging.StreamHandler(sys.stdout)
    ]
)


load_status = load_dotenv(".env")
if load_status is False:
    logging.error('Environment variables not loaded.')
    raise RuntimeError('Environment variables not loaded.')

try:
    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USER = os.getenv("NEO4J_USER")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
    DATA_PATH = os.getenv("DATA_PATH", "../data")

    logging.info('Environment variables loaded successfully.')

except Exception as e:
    logging.error(f"Missing required environment variable: {e}")
    raise
