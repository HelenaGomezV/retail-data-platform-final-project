import pandas as pd
import logging
import os
from datetime import datetime

# =========================
# Logging configuration
# =========================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(
    LOG_DIR, f"ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# =========================
# Paths
# =========================
RAW_DATA_PATH = "data/raw/Warehouse_and_Retail_Sales.csv"
PROCESSED_DATA_DIR = "data/processed"
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# =========================
# Ingestion function
# =========================
def ingest_csv():
    logging.info("Starting data ingestion")

    try:
        df = pd.read_csv(RAW_DATA_PATH)
        logging.info(
            f"CSV loaded successfully | rows={df.shape[0]} | cols={df.shape[1]}"
        )

        if df.empty:
            raise ValueError("Dataset is empty")

        logging.info("Basic data validation passed")

        output_file = os.path.join(
            PROCESSED_DATA_DIR,
            f"warehouse_retail_loaded_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

        df.to_csv(output_file, index=False)
        logging.info(f"Processed data written to {output_file}")

        logging.info("Data ingestion completed successfully")

    except Exception as e:
        logging.error("Data ingestion failed", exc_info=True)
        raise e


if __name__ == "__main__":
    ingest_csv()
