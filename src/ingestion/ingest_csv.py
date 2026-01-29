import pandas as pd
import logging
import os
from datetime import datetime
import sys

# =========================
# Configuration
# =========================
RAW_DATA_PATH = "data/raw/Warehouse_and_Retail_Sales.csv"
PROCESSED_DATA_DIR = "data/processed"
LOG_DIR = "logs"

REQUIRED_COLUMNS = [
    "YEAR",
    "MONTH",
    "SUPPLIER",
    "ITEM CODE",
    "ITEM DESCRIPTION",
    "ITEM TYPE",
    "RETAIL SALES",
    "RETAIL TRANSFERS",
    "WAREHOUSE SALES"
]

# =========================
# Setup directories
# =========================
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# =========================
# Logging configuration
# =========================
log_file = os.path.join(
    LOG_DIR,
    f"ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

# =========================
# Ingestion logic
# =========================
def ingest_csv():
    logging.info("Starting ingestion process")

    # 1. Read raw CSV
    try:
        df = pd.read_csv(RAW_DATA_PATH)
        logging.info(f"CSV loaded successfully | rows={df.shape[0]} | cols={df.shape[1]}")
    except Exception as e:
        logging.error("Failed to read raw CSV", exc_info=True)
        raise e

    # 2. Basic validation
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        logging.error(f"Missing required columns: {missing_cols}")
        raise ValueError("Schema validation failed")

    if df.empty:
        logging.error("Dataset is empty")
        raise ValueError("Empty dataset")

    logging.info("Schema and basic validation passed")

    # 3. Add ingestion metadata
    df["ingestion_timestamp"] = datetime.utcnow()

    # 4. Write processed copy
    output_file = os.path.join(
        PROCESSED_DATA_DIR,
        f"warehouse_retail_ingested_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    df.to_csv(output_file, index=False)
    logging.info(f"Processed data written to {output_file}")

    logging.info("Ingestion process completed successfully")


if __name__ == "__main__":
    ingest_csv()
    