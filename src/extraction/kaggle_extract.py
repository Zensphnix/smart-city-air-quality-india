"""Download Kaggle historical Indian AQI data and persist city_day.csv with metadata.

Configure a Kaggle API token first. If Databricks blocks outbound access, download
the dataset locally and upload city_day.csv to the Volume's kaggle_historical folder.
"""

from __future__ import annotations

import argparse
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

DATASET = "rohanrao/air-quality-data-in-india"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, help="Local folder or Unity Catalog Volume path")
    parser.add_argument("--dataset", default=DATASET, help="Kaggle dataset slug")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    ingest_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    destination_dir = Path(args.output_dir) / "kaggle_historical" / f"ingest_date={ingest_ts[:10]}"
    destination_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="kaggle_aqi_") as temp_dir:
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(args.dataset, path=temp_dir, unzip=True, quiet=False)
        candidates = list(Path(temp_dir).rglob("city_day.csv"))
        if not candidates:
            raise FileNotFoundError("The Kaggle download did not contain city_day.csv")
        dataframe = pd.read_csv(candidates[0])
        dataframe["source"] = "kaggle_air_quality_india"
        dataframe["ingest_ts"] = ingest_ts
        output_csv = destination_dir / f"city_day_{ingest_ts.replace(':', '').replace('+', '_')}.csv"
        dataframe.to_csv(output_csv, index=False)
        logging.info("Saved %s rows to %s", len(dataframe), output_csv)


if __name__ == "__main__":
    main()
