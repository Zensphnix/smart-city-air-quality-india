"""Download Open-Meteo Air Quality and Weather data as immutable raw JSON files.

Example (local execution):
python -m src.extraction.open_meteo_extract --cities-config config/cities.yaml --start-date 2026-11-08 --end-date 2026-11-14 --output-dir data/raw
For Databricks, use a Unity Catalog Volume path for --output-dir.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests
import yaml

AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 2


def configure_logging() -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    return logging.getLogger(__name__)


def get_json_with_retry(url: str, params: dict[str, Any], logger: logging.Logger) -> dict[str, Any]:
    """Request one API payload with bounded exponential retry/backoff."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, timeout=45)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            if attempt == MAX_ATTEMPTS:
                raise RuntimeError(f"API request failed after {MAX_ATTEMPTS} attempts: {exc}") from exc
            delay = BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.warning("Attempt %s/%s failed (%s). Retrying in %s seconds.", attempt, MAX_ATTEMPTS, exc, delay)
            time.sleep(delay)
    raise AssertionError("Unreachable")


def write_raw_payload(output_dir: Path, source: str, city: dict[str, Any], payload: dict[str, Any], ingest_ts: str) -> Path:
    """Write API output inside an envelope containing ingestion metadata."""
    safe_city = city["city"].lower().replace(" ", "_")
    file_dir = output_dir / source / f"ingest_date={ingest_ts[:10]}"
    file_dir.mkdir(parents=True, exist_ok=True)
    destination = file_dir / f"{safe_city}_{ingest_ts.replace(':', '').replace('+', '_')}.json"
    envelope = {
        "metadata": {"source": source, "city": city["city"], "state": city["state"], "latitude": city["latitude"], "longitude": city["longitude"], "ingest_ts": ingest_ts},
        "payload": payload,
    }
    destination.write_text(json.dumps(envelope, ensure_ascii=False), encoding="utf-8")
    return destination


def extract_city(city: dict[str, Any], start_date: str, end_date: str, timezone_name: str, output_dir: Path, logger: logging.Logger) -> list[Path]:
    """Fetch and persist both required Open-Meteo source payloads for one city."""
    common = {"latitude": city["latitude"], "longitude": city["longitude"], "start_date": start_date, "end_date": end_date, "timezone": timezone_name}
    ingest_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    air_payload = get_json_with_retry(AIR_QUALITY_URL, {**common, "hourly": "pm2_5,pm10,nitrogen_dioxide,sulphur_dioxide,carbon_monoxide,ozone,us_aqi"}, logger)
    weather_payload = get_json_with_retry(WEATHER_URL, {**common, "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,surface_pressure,precipitation"}, logger)
    return [write_raw_payload(output_dir, "open_meteo_air_quality", city, air_payload, ingest_ts), write_raw_payload(output_dir, "open_meteo_weather", city, weather_payload, ingest_ts)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cities-config", required=True, help="Path to config/cities.yaml")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, type=date.fromisoformat, help="YYYY-MM-DD")
    parser.add_argument("--output-dir", required=True, help="Local folder or Unity Catalog Volume path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.start_date > args.end_date:
        raise ValueError("--start-date must be on or before --end-date")
    config = yaml.safe_load(Path(args.cities_config).read_text(encoding="utf-8"))
    logger = configure_logging()
    for city in config["cities"]:
        files = extract_city(city, args.start_date.isoformat(), args.end_date.isoformat(), config["timezone"], Path(args.output_dir), logger)
        logger.info("Saved %s", ", ".join(str(file) for file in files))


if __name__ == "__main__":
    main()
