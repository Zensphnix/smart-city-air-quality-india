# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 – Raw extraction to Unity Catalog Volume
# MAGIC
# MAGIC **One-time prerequisite:** In Catalog Explorer upload `config/cities.yaml` into
# MAGIC `smart_city_aqi > raw > raw_air_quality > reference`. Update the catalog widget if needed.

# COMMAND ----------

# MAGIC %pip install requests pyyaml

# COMMAND ----------

dbutils.widgets.text("catalog", "smart_city_aqi", "Unity Catalog name")
dbutils.widgets.text("start_date", "2026-11-08", "Start date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2026-11-14", "End date (YYYY-MM-DD)")

from datetime import date, datetime, timezone
import json
import time
from pathlib import Path
import requests
import yaml

CATALOG = dbutils.widgets.get("catalog")
START_DATE, END_DATE = dbutils.widgets.get("start_date"), dbutils.widgets.get("end_date")
VOLUME_PATH = f"/Volumes/{CATALOG}/raw/raw_air_quality"
CITIES_CONFIG_PATH = f"{VOLUME_PATH}/reference/cities.yaml"
if date.fromisoformat(START_DATE) > date.fromisoformat(END_DATE):
    raise ValueError("Start date must be on or before end date.")

# Git-folder friendly fallback: creates the same reference file automatically on
# the first run, so manual YAML upload is not required.
DEFAULT_CITY_CONFIG = {
    "timezone": "Asia/Kolkata",
    "cities": [
        {"city": "Delhi", "state": "Delhi", "latitude": 28.6139, "longitude": 77.2090},
        {"city": "Gurugram", "state": "Haryana", "latitude": 28.4595, "longitude": 77.0266},
        {"city": "Mumbai", "state": "Maharashtra", "latitude": 19.0760, "longitude": 72.8777},
        {"city": "Kolkata", "state": "West Bengal", "latitude": 22.5726, "longitude": 88.3639},
        {"city": "Chennai", "state": "Tamil Nadu", "latitude": 13.0827, "longitude": 80.2707},
        {"city": "Bengaluru", "state": "Karnataka", "latitude": 12.9716, "longitude": 77.5946},
        {"city": "Hyderabad", "state": "Telangana", "latitude": 17.3850, "longitude": 78.4867},
        {"city": "Pune", "state": "Maharashtra", "latitude": 18.5204, "longitude": 73.8567},
        {"city": "Ahmedabad", "state": "Gujarat", "latitude": 23.0225, "longitude": 72.5714},
        {"city": "Lucknow", "state": "Uttar Pradesh", "latitude": 26.8467, "longitude": 80.9462},
        {"city": "Jaipur", "state": "Rajasthan", "latitude": 26.9124, "longitude": 75.7873},
        {"city": "Patna", "state": "Bihar", "latitude": 25.5941, "longitude": 85.1376},
    ],
}
if not Path(CITIES_CONFIG_PATH).exists():
    dbutils.fs.mkdirs(f"{VOLUME_PATH}/reference")
    dbutils.fs.put(CITIES_CONFIG_PATH, yaml.safe_dump(DEFAULT_CITY_CONFIG), overwrite=False)
with open(CITIES_CONFIG_PATH, encoding="utf-8") as config_file:
    city_config = yaml.safe_load(config_file)

# COMMAND ----------

AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_json(url: str, params: dict) -> dict:
    for attempt in range(1, 5):
        try:
            response = requests.get(url, params=params, timeout=45)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as error:
            if attempt == 4:
                raise RuntimeError(f"Request to {url} failed after 4 attempts: {error}") from error
            time.sleep(2 ** attempt)


def save_envelope(source: str, city: dict, payload: dict, ingest_ts: str) -> str:
    city_key = city["city"].lower().replace(" ", "_")
    target_dir = f"{VOLUME_PATH}/{source}/ingest_date={ingest_ts[:10]}"
    dbutils.fs.mkdirs(target_dir)
    target_file = f"{target_dir}/{city_key}_{ingest_ts.replace(':', '').replace('+', '_')}.json"
    envelope = {"metadata": {"source": source, "city": city["city"], "state": city["state"], "latitude": city["latitude"], "longitude": city["longitude"], "ingest_ts": ingest_ts}, "payload": payload}
    dbutils.fs.put(target_file, json.dumps(envelope), overwrite=False)
    return target_file

# COMMAND ----------

ingest_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
written_files = []
for city in city_config["cities"]:
    common = {"latitude": city["latitude"], "longitude": city["longitude"], "start_date": START_DATE, "end_date": END_DATE, "timezone": city_config["timezone"]}
    air_quality = fetch_json(AIR_QUALITY_URL, {**common, "hourly": "pm2_5,pm10,nitrogen_dioxide,sulphur_dioxide,carbon_monoxide,ozone,us_aqi"})
    weather = fetch_json(WEATHER_URL, {**common, "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,surface_pressure,precipitation"})
    written_files += [save_envelope("open_meteo_air_quality", city, air_quality, ingest_ts), save_envelope("open_meteo_weather", city, weather, ingest_ts)]
display(spark.createDataFrame([(path,) for path in written_files], ["raw_file"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kaggle historical CSV fallback
# MAGIC Download `city_day.csv` from Kaggle's **Air Quality Data in India** dataset and upload it to
# MAGIC `/Volumes/<catalog>/raw/raw_air_quality/kaggle_historical/`. When outbound internet and
# MAGIC Kaggle credentials are available, run locally:
# MAGIC `python -m src.extraction.kaggle_extract --output-dir data/raw`

# COMMAND ----------

display(dbutils.fs.ls(VOLUME_PATH))