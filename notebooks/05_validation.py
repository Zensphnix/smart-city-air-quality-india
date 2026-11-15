# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 – Data quality validation
# MAGIC
# MAGIC Checks every populated Medallion layer for volume, nulls, valid ranges, duplicates,
# MAGIC and freshness. Results are appended to `monitoring.dq_results` so quality history can
# MAGIC be reported and audited over time.

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "smart_city_aqi", "Unity Catalog name")
CATALOG = dbutils.widgets.get("catalog")
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"
DQ_TABLE = f"{CATALOG}.monitoring.dq_results"
RUN_ID = str(uuid4())
results = []


def record(layer: str, table_name: str, check_name: str, status: str, actual_value: float | None, threshold: str, details: str) -> None:
    results.append((RUN_ID, datetime.now(timezone.utc), layer, table_name, check_name, status, actual_value, threshold, details))


def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def row_count_check(layer: str, table_name: str) -> None:
    if not table_exists(table_name):
        record(layer, table_name, "table_exists_and_has_rows", "SKIPPED", None, "> 0", "Table has not been created; upstream optional source may be unavailable.")
        return
    rows = spark.table(table_name).count()
    record(layer, table_name, "row_count", "PASS" if rows > 0 else "FAIL", float(rows), "> 0", "Row-count validation.")


def null_percentage_check(table_name: str, column_name: str, max_percent: float = 5.0) -> None:
    if not table_exists(table_name):
        return
    metrics = spark.table(table_name).agg(
        F.count(F.lit(1)).alias("total_rows"),
        F.sum(F.when(F.col(column_name).isNull(), 1).otherwise(0)).alias("null_rows"),
    ).first()
    total = metrics["total_rows"]
    null_pct = 0.0 if total == 0 else round(100.0 * (metrics["null_rows"] or 0) / total, 2)
    record("silver", table_name, f"null_percentage_{column_name}", "PASS" if null_pct <= max_percent else "FAIL", null_pct, f"<= {max_percent}%", f"{metrics['null_rows'] or 0} null values out of {total} rows.")


def invalid_range_check(table_name: str, column_name: str, lower: float, upper: float) -> None:
    if not table_exists(table_name):
        return
    invalid_rows = (
        spark.table(table_name)
        .filter(F.col(column_name).isNotNull() & ~F.col(column_name).between(lower, upper))
        .count()
    )
    record("silver", table_name, f"range_{column_name}", "PASS" if invalid_rows == 0 else "FAIL", float(invalid_rows), "0 invalid rows", f"Expected {column_name} between {lower} and {upper} when present.")


def duplicate_check(table_name: str, key_columns: list[str]) -> None:
    if not table_exists(table_name):
        return
    duplicate_rows = (
        spark.table(table_name)
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .agg(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicates"))
        .first()["duplicates"]
    )
    record("silver", table_name, f"duplicate_{'_'.join(key_columns)}", "PASS" if duplicate_rows == 0 else "FAIL", float(duplicate_rows), "0 duplicate rows", "Duplicate rows beyond the first record for each business key.")


def freshness_check(table_name: str, timestamp_column: str, max_age_hours: int = 48) -> None:
    if not table_exists(table_name):
        return
    latest_timestamp = spark.table(table_name).agg(F.max(timestamp_column).alias("latest_timestamp")).first()["latest_timestamp"]
    if latest_timestamp is None:
        record("gold", table_name, f"freshness_{timestamp_column}", "FAIL", None, f"<= {max_age_hours} hours", "No timestamp was available for freshness evaluation.")
        return
    latest_utc = latest_timestamp.replace(tzinfo=timezone.utc) if latest_timestamp.tzinfo is None else latest_timestamp.astimezone(timezone.utc)
    age_hours = round((datetime.now(timezone.utc) - latest_utc).total_seconds() / 3600, 2)
    record("gold", table_name, f"freshness_{timestamp_column}", "PASS" if age_hours <= max_age_hours else "FAIL", age_hours, f"<= {max_age_hours} hours", f"Latest timestamp: {latest_timestamp}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume checks across Bronze, Silver, and Gold

# COMMAND ----------

for layer, table in [
    ("bronze", f"{BRONZE}.bronze_open_meteo_air_quality"),
    ("bronze", f"{BRONZE}.bronze_open_meteo_weather"),
    ("bronze", f"{BRONZE}.bronze_kaggle_city_day"),
    ("silver", f"{SILVER}.silver_hourly_city_environment"),
    ("silver", f"{SILVER}.silver_city_daily_aqi"),
    ("gold", f"{GOLD}.gold_city_daily_aqi"),
    ("gold", f"{GOLD}.gold_city_monthly_trend"),
    ("gold", f"{GOLD}.gold_pollution_hotspots"),
    ("gold", f"{GOLD}.gold_pollutant_contribution"),
    ("gold", f"{GOLD}.gold_weather_aqi_correlation"),
    ("gold", f"{GOLD}.gold_hourly_pattern"),
]:
    row_count_check(layer, table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver integrity checks

# COMMAND ----------

HOURLY_TABLE = f"{SILVER}.silver_hourly_city_environment"
DAILY_TABLE = f"{SILVER}.silver_city_daily_aqi"

for required_column in ["city", "event_ts_utc", "aqi_category"]:
    null_percentage_check(HOURLY_TABLE, required_column)
for required_column in ["city", "observed_date", "aqi_category"]:
    null_percentage_check(DAILY_TABLE, required_column)

for measurement, lower, upper in [
    ("aqi", 0, 500), ("pm2_5", 0, 1000), ("pm10", 0, 1500),
    ("temperature_c", -10, 60), ("humidity_pct", 0, 100), ("wind_speed_kmh", 0, 150),
]:
    invalid_range_check(HOURLY_TABLE, measurement, lower, upper)

invalid_range_check(DAILY_TABLE, "aqi", 0, 500)
duplicate_check(HOURLY_TABLE, ["city", "event_ts_utc"])
duplicate_check(DAILY_TABLE, ["city", "observed_date", "source_system"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Freshness checks

# COMMAND ----------

freshness_check(HOURLY_TABLE, "ingest_ts")
for gold_table in [
    "gold_city_daily_aqi", "gold_city_monthly_trend", "gold_pollution_hotspots",
    "gold_pollutant_contribution", "gold_weather_aqi_correlation", "gold_hourly_pattern",
]:
    freshness_check(f"{GOLD}.{gold_table}", "gold_refreshed_ts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist and review results

# COMMAND ----------

dq_schema = "run_id string, checked_at timestamp, layer string, table_name string, check_name string, status string, actual_value double, threshold string, details string"
dq_results_df = spark.createDataFrame(results, dq_schema)
dq_results_df.write.format("delta").mode("append").saveAsTable(DQ_TABLE)

display(dq_results_df.orderBy("layer", "table_name", "check_name"))
display(spark.table(DQ_TABLE).orderBy(F.col("checked_at").desc()).limit(100))