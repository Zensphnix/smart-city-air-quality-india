# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 – Silver: cleaned, conformed city environment data
# MAGIC
# MAGIC Converts Bronze records into typed records, standardizes Indian city names, converts
# MAGIC Open-Meteo local timestamps to UTC, replaces invalid ranges with null, removes
# MAGIC duplicates, joins hourly AQI and weather, and uses Delta MERGE for idempotent upserts.

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "smart_city_aqi", "Unity Catalog name")
CATALOG = dbutils.widgets.get("catalog")
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

BRONZE_AIR = f"{BRONZE}.bronze_open_meteo_air_quality"
BRONZE_WEATHER = f"{BRONZE}.bronze_open_meteo_weather"
BRONZE_KAGGLE = f"{BRONZE}.bronze_kaggle_city_day"
SILVER_HOURLY = f"{SILVER}.silver_hourly_city_environment"
SILVER_DAILY = f"{SILVER}.silver_city_daily_aqi"

# COMMAND ----------

def canonical_city(city_column: F.Column) -> F.Column:
    """Standardize known historical aliases to the configured city names."""
    normalized = F.initcap(F.trim(city_column))
    return (
        F.when(normalized == "Bangalore", "Bengaluru")
        .when(normalized == "Gurgaon", "Gurugram")
        .when(normalized == "Bombay", "Mumbai")
        .when(normalized == "Calcutta", "Kolkata")
        .otherwise(normalized)
    )


def valid_number(column: F.Column, lower: float, upper: float) -> F.Column:
    """Retain a plausible numeric measurement; convert invalid or outlier values to null."""
    return F.when(column.between(lower, upper), column.cast("double"))


def aqi_category(aqi_column: F.Column) -> F.Column:
    """CPCB AQI bands, applied consistently throughout the pipeline and dashboard."""
    return (
        F.when(aqi_column.between(0, 50), "Good")
        .when(aqi_column.between(51, 100), "Satisfactory")
        .when(aqi_column.between(101, 200), "Moderate")
        .when(aqi_column.between(201, 300), "Poor")
        .when(aqi_column.between(301, 400), "Very Poor")
        .when(aqi_column.between(401, 500), "Severe")
    )


def latest_per_key(dataframe: DataFrame, keys: list[str]) -> DataFrame:
    """Keep the newest Bronze record for each business key."""
    window = Window.partitionBy(*keys).orderBy(F.col("bronze_loaded_ts").desc_nulls_last(), F.col("ingest_ts").desc_nulls_last())
    return dataframe.withColumn("_row_number", F.row_number().over(window)).filter(F.col("_row_number") == 1).drop("_row_number")


def merge_delta(source_df: DataFrame, target_table: str, merge_condition: str) -> None:
    """Create once, then perform idempotent Delta upserts on reruns."""
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return
    view_name = "silver_merge_source"
    source_df.createOrReplaceTempView(view_name)
    spark.sql(
        f"""
        MERGE INTO {target_table} AS target
        USING {view_name} AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse and clean hourly Open-Meteo air-quality payloads

# COMMAND ----------

AIR_SCHEMA = """
  STRUCT<
    timezone: STRING,
    hourly: STRUCT<
      time: ARRAY<STRING>,
      pm2_5: ARRAY<DOUBLE>, pm10: ARRAY<DOUBLE>, nitrogen_dioxide: ARRAY<DOUBLE>,
      sulphur_dioxide: ARRAY<DOUBLE>, carbon_monoxide: ARRAY<DOUBLE>, ozone: ARRAY<DOUBLE>,
      us_aqi: ARRAY<DOUBLE>
    >
  >
"""

air_payload = (
    spark.table(BRONZE_AIR)
    .select("city", "state", "latitude", "longitude", "ingest_ts", "bronze_loaded_ts", "file_name", F.from_json("payload_json", AIR_SCHEMA).alias("api"))
    .select("city", "state", "latitude", "longitude", "ingest_ts", "bronze_loaded_ts", "file_name", "api.timezone", F.explode(F.arrays_zip("api.hourly.time", "api.hourly.pm2_5", "api.hourly.pm10", "api.hourly.nitrogen_dioxide", "api.hourly.sulphur_dioxide", "api.hourly.carbon_monoxide", "api.hourly.ozone", "api.hourly.us_aqi")).alias("hour"))
    .select("city", "state", "latitude", "longitude", "ingest_ts", "bronze_loaded_ts", "file_name", "timezone", F.col("hour.time").alias("event_time_local_text"), F.col("hour.pm2_5").alias("pm2_5"), F.col("hour.pm10").alias("pm10"), F.col("hour.nitrogen_dioxide").alias("no2"), F.col("hour.sulphur_dioxide").alias("so2"), F.col("hour.carbon_monoxide").alias("co"), F.col("hour.ozone").alias("o3"), F.col("hour.us_aqi").alias("aqi"))
    .withColumn("city", canonical_city(F.col("city")))
    .withColumn("event_ts_local", F.to_timestamp("event_time_local_text"))
    .withColumn("event_ts_utc", F.to_utc_timestamp("event_ts_local", "Asia/Kolkata"))
    .withColumn("observed_date", F.to_date("event_ts_local"))
    .withColumn("pm2_5", valid_number(F.col("pm2_5"), 0, 1000))
    .withColumn("pm10", valid_number(F.col("pm10"), 0, 1500))
    .withColumn("no2", valid_number(F.col("no2"), 0, 1000))
    .withColumn("so2", valid_number(F.col("so2"), 0, 1000))
    .withColumn("co", valid_number(F.col("co"), 0, 100000))
    .withColumn("o3", valid_number(F.col("o3"), 0, 1000))
    .withColumn("aqi", valid_number(F.col("aqi"), 0, 500))
    .filter(F.col("event_ts_utc").isNotNull() & F.col("city").isNotNull())
)
air_silver = latest_per_key(air_payload, ["city", "event_ts_utc"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse and clean hourly Open-Meteo weather payloads

# COMMAND ----------

WEATHER_SCHEMA = """
  STRUCT<
    timezone: STRING,
    hourly: STRUCT<
      time: ARRAY<STRING>, temperature_2m: ARRAY<DOUBLE>, relative_humidity_2m: ARRAY<DOUBLE>,
      wind_speed_10m: ARRAY<DOUBLE>, surface_pressure: ARRAY<DOUBLE>, precipitation: ARRAY<DOUBLE>
    >
  >
"""

weather_payload = (
    spark.table(BRONZE_WEATHER)
    .select("city", "ingest_ts", "bronze_loaded_ts", "file_name", F.from_json("payload_json", WEATHER_SCHEMA).alias("api"))
    .select("city", "ingest_ts", "bronze_loaded_ts", "file_name", "api.timezone", F.explode(F.arrays_zip("api.hourly.time", "api.hourly.temperature_2m", "api.hourly.relative_humidity_2m", "api.hourly.wind_speed_10m", "api.hourly.surface_pressure", "api.hourly.precipitation")).alias("hour"))
    .select("city", "ingest_ts", "bronze_loaded_ts", "file_name", "timezone", F.col("hour.time").alias("event_time_local_text"), F.col("hour.temperature_2m").alias("temperature_c"), F.col("hour.relative_humidity_2m").alias("humidity_pct"), F.col("hour.wind_speed_10m").alias("wind_speed_kmh"), F.col("hour.surface_pressure").alias("pressure_hpa"), F.col("hour.precipitation").alias("precipitation_mm"))
    .withColumn("city", canonical_city(F.col("city")))
    .withColumn("event_ts_local", F.to_timestamp("event_time_local_text"))
    .withColumn("event_ts_utc", F.to_utc_timestamp("event_ts_local", "Asia/Kolkata"))
    .withColumn("temperature_c", valid_number(F.col("temperature_c"), -10, 60))
    .withColumn("humidity_pct", valid_number(F.col("humidity_pct"), 0, 100))
    .withColumn("wind_speed_kmh", valid_number(F.col("wind_speed_kmh"), 0, 150))
    .withColumn("pressure_hpa", valid_number(F.col("pressure_hpa"), 800, 1100))
    .withColumn("precipitation_mm", valid_number(F.col("precipitation_mm"), 0, 500))
    .filter(F.col("event_ts_utc").isNotNull() & F.col("city").isNotNull())
)
weather_silver = latest_per_key(weather_payload, ["city", "event_ts_utc"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join AQI and weather, categorize AQI, and MERGE the hourly Silver table

# COMMAND ----------

hourly_silver = (
    air_silver.alias("aq")
    .join(weather_silver.alias("wx"), on=[F.col("aq.city") == F.col("wx.city"), F.col("aq.event_ts_utc") == F.col("wx.event_ts_utc")], how="left")
    .select(
        F.col("aq.city"), F.col("aq.state"), F.col("aq.latitude"), F.col("aq.longitude"), F.col("aq.event_ts_local"), F.col("aq.event_ts_utc"), F.col("aq.observed_date"),
        F.col("aq.pm2_5"), F.col("aq.pm10"), F.col("aq.no2"), F.col("aq.so2"), F.col("aq.co"), F.col("aq.o3"), F.col("aq.aqi"),
        F.col("wx.temperature_c"), F.col("wx.humidity_pct"), F.col("wx.wind_speed_kmh"), F.col("wx.pressure_hpa"), F.col("wx.precipitation_mm"),
        F.lit("open_meteo").alias("source_system"), F.col("aq.ingest_ts"), F.col("aq.file_name"), F.col("aq.bronze_loaded_ts")
    )
    .withColumn("aqi_category", aqi_category(F.col("aqi")))
)
merge_delta(hourly_silver, SILVER_HOURLY, "target.city = source.city AND target.event_ts_utc = source.event_ts_utc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a conformed daily AQI table from Open-Meteo and Kaggle history

# COMMAND ----------

open_meteo_daily = (
    hourly_silver.groupBy("city", "state", "observed_date")
    .agg(F.avg("aqi").alias("aqi"), F.avg("pm2_5").alias("pm2_5"), F.avg("pm10").alias("pm10"), F.avg("no2").alias("no2"), F.avg("so2").alias("so2"), F.avg("co").alias("co"), F.avg("o3").alias("o3"), F.max("ingest_ts").alias("ingest_ts"))
    .withColumn("source_system", F.lit("open_meteo"))
)

if spark.catalog.tableExists(BRONZE_KAGGLE):
    kaggle_daily = (
        spark.table(BRONZE_KAGGLE)
        .select(F.col("City").alias("city"), F.to_date("Date").alias("observed_date"), F.col("AQI").cast("double").alias("aqi"), F.col("PM2.5").cast("double").alias("pm2_5"), F.col("PM10").cast("double").alias("pm10"), F.col("NO2").cast("double").alias("no2"), F.col("SO2").cast("double").alias("so2"), F.col("CO").cast("double").alias("co"), F.col("O3").cast("double").alias("o3"), "ingest_ts")
        .withColumn("city", canonical_city(F.col("city")))
        .withColumn("aqi", valid_number(F.col("aqi"), 0, 500))
        .withColumn("pm2_5", valid_number(F.col("pm2_5"), 0, 1000))
        .withColumn("pm10", valid_number(F.col("pm10"), 0, 1500))
        .withColumn("no2", valid_number(F.col("no2"), 0, 1000))
        .withColumn("so2", valid_number(F.col("so2"), 0, 1000))
        .withColumn("co", valid_number(F.col("co"), 0, 100000))
        .withColumn("o3", valid_number(F.col("o3"), 0, 1000))
        .withColumn("state", F.lit(None).cast("string"))
        .withColumn("source_system", F.lit("kaggle_air_quality_india"))
        .filter(F.col("city").isNotNull() & F.col("observed_date").isNotNull())
    )
    kaggle_daily = latest_per_key(kaggle_daily, ["city", "observed_date", "source_system"])
    daily_silver = open_meteo_daily.unionByName(kaggle_daily.select(open_meteo_daily.columns), allowMissingColumns=True)
else:
    daily_silver = open_meteo_daily

daily_silver = daily_silver.withColumn("aqi_category", aqi_category(F.col("aqi")))
merge_delta(daily_silver, SILVER_DAILY, "target.city = source.city AND target.observed_date = source.observed_date AND target.source_system = source.source_system")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver output

# COMMAND ----------

display(spark.table(SILVER_HOURLY).orderBy(F.col("event_ts_utc").desc()).limit(20))
display(spark.table(SILVER_DAILY).orderBy(F.col("observed_date").desc()).limit(20))