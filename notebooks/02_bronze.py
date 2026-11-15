# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Bronze: immutable raw data to Delta
# MAGIC
# MAGIC Bronze is append-only. Open-Meteo payloads are retained exactly as raw JSON strings;
# MAGIC Kaggle rows retain every source column. Every record receives source, ingestion time,
# MAGIC source file, and Bronze load timestamp metadata.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "smart_city_aqi", "Unity Catalog name")
CATALOG = dbutils.widgets.get("catalog")
RAW_VOLUME = f"/Volumes/{CATALOG}/raw/raw_air_quality"
BRONZE_SCHEMA = f"{CATALOG}.bronze"

AIR_BRONZE_TABLE = f"{BRONZE_SCHEMA}.bronze_open_meteo_air_quality"
WEATHER_BRONZE_TABLE = f"{BRONZE_SCHEMA}.bronze_open_meteo_weather"
KAGGLE_BRONZE_TABLE = f"{BRONZE_SCHEMA}.bronze_kaggle_city_day"

# COMMAND ----------

def has_files(path: str, suffix: str) -> bool:
    """Return True only when a raw-source folder contains at least one matching file."""
    try:
        for item in dbutils.fs.ls(path):
            if item.isDir() and has_files(item.path, suffix):
                return True
            if not item.isDir() and item.path.lower().endswith(suffix):
                return True
    except Exception:
        return False
    return False


def append_open_meteo(source_folder: str, source_name: str, target_table: str) -> int:
    """Store each raw API envelope unchanged with metadata extracted from its envelope."""
    source_path = f"{RAW_VOLUME}/{source_folder}"
    if not has_files(source_path, ".json"):
        print(f"No JSON files found in {source_path}; skipping {target_table}.")
        return 0

    bronze_df = (
        spark.read.text(source_path, wholetext=True)
        .withColumnRenamed("value", "raw_json")
        .withColumn("source", F.lit(source_name))
        .withColumn("ingest_ts", F.to_timestamp(F.get_json_object("raw_json", "$.metadata.ingest_ts")))
        # input_file_name() is unsupported with Unity Catalog; _metadata is the supported source.
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("bronze_loaded_ts", F.current_timestamp())
        .withColumn("city", F.get_json_object("raw_json", "$.metadata.city"))
        .withColumn("state", F.get_json_object("raw_json", "$.metadata.state"))
        .withColumn("latitude", F.get_json_object("raw_json", "$.metadata.latitude").cast("double"))
        .withColumn("longitude", F.get_json_object("raw_json", "$.metadata.longitude").cast("double"))
        .withColumn("payload_json", F.get_json_object("raw_json", "$.payload"))
    )
    bronze_df.write.format("delta").mode("append").saveAsTable(target_table)
    return bronze_df.count()


def append_kaggle() -> int:
    """Append historical CSV rows while preserving all original Kaggle columns."""
    source_path = f"{RAW_VOLUME}/kaggle_historical"
    if not has_files(source_path, ".csv"):
        print(f"No CSV files found in {source_path}; skipping {KAGGLE_BRONZE_TABLE}.")
        return 0

    raw_df = spark.read.option("header", "true").option("inferSchema", "false").csv(source_path)
    source_column = F.coalesce(F.col("source"), F.lit("kaggle_air_quality_india")) if "source" in raw_df.columns else F.lit("kaggle_air_quality_india")
    ingest_column = F.to_timestamp(F.col("ingest_ts")) if "ingest_ts" in raw_df.columns else F.current_timestamp()
    bronze_df = (
        raw_df.withColumn("source", source_column)
        .withColumn("ingest_ts", ingest_column)
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("bronze_loaded_ts", F.current_timestamp())
    )
    bronze_df.write.format("delta").mode("append").saveAsTable(KAGGLE_BRONZE_TABLE)
    return bronze_df.count()

# COMMAND ----------

air_rows = append_open_meteo("open_meteo_air_quality", "open_meteo_air_quality", AIR_BRONZE_TABLE)
weather_rows = append_open_meteo("open_meteo_weather", "open_meteo_weather", WEATHER_BRONZE_TABLE)
kaggle_rows = append_kaggle()

summary_df = spark.createDataFrame(
    [
        (AIR_BRONZE_TABLE, air_rows),
        (WEATHER_BRONZE_TABLE, weather_rows),
        (KAGGLE_BRONZE_TABLE, kaggle_rows),
    ],
    ["table_name", "rows_appended"],
).withColumn("loaded_at", F.current_timestamp())
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze tables

# COMMAND ----------

for table_name in [AIR_BRONZE_TABLE, WEATHER_BRONZE_TABLE, KAGGLE_BRONZE_TABLE]:
    if spark.catalog.tableExists(table_name):
        print(f"{table_name}: {spark.table(table_name).count()} total rows")
        display(spark.table(table_name).limit(5))
