# Databricks notebook source
# MAGIC %md
# MAGIC # 00 – Unity Catalog setup
# MAGIC
# MAGIC Run this notebook once before any extraction or transformation notebook. It uses
# MAGIC Unity Catalog objects only; it does not use DBFS root or legacy Hive metastore paths.

# COMMAND ----------

# Configuration: change CATALOG to an existing workspace catalog (for example, `main`)
# if your Databricks Free Edition workspace does not allow catalog creation.
CATALOG = "smart_city_aqi"
RAW_SCHEMA = "raw"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
MONITORING_SCHEMA = "monitoring"
RAW_VOLUME = "raw_air_quality"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create catalog and schemas

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

for schema_name in [RAW_SCHEMA, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, MONITORING_SCHEMA]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema_name}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}.{RAW_VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create volume folders
# MAGIC
# MAGIC Raw files remain immutable. Later extraction scripts write to a timestamped path
# MAGIC beneath one of these source folders.

# COMMAND ----------

VOLUME_PATH = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{RAW_VOLUME}"

for folder in [
    "open_meteo_air_quality",
    "open_meteo_weather",
    "kaggle_historical",
    "reference",
]:
    dbutils.fs.mkdirs(f"{VOLUME_PATH}/{folder}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify setup

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{RAW_SCHEMA}"))
display(dbutils.fs.ls(VOLUME_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected result:** five schemas (`raw`, `bronze`, `silver`, `gold`, `monitoring`)
# MAGIC and one Volume, `raw_air_quality`. Capture this result and the Catalog Explorer for the report.
