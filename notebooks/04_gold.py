# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 – Gold: dashboard-ready AQI intelligence tables
# MAGIC
# MAGIC Rebuilds compact analytical tables from Silver. Gold tables are deterministic derived
# MAGIC outputs, so each run replaces the prior table snapshot with fresh results.

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "smart_city_aqi", "Unity Catalog name")
CATALOG = dbutils.widgets.get("catalog")
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

SILVER_HOURLY = f"{SILVER}.silver_hourly_city_environment"
SILVER_DAILY = f"{SILVER}.silver_city_daily_aqi"


def write_gold(dataframe: DataFrame, table_name: str) -> None:
    """Publish one dashboard table as a Delta snapshot with a common refresh timestamp."""
    (
        dataframe.withColumn("gold_refreshed_ts", F.current_timestamp())
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def cpcb_category(aqi: F.Column) -> F.Column:
    return (
        F.when(aqi.between(0, 50), "Good")
        .when(aqi.between(51, 100), "Satisfactory")
        .when(aqi.between(101, 200), "Moderate")
        .when(aqi.between(201, 300), "Poor")
        .when(aqi.between(301, 400), "Very Poor")
        .when(aqi.between(401, 500), "Severe")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conformed daily AQI base
# MAGIC
# MAGIC When Kaggle and Open-Meteo overlap for a city/date, retain Kaggle as the historical
# MAGIC source of record; Open-Meteo supplies recent dates that Kaggle does not cover.

# COMMAND ----------

source_priority = F.when(F.col("source_system") == "kaggle_air_quality_india", 1).otherwise(2)
daily_window = Window.partitionBy("city", "observed_date").orderBy(source_priority, F.col("ingest_ts").desc_nulls_last())

daily_base = (
    spark.table(SILVER_DAILY)
    .filter(F.col("aqi").isNotNull())
    .withColumn("_source_rank", F.row_number().over(daily_window))
    .filter(F.col("_source_rank") == 1)
    .drop("_source_rank")
    .withColumn("aqi_category", cpcb_category(F.col("aqi")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Daily AQI – page filters, line charts, and City Detail drill-through

# COMMAND ----------

gold_city_daily_aqi = daily_base.select(
    "city", "state", "observed_date", "source_system", "aqi", "aqi_category",
    "pm2_5", "pm10", "no2", "so2", "co", "o3", "ingest_ts",
    F.year("observed_date").alias("year"),
    F.month("observed_date").alias("month_number"),
    F.date_format("observed_date", "MMM").alias("month_name"),
    F.quarter("observed_date").alias("quarter"),
    F.when(F.dayofweek("observed_date").isin(1, 7), "Weekend").otherwise("Weekday").alias("day_type"),
)
write_gold(gold_city_daily_aqi, f"{GOLD}.gold_city_daily_aqi")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Monthly trend – Overview and Trends pages

# COMMAND ----------

gold_city_monthly_trend = (
    gold_city_daily_aqi.groupBy("city", "year", "month_number", "month_name")
    .agg(
        F.avg("aqi").alias("avg_aqi"),
        F.max("aqi").alias("max_aqi"),
        F.min("aqi").alias("min_aqi"),
        F.count("aqi").alias("days_with_aqi"),
        F.sum(F.when(F.col("aqi") >= 201, 1).otherwise(0)).alias("days_poor_plus"),
        F.avg("pm2_5").alias("avg_pm2_5"),
        F.avg("pm10").alias("avg_pm10"),
    )
    .withColumn("month_start_date", F.make_date("year", "month_number", F.lit(1)))
    .withColumn("aqi_category", cpcb_category(F.round("avg_aqi").cast("double")))
)
write_gold(gold_city_monthly_trend, f"{GOLD}.gold_city_monthly_trend")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pollution hotspots – ranked city bars and conditional-format table

# COMMAND ----------

gold_pollution_hotspots = (
    gold_city_daily_aqi.groupBy("city", "state")
    .agg(
        F.avg("aqi").alias("avg_aqi"),
        F.max("aqi").alias("max_aqi"),
        F.count("observed_date").alias("days_observed"),
        F.sum(F.when(F.col("aqi") >= 201, 1).otherwise(0)).alias("days_poor_plus"),
        F.round(100 * F.avg(F.when(F.col("aqi") >= 201, 1).otherwise(0)), 1).alias("poor_plus_pct"),
    )
    .withColumn("avg_aqi_category", cpcb_category(F.round("avg_aqi").cast("double")))
)
hotspot_window = Window.orderBy(F.col("avg_aqi").desc(), F.col("max_aqi").desc())
gold_pollution_hotspots = gold_pollution_hotspots.withColumn("hotspot_rank", F.dense_rank().over(hotspot_window))
write_gold(gold_pollution_hotspots, f"{GOLD}.gold_pollution_hotspots")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pollutant contribution – dominant pollutant by normalized severity

# COMMAND ----------

# MAGIC %md
# MAGIC Concentrations use different units, so dominance is based on mean concentration divided
# MAGIC by a practical reference level rather than comparing raw units directly.

# COMMAND ----------

pollutant_frames = [
    gold_city_daily_aqi.select("city", "state", F.lit("PM2.5").alias("pollutant"), (F.col("pm2_5") / F.lit(60.0)).alias("severity_index"), F.col("pm2_5").alias("avg_concentration")),
    gold_city_daily_aqi.select("city", "state", F.lit("PM10").alias("pollutant"), (F.col("pm10") / F.lit(100.0)).alias("severity_index"), F.col("pm10").alias("avg_concentration")),
    gold_city_daily_aqi.select("city", "state", F.lit("NO2").alias("pollutant"), (F.col("no2") / F.lit(80.0)).alias("severity_index"), F.col("no2").alias("avg_concentration")),
    gold_city_daily_aqi.select("city", "state", F.lit("SO2").alias("pollutant"), (F.col("so2") / F.lit(80.0)).alias("severity_index"), F.col("so2").alias("avg_concentration")),
    gold_city_daily_aqi.select("city", "state", F.lit("CO").alias("pollutant"), (F.col("co") / F.lit(2000.0)).alias("severity_index"), F.col("co").alias("avg_concentration")),
    gold_city_daily_aqi.select("city", "state", F.lit("O3").alias("pollutant"), (F.col("o3") / F.lit(100.0)).alias("severity_index"), F.col("o3").alias("avg_concentration")),
]
pollutant_long = reduce(lambda left, right: left.unionByName(right), pollutant_frames).filter(F.col("severity_index").isNotNull())
pollutant_summary = pollutant_long.groupBy("city", "state", "pollutant").agg(F.avg("severity_index").alias("avg_severity_index"), F.avg("avg_concentration").alias("avg_concentration"))
pollutant_window = Window.partitionBy("city").orderBy(F.col("avg_severity_index").desc())
gold_pollutant_contribution = pollutant_summary.withColumn("pollutant_rank", F.row_number().over(pollutant_window)).filter(F.col("pollutant_rank") == 1).withColumnRenamed("pollutant", "dominant_pollutant")
write_gold(gold_pollutant_contribution, f"{GOLD}.gold_pollutant_contribution")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Weather–AQI correlation – Weather Impact page

# COMMAND ----------

hourly_environment = spark.table(SILVER_HOURLY).filter(F.col("aqi").isNotNull())
weather_correlation_summary = (
    hourly_environment.groupBy("city", "state")
    .agg(
        F.count("aqi").alias("hourly_observations"),
        F.avg("aqi").alias("avg_aqi"),
        F.avg("temperature_c").alias("avg_temperature_c"),
        F.avg("humidity_pct").alias("avg_humidity_pct"),
        F.avg("wind_speed_kmh").alias("avg_wind_speed_kmh"),
        F.corr("aqi", "temperature_c").alias("aqi_temperature_correlation"),
        F.corr("aqi", "humidity_pct").alias("aqi_humidity_correlation"),
        F.corr("aqi", "wind_speed_kmh").alias("aqi_wind_correlation"),
    )
)
gold_weather_aqi_correlation = (
    hourly_environment.select("city", "state", "event_ts_local", "event_ts_utc", "observed_date", "aqi", "aqi_category", "temperature_c", "humidity_pct", "wind_speed_kmh", "pressure_hpa", "precipitation_mm")
    .withColumn(
        "wind_bucket",
        F.when(F.col("wind_speed_kmh") < 5, "0–4 km/h")
        .when(F.col("wind_speed_kmh") < 10, "5–9 km/h")
        .when(F.col("wind_speed_kmh") < 20, "10–19 km/h")
        .otherwise("20+ km/h"),
    )
    .join(weather_correlation_summary, on=["city", "state"], how="left")
)
write_gold(gold_weather_aqi_correlation, f"{GOLD}.gold_weather_aqi_correlation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Hourly pattern – hour × weekday heatmap

# COMMAND ----------

gold_hourly_pattern = (
    hourly_environment.withColumn("hour_of_day", F.hour("event_ts_local"))
    .withColumn("weekday_number", ((F.dayofweek("event_ts_local") + 5) % 7) + 1)
    .withColumn("weekday_name", F.date_format("event_ts_local", "E"))
    .groupBy("city", "state", "hour_of_day", "weekday_number", "weekday_name")
    .agg(F.avg("aqi").alias("avg_aqi"), F.max("aqi").alias("max_aqi"), F.count("aqi").alias("hourly_observations"))
    .withColumn("avg_aqi_category", cpcb_category(F.round("avg_aqi").cast("double")))
    .withColumn("is_rush_hour", F.col("hour_of_day").isin(8, 9, 10, 17, 18, 19, 20))
)
write_gold(gold_hourly_pattern, f"{GOLD}.gold_hourly_pattern")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify all Gold outputs

# COMMAND ----------

gold_tables = [
    "gold_city_daily_aqi", "gold_city_monthly_trend", "gold_pollution_hotspots",
    "gold_pollutant_contribution", "gold_weather_aqi_correlation", "gold_hourly_pattern",
]
display(spark.createDataFrame([(f"{GOLD}.{table}", spark.table(f"{GOLD}.{table}").count()) for table in gold_tables], ["gold_table", "row_count"]))