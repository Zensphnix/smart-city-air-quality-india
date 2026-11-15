# Databricks notebook source
# MAGIC %md
# MAGIC # 06 – Master pipeline orchestration
# MAGIC
# MAGIC Runs the complete pipeline in dependency order and records each notebook execution in
# MAGIC `monitoring.pipeline_run_log`. For scheduled executions, run this notebook as the sole
# MAGIC task in a Databricks Job on serverless compute.

# COMMAND ----------

from datetime import date, datetime, timedelta, timezone
from time import perf_counter
from uuid import uuid4

from pyspark.sql import functions as F

default_start_date = (date.today() - timedelta(days=7)).isoformat()
default_end_date = (date.today() - timedelta(days=1)).isoformat()

dbutils.widgets.text("catalog", "smart_city_aqi", "Unity Catalog name")
dbutils.widgets.text("start_date", default_start_date, "Extraction start date")
dbutils.widgets.text("end_date", default_end_date, "Extraction end date")
dbutils.widgets.dropdown("run_setup", "false", ["false", "true"], "Run 00_setup")
dbutils.widgets.dropdown("run_extraction", "true", ["true", "false"], "Run 01_extract")
dbutils.widgets.text("notebook_base_path", ".", "Relative path to this notebook folder")

CATALOG = dbutils.widgets.get("catalog")
START_DATE = dbutils.widgets.get("start_date")
END_DATE = dbutils.widgets.get("end_date")
RUN_SETUP = dbutils.widgets.get("run_setup") == "true"
RUN_EXTRACTION = dbutils.widgets.get("run_extraction") == "true"
NOTEBOOK_BASE_PATH = dbutils.widgets.get("notebook_base_path").rstrip("/") or "."
RUN_ID = str(uuid4())
RUN_LOG_TABLE = f"{CATALOG}.monitoring.pipeline_run_log"

if date.fromisoformat(START_DATE) > date.fromisoformat(END_DATE):
    raise ValueError("Extraction start date must be on or before end date.")

# COMMAND ----------

def notebook_path(notebook_name: str) -> str:
    """Build a relative notebook reference suitable for imported Databricks source notebooks."""
    return f"{NOTEBOOK_BASE_PATH}/{notebook_name}" if NOTEBOOK_BASE_PATH != "." else f"./{notebook_name}"


def log_step(step_name: str, status: str, started_at: datetime, duration_seconds: float, details: str) -> None:
    """Append one step result immediately so failures remain visible in the run log."""
    log_df = spark.createDataFrame(
        [(RUN_ID, started_at, datetime.now(timezone.utc), step_name, status, round(duration_seconds, 2), details)],
        "run_id string, started_at timestamp, completed_at timestamp, step_name string, status string, duration_seconds double, details string",
    )
    log_df.write.format("delta").mode("append").saveAsTable(RUN_LOG_TABLE)


def run_step(step_name: str, arguments: dict[str, str] | None = None) -> None:
    """Run one notebook, log outcome, and stop the master pipeline on an error."""
    started_at = datetime.now(timezone.utc)
    timer = perf_counter()
    try:
        dbutils.notebook.run(notebook_path(step_name), timeout_seconds=0, arguments=arguments or {})
        log_step(step_name, "SUCCESS", started_at, perf_counter() - timer, "Notebook completed.")
    except Exception as error:
        log_step(step_name, "FAILED", started_at, perf_counter() - timer, str(error)[:2000])
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute in dependency order

# COMMAND ----------

if RUN_SETUP:
    run_step("00_setup")

if RUN_EXTRACTION:
    run_step("01_extract", {"catalog": CATALOG, "start_date": START_DATE, "end_date": END_DATE})

run_step("02_bronze", {"catalog": CATALOG})
run_step("03_silver", {"catalog": CATALOG})
run_step("04_gold", {"catalog": CATALOG})
run_step("05_validation", {"catalog": CATALOG})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review this pipeline run

# COMMAND ----------

display(
    spark.table(RUN_LOG_TABLE)
    .filter(F.col("run_id") == RUN_ID)
    .orderBy("started_at")
)
