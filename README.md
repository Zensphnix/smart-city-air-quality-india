# Smart City – Air Quality and Pollution Intelligence (India)

An end-to-end Databricks and Power BI project that analyzes air quality, weather, pollution hotspots, and AQI trends across Indian cities.

## Current progress

Steps 1–8 are complete: the data pipeline, quality checks, orchestration, and Power BI dashboard specification.

## Project structure

```text
.
├── config/                 # Runtime configuration and city reference data
├── dashboard/              # Power BI theme, measures, and build notes
├── docs/                   # Architecture, report, and presentation material
├── notebooks/              # Databricks notebooks run in numeric order
├── screenshots/            # Report-ready screenshots captured during the build
├── src/
│   └── extraction/         # Python API and historical-data extraction scripts
├── requirements.txt        # Local extraction dependencies
└── .gitignore
```

## Step 1 – Databricks setup

1. In Databricks Free Edition, create a Git folder or upload this repository.
2. Import `notebooks/00_setup.py` as a Databricks notebook, selecting **Python**.
3. Run all cells. It creates the `smart_city_aqi` catalog, `raw`, `bronze`, `silver`, `gold`, and `monitoring` schemas, plus the raw-data Volume.
4. Confirm the objects in **Catalog** > `smart_city_aqi`.
5. Upload `config/cities.yaml` to your repository (it is read by later extraction notebooks).

### Screenshot for the report

Capture the Catalog Explorer view showing the five schemas and `raw_air_quality` Volume. Name it `screenshots/01_unity_catalog_setup.png`.

> If your Free Edition workspace does not permit `CREATE CATALOG`, set `CATALOG` in the first notebook cell to the catalog already visible in Catalog Explorer (usually `main`), then rerun the notebook.

## Planned pipeline

Open-Meteo Air Quality API + Open-Meteo Weather API + Kaggle historical AQI CSV → Unity Catalog Volume → Bronze Delta → Silver Delta → Gold Delta → Power BI.

## Step 2 – Raw extraction

1. In Catalog Explorer, open `smart_city_aqi > raw > raw_air_quality > reference` and upload [cities.yaml](config/cities.yaml).
2. Import and run `notebooks/01_extract.py`. Set the date widgets to a recent valid period. It writes two timestamped JSON payloads per city into the Volume.
3. Download `city_day.csv` from Kaggle's *Air Quality Data in India*, then upload it to `raw_air_quality/kaggle_historical/`. Or run the local scripts after configuring a Kaggle API token:

```bash
pip install -r requirements.txt
python -m src.extraction.open_meteo_extract --cities-config config/cities.yaml --start-date 2026-11-08 --end-date 2026-11-14 --output-dir data/raw
python -m src.extraction.kaggle_extract --output-dir data/raw
```

When Databricks blocks external HTTP calls, use the local scripts and upload the generated `data/raw` subfolders to their matching Volume paths.

### Screenshot for the report

Capture `raw_air_quality` showing `open_meteo_air_quality`, `open_meteo_weather`, and `kaggle_historical`. Save it as `screenshots/02_raw_extraction_volume.png`.

## Step 3 – Bronze Delta ingestion

1. Import and run `notebooks/02_bronze.py` after Step 2 has produced raw files.
2. Verify these Delta tables in Catalog Explorer: `bronze_open_meteo_air_quality`, `bronze_open_meteo_weather`, and `bronze_kaggle_city_day`.
3. The notebook safely skips the Kaggle table when the historical CSV has not been uploaded; rerun it after upload to append it.

### Screenshot for the report

Capture the notebook's verification output showing the three Bronze table names and row counts. Save it as `screenshots/03_bronze_delta_tables.png`.

## Step 4 – Silver transformations

1. Import and run `notebooks/03_silver.py` after Bronze tables are populated.
2. Verify `silver_hourly_city_environment` and `silver_city_daily_aqi` in Catalog Explorer.
3. Rerun the notebook once: the row counts should not increase for the same business keys, because the target is updated with Delta `MERGE`.

### Screenshot for the report

Capture the Silver hourly table preview showing city, local/UTC timestamp, AQI category, and weather columns. Save it as `screenshots/04_silver_joined_data.png`.

## Step 5 – Gold dashboard tables

1. Import and run `notebooks/04_gold.py` after Silver completes.
2. In Catalog Explorer, confirm all six required Gold tables: `gold_city_daily_aqi`, `gold_city_monthly_trend`, `gold_pollution_hotspots`, `gold_pollutant_contribution`, `gold_weather_aqi_correlation`, and `gold_hourly_pattern`.
3. These tables are rebuilt from Silver on every run, giving Power BI a single current snapshot.

### Screenshot for the report

Capture the Gold verification table showing all six table names and row counts. Save it as `screenshots/05_gold_tables.png`.

## Step 6 – Data quality validation

1. Import and run `notebooks/05_validation.py` after Gold completes.
2. Open `monitoring.dq_results` in Catalog Explorer. Each run appends its results with a unique run ID and timestamp.
3. Resolve any `FAIL` row before connecting Power BI. A `SKIPPED` Kaggle check is expected until the historical file is uploaded.

### Screenshot for the report

Capture `dq_results` filtered to the latest run ID, including its PASS/FAIL statuses. Save it as `screenshots/06_data_quality_results.png`.

## Step 7 – Orchestration

1. Import `notebooks/06_orchestrate.py` into the same Workspace folder as the numbered notebooks.
2. Run it once interactively, with `run_setup=false` and `run_extraction=true`, to verify the order and the generated `monitoring.pipeline_run_log` entries.
3. Create a serverless Databricks Job using this master notebook as its one task. Follow [the exact click steps](docs/orchestration.md).
4. If external API calls are blocked, upload local raw output and set `run_extraction=false`; all downstream layers still run.

### Screenshot for the report

Capture the Databricks Job run showing a successful master notebook execution. Save it as `screenshots/07_orchestration_job_run.png`.

## Step 8 – Power BI dashboard

1. Follow [the Power BI build guide](dashboard/power_bi_build_guide.md) to load all six Gold tables and create the model, measures, pages, navigation, tooltips, drill-through, and reset bookmark.
2. Import [the dark theme](dashboard/aqi_dark_theme.json), then paste [the DAX script](dashboard/dax_measures.dax) in the order stated at the top of that file.
3. Take the seven dashboard screenshots listed in the guide.

## Data-source credits

| Source | Data used | Credit / access |
|---|---|---|
| Open-Meteo Air Quality API | Hourly PM2.5, PM10, NO2, SO2, CO, O3, US AQI | [Open-Meteo documentation](https://open-meteo.com/en/docs/air-quality-api) |
| Open-Meteo Weather API | Hourly temperature, humidity, wind, pressure, precipitation | [Open-Meteo Weather API](https://open-meteo.com/en/docs) |
| Kaggle: Air Quality Data in India | Historical city-level daily AQI (`city_day.csv`) | [Rohan Rao dataset](https://www.kaggle.com/datasets/rohanrao/air-quality-data-in-india) |

Open-Meteo requests clear attribution to both Open-Meteo and its underlying data providers; retain this table in the final submission.

## Architecture

See the [architecture diagram and layer rationale](docs/architecture.md).

## Technology stack

| Area | Tools |
|---|---|
| Extraction | Python, requests, PyYAML, Kaggle API |
| Storage and engineering | Databricks Free Edition serverless, Unity Catalog, Volumes, PySpark, Delta Lake |
| Quality and orchestration | PySpark validations, Delta audit tables, Databricks Jobs |
| Visualization | Power BI Desktop, DAX, Azure Databricks connector |

## How to run

Run numbered notebooks in order, or use `06_orchestrate.py` after initial setup.

1. `00_setup` — create catalog objects and Volume.
2. `01_extract` — fetch API payloads and automatically create the city reference file; upload the Kaggle CSV only if using historical data.
3. `02_bronze` → `03_silver` → `04_gold` → `05_validation`.
4. `06_orchestrate` — use for repeat runs or as the single Databricks Job task.
5. Connect Power BI to the six Gold tables and follow the dashboard build guide.

## Expected results

The completed dashboard identifies high-AQI cities, Poor+ day counts, dominant pollutants, seasonal movements, weather associations, and hourly rush-period patterns. Record only actual dashboard results in the final report; correlations are not causal evidence.

## Screenshots to include

`01_unity_catalog_setup.png`, `02_raw_extraction_volume.png`, `03_bronze_delta_tables.png`, `04_silver_joined_data.png`, `05_gold_tables.png`, `06_data_quality_results.png`, `07_orchestration_job_run.png`, and the Power BI page screenshots described in the dashboard guide.

## Team contributions

Replace the placeholders before submission.

| Team member | Primary ownership | Supporting work |
|---|---|---|
| Member 1 | Project coordination, requirements, documentation | Testing and presentation |
| Member 2 | Extraction, Unity Catalog, Bronze | Architecture slide |
| Member 3 | Silver/Gold Spark transformations, data quality | Dashboard data model |
| Member 4 | Power BI, DAX, UX, screenshots | Viva preparation |

## Deliverables

- [Architecture](docs/architecture.md)
- [Power BI build guide](dashboard/power_bi_build_guide.md)
- [Final report outline](docs/final_report_outline.md)
- [Presentation outline](docs/presentation_outline.md)
- [Viva preparation](docs/viva_questions.md)
- [Git + Databricks quick start](docs/git_databricks_quickstart.md)
