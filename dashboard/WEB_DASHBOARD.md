# Local website dashboard

This replaces the optional Power BI step. It displays the live Databricks Gold tables in a browser.

## Run it locally

Open PowerShell in the repository folder and run:

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run dashboard/app.py
```

The page opens automatically at `http://localhost:8501`.

## Connect the website to Databricks

In Databricks, open **SQL Warehouses** > your running serverless warehouse > **Connection details**. Copy:

- **Server hostname**
- **HTTP path**

Create a Personal Access Token in **Settings** > **Developer** > **Access tokens**. In the website sidebar, paste those three values. They stay in the local browser session and are not saved in Git.

The website reads these Gold tables:

- `gold_city_daily_aqi`
- `gold_pollution_hotspots`
- `gold_pollutant_contribution`
- `gold_hourly_pattern`
