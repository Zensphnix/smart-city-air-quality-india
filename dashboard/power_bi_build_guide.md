# Power BI dashboard build guide

## 1. Connect to Gold tables

1. In Databricks, open **SQL Warehouses**, start your serverless warehouse, then open **Connection details**. Copy **Server hostname** and **HTTP path**.
2. In Power BI Desktop, select **Home > Get data > More…**, search `Databricks`, choose **Azure Databricks**, then select **Connect**.
3. Paste the Server hostname and HTTP path. Under **Advanced options**, enter catalog `smart_city_aqi` and database `gold`; select **Import** for a responsive student-project dashboard.
4. Select Microsoft Entra ID / OAuth sign-in if available in your workspace, then select all six `gold_*` tables in Navigator and choose **Load**.
5. Select **View > Themes > Browse for themes** and import `aqi_dark_theme.json`.

The current connector workflow requires a Databricks Server hostname and HTTP path, then lets you choose Import or DirectQuery and authenticate before choosing tables. [Microsoft Learn](https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi-desktop)

## 2. Model

1. In **Table tools > New table**, create `Date` and `Dim City` from `dax_measures.dax`.
2. Mark `Date[Date]` as a date table.
3. Create one-to-many, single-direction relationships from `Dim City[City]` to the `city` column of every Gold table.
4. Link `Date[Date]` to `gold_city_daily_aqi[observed_date]`, `Date[Date]` to `gold_weather_aqi_correlation[observed_date]`, and `Date[Date]` to `gold_city_monthly_trend[month_start_date]`.
5. Create the listed measures. Sort `Date[Month]` by `Date[Month Number]` and `Date[Weekday]` by `Date[Weekday Number]`.

## 3. Global visual rules

- Canvas: 16:9, 1280 × 720; turn on the 8 px grid and snap all visuals to it.
- Page fill: `#0F172A`; cards: `#1E293B`; borders: `#334155`; primary text: `#F1F5F9`; muted labels: `#94A3B8`; accent: `#38BDF8`.
- Use Segoe UI: page title 24, card number 32 bold, labels 11. Hide visual headers, chart gridlines, and unnecessary legends.
- Use the AQI colors without exceptions: Good `#00B050`, Satisfactory `#92D050`, Moderate `#FFD600`, Poor `#FF9900`, Very Poor `#FF3B30`, Severe `#B71C1C`.
- Keep every page to six or seven visuals. Use rounded 4 px card borders and equal 16 px gutters.

## 4. Page recipes

### Overview

Top bar: text title bound to `Overview Title`; slicers for `Dim City[City]`, `Date[Date]` (Between), `gold_city_daily_aqi[aqi_category]`, and a disconnected pollutant slicer only if added later.

Row 1: four Cards — `Avg AQI`, `Worst City`, `Days Poor+`, `Dominant Pollutant`.

Row 2: left (60%) Azure Maps / Map: Location `city`, Bubble size `Avg AQI`, Legend `aqi_category`, color with the CPCB palette. Right (40%): line chart with `Date[Date]` on X, `Avg AQI` on Y, `Dim City[City]` as legend.

Row 3: left clustered bar, Axis `city`, Value `Avg AQI`, sort descending. Right KPI/insight card with `Winter Insight` and `Last Refreshed Label`.

### Hotspots

Use `gold_pollution_hotspots`. Row 1: ranked horizontal bar (city / avg_aqi); city × month matrix heatmap from `gold_city_monthly_trend` (conditional background by Avg AQI); Top-10 table (city, avg_aqi, max_aqi, days_poor_plus, poor_plus_pct) with AQI conditional formatting. Row 2: dominant pollutant bar from `gold_pollutant_contribution` and a Poor+ percentage card.

### Trends

Use `gold_city_daily_aqi`: daily line (`Date[Date]`, `Avg AQI`) with Analytics-pane trend line; monthly seasonality column chart (`Date[Month]`, Avg AQI); YoY line using `Avg AQI` and `Avg AQI YoY`; compact card for `AQI YoY Change %`.

### Weather Impact

Use `gold_weather_aqi_correlation`: scatter plot with X `temperature_c`, Y `aqi`, Details `city`, Size `humidity_pct`, Play axis `observed_date`; duplicate it for humidity versus AQI if space allows. Add clustered columns for `wind_bucket` versus average AQI and cards for the three correlation columns. Do not use a pie chart.

### Hourly Pattern

Use `gold_hourly_pattern`: Matrix with Rows `weekday_name`, Columns `hour_of_day`, Values `avg_aqi`; apply background conditional formatting using CPCB AQI colors. Add a rush-hour card using `Rush Hour Avg AQI` and a line of hour_of_day versus average AQI.

### City Detail (drill-through)

Create a hidden drill-through page. Add `Dim City[City]` to Drill-through fields and turn on **Keep all filters**. Include a daily AQI line, pollutant cards, dominant pollutant, and a monthly trend. On Overview and Hotspots, right-click a city to drill through.

## 5. UX finishing

1. Create five page-navigator buttons: Overview, Hotspots, Trends, Weather Impact, Hourly Pattern. In **Insert > Buttons > Navigator > Page navigator**, format the selected state with `#38BDF8` accent and inactive state with `#94A3B8` text.
2. Add a Reset filters button: set all slicers to default, open **View > Bookmarks > Add**, name it `Reset filters`, then set the button Action to that bookmark.
3. Make a tooltip page (Page information > Tooltip = On; Canvas type = Tooltip) with mini trend line, Avg AQI, and Poor+ %. Assign it in each major visual's Tooltip settings.
4. Set visual interactions in **Format > Edit interactions** so map, bar, heatmap, and table cross-filter each other. Disable interactions only where they create confusing double filtering.
5. Add `Last Refreshed Label` to the footer on every page. Hide all gridlines and default visual borders that conflict with the theme.

## Screenshot checklist

Take one full-page screenshot for each completed page: Overview, Hotspots, Trends, Weather Impact, Hourly Pattern, City Detail, and the tooltip page. Save them under `screenshots/` with clear names such as `08_powerbi_overview.png`.
