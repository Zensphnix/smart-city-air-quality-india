# Final report outline

## 1. Title page

Project title, university, course, faculty guide, team members, submission date (20 November 2026).

## 2. Abstract

In 150–200 words: state the Smart City pollution-intelligence problem, the three-source Databricks pipeline, Medallion design, Power BI dashboard, and key decision-support outcomes.

## 3. Problem statement and objectives

- Cities need an integrated view of pollution, weather, hotspots, and temporal patterns.
- Objectives: ingest three free sources; create reliable Delta layers; validate quality; deliver an interactive dashboard.

## 4. Scope and assumptions

12 Indian cities; hourly recent Open-Meteo data plus Kaggle daily history; city-level analysis, not station-level or causal forecasting. State that API AQI and historical AQI may use different source methodologies.

## 5. Data sources

Describe each source, variables, granularity, coverage, and attribution. Include the source-credit links from README.

## 6. Architecture and technology stack

Insert `architecture.md` Mermaid export. Explain Python, Databricks Free Edition serverless, Unity Catalog, Volumes, PySpark, Delta Lake, and Power BI.

## 7. Implementation

Use short subsections: extraction and fallback; Bronze; Silver cleaning and MERGE; Gold tables; quality checks; orchestration.

## 8. Dashboard design and insights

Insert Overview, Hotspots, Trends, Weather Impact, Hourly Pattern, City Detail, and tooltip screenshots. Describe no more than three evidence-backed insights from your actual run.

## 9. Testing and data quality

Include `dq_results` screenshot, row-count checks, null/range rules, duplicate tests, freshness threshold, and idempotent rerun result.

## 10. Results

Report actual numbers only: highest average-AQI city, Poor+ days, dominant pollutant, seasonal change, and weather associations. Do not claim causation from correlation.

## 11. Limitations and future work

Modelled/API data versus regulatory-monitor readings; limited historical granularity; no station-level data; no predictive model. Future: CPCB station feed, alerts, forecasting, incremental Auto Loader ingestion.

## 12. Conclusion

Summarize how the solution makes multi-source environmental data usable for city monitoring.

## 13. References and appendix

List source URLs, Databricks and Power BI documentation, screenshots, selected code excerpts, and full data dictionary.
