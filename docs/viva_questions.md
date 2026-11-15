# Viva preparation — likely questions and short answers

1. **Why did you use three sources?**  Air-quality and weather APIs provide complementary hourly recent data, while Kaggle adds longer daily history for trend analysis.
2. **Why store raw data in a Unity Catalog Volume?**  It is governed, workspace-accessible object storage and avoids deprecated DBFS-root patterns.
3. **What happens if Databricks cannot call the API?**  The same extraction scripts run locally; their timestamped output is uploaded to the corresponding Volume folder, then downstream notebooks run unchanged.
4. **Why is Bronze append-only?**  It preserves source lineage and enables reprocessing if transformation rules change.
5. **What metadata is captured in Bronze?**  Source, original file path, source ingestion time, and Bronze load timestamp.
6. **What does Medallion architecture mean here?**  Bronze preserves raw records, Silver creates clean conformed data, and Gold provides business-ready aggregates.
7. **Why use Delta Lake?**  Delta provides ACID reliability, schema-aware tables, time-travel capability, and MERGE support on the lakehouse.
8. **Why use MERGE in Silver?**  The city plus timestamp/date business keys make reruns idempotent: corrected data updates rather than creates duplicates.
9. **How do you handle timezone?**  Open-Meteo timestamps are treated as Asia/Kolkata local time and transformed to UTC; both local and UTC fields are retained.
10. **How are outliers handled?**  Implausible values are converted to null using documented practical ranges, preserving the row for audit rather than silently dropping it.
11. **How are AQI categories derived?**  The pipeline maps AQI 0–50, 51–100, 101–200, 201–300, 301–400, and 401–500 to the CPCB category labels.
12. **How do you ensure quality?**  The validation notebook records row count, null %, range, duplicate, and freshness checks to `monitoring.dq_results` on every run.
13. **How is dominant pollutant calculated?**  Average pollutant concentration is normalized against a practical reference level before ranking, so incompatible raw units are not directly compared.
14. **Does weather correlation prove causation?**  No. It describes association in the observed data only; wind, temperature, and humidity are not controlled experimental variables.
15. **Why use Gold tables in Power BI rather than Silver?**  Gold tables simplify the semantic model, reduce dashboard computation, and expose a stable, business-friendly layer.
