# Presentation outline — 11 slides

| # | Slide | Key content | Suggested speaker |
|---:|---|---|---|
| 1 | Title | Project, team, faculty guide, deadline | Member 1 |
| 2 | Problem and motivation | Why city-level pollution intelligence matters; pain point of fragmented data | Member 1 |
| 3 | Objectives and scope | 12 cities, 3 sources, 5-day implementation scope, non-goals | Member 1 |
| 4 | Architecture | Mermaid diagram; Extraction → Medallion → Power BI | Member 2 |
| 5 | Data sources | Open-Meteo air, Open-Meteo weather, Kaggle history; granularities and fallback | Member 2 |
| 6 | Engineering | Unity Catalog Volume, Bronze append-only, Silver MERGE, Gold tables | Member 2 |
| 7 | Data quality and orchestration | `dq_results`, row/range/freshness checks, Databricks Job | Member 3 |
| 8 | Dashboard overview | Overview screenshot; KPI cards, map, trend, slicers | Member 3 |
| 9 | Analytical pages | Hotspots, Trends, Weather Impact, Hourly Pattern; show strongest actual insight | Member 3 |
| 10 | Results and limitations | Actual observations, no causal claims, data limitations | Member 4 |
| 11 | Conclusion and future work | Business value, next enhancements, thank you / questions | Member 4 |

## Delivery rules

- Spend about 45–60 seconds per slide; target a 9–11 minute presentation.
- Use screenshots with large labels; never show unreadable code on slides.
- Keep every result tied to the dashboard evidence and state the data date range aloud.
- Have each member present at least one technical slide and one outcome slide where possible.
