"""Local web dashboard for the Smart City Air Quality project.

Run from the repository root:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
import streamlit.components.v1 as components
from databricks import sql


CATALOG = "smart_city_aqi"
GOLD = f"{CATALOG}.gold"
PAGES = (
    "Dashboard", "City Explorer", "Pollution Map", "Pollutant Analysis",
    "Weather & Pollution", "Historical Trends", "Pipeline Health",
)

st.set_page_config(
    page_title="Smart City AQI",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
      :root { --ink: #e7eef9; --muted: #8ea0b8; --panel: rgba(20, 32, 55, .82); --edge: rgba(105, 181, 255, .16); --cyan: #51d6ff; }
      .stApp {
        background: radial-gradient(circle at 85% -15%, #153a5d 0, transparent 31%), radial-gradient(circle at 23% 8%, #17294d 0, transparent 28%), #091323;
        color: var(--ink);
      }
      [data-testid="stHeader"] { background: transparent; }
      #MainMenu, footer { visibility: hidden; }
      [data-testid="stSidebar"] { background: linear-gradient(180deg, #111b2c 0%, #0a1220 100%); border-right: 1px solid var(--edge); }
      [data-testid="stSidebar"] > div:first-child { padding-top: 1.4rem; }
      [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 { color: #f7fbff; }
      [data-testid="stSidebar"] [data-testid="stCaptionContainer"] { color: var(--muted); }
      [data-testid="stSidebar"] input { background: #101d31 !important; border: 1px solid #294260 !important; border-radius: 9px !important; color: #f7fbff !important; }
      [data-testid="stSidebar"] input:focus { border-color: var(--cyan) !important; box-shadow: 0 0 0 2px rgba(81, 214, 255, .15) !important; }
      [data-testid="stSidebar"] [role="radiogroup"] label { border-radius: 8px; padding: .2rem .25rem; transition: background .2s ease, transform .2s ease; }
      [data-testid="stSidebar"] [role="radiogroup"] label:hover { background: rgba(81, 214, 255, .1); transform: translateX(2px); }
      [data-testid="stSidebar"] button { width: 100%; border-radius: 9px; border: 1px solid rgba(81, 214, 255, .55); color: #dff7ff; background: linear-gradient(135deg, #123958, #174e70); transition: transform .18s ease, box-shadow .18s ease; }
      [data-testid="stSidebar"] button:hover { transform: translateY(-1px); box-shadow: 0 10px 22px rgba(16, 136, 204, .18); }
      [data-testid="stMetric"] {
        background: linear-gradient(145deg, rgba(28, 49, 79, .94), rgba(15, 29, 49, .94));
        border: 1px solid var(--edge); border-radius: 15px; padding: 18px;
        min-height: 126px; height: 100%; box-sizing: border-box;
        box-shadow: 0 16px 35px rgba(0, 0, 0, .18); animation: rise .55s ease both;
      }
      [data-testid="stMetricLabel"] { color: #9eb1ca; letter-spacing: .02em; }
      [data-testid="stMetricValue"] { color: #f7fbff; }
      h1, h2, h3 { color: #f1f5f9; letter-spacing: -.02em; }
      [data-testid="stDataFrame"], [data-testid="stPlotlyChart"] { border: 1px solid var(--edge); border-radius: 14px; overflow: hidden; background: rgba(17, 31, 53, .78); box-shadow: 0 16px 35px rgba(0, 0, 0, .14); }
      [data-testid="stAlert"] { border-radius: 12px; border: 1px solid var(--edge); }
      .hero { position: relative; overflow: hidden; padding: 1.7rem 2rem; margin: .1rem 0 1.4rem; border: 1px solid var(--edge); border-radius: 19px; background: linear-gradient(115deg, rgba(18, 47, 78, .96), rgba(16, 29, 54, .92) 58%, rgba(12, 24, 43, .9)); box-shadow: 0 18px 48px rgba(0, 0, 0, .2); animation: rise .6s ease both; }
      .hero:after { content: ''; position: absolute; width: 360px; height: 360px; border-radius: 50%; right: -105px; top: -235px; border: 46px solid rgba(81, 214, 255, .09); }
      .eyebrow { margin: 0 0 .35rem; color: #72ddff; font-size: .74rem; font-weight: 800; letter-spacing: .14em; }
      .hero h1 { position: relative; margin: 0; font-size: clamp(1.55rem, 3vw, 2.45rem); z-index: 1; }
      .hero p.copy { position: relative; margin: .45rem 0 0; color: #adc1d8; z-index: 1; }
      .status { position: absolute; right: 2rem; bottom: 1.75rem; color: #b9f8da; font-size: .8rem; z-index: 1; }
      .status:before { content: ''; display: inline-block; width: 8px; height: 8px; background: #4de4a6; border-radius: 50%; margin-right: 7px; box-shadow: 0 0 0 4px rgba(77, 228, 166, .12); animation: pulse 1.8s infinite; }
      .section-kicker { color: #77dfff; font-size: .75rem; text-transform: uppercase; letter-spacing: .13em; font-weight: 800; margin-bottom: -.4rem; }
      .empty-state { margin-top: 2rem; max-width: 670px; padding: 2.2rem; border-radius: 16px; border: 1px dashed rgba(113, 202, 255, .34); background: rgba(18, 34, 57, .62); animation: rise .45s ease both; }
      .empty-state h2 { margin-top: 0; } .empty-state p { color: #aabbd1; line-height: 1.65; }
      .metro-card { height: 145px; box-sizing: border-box; margin: .38rem 0; padding: 1rem; border-radius: 16px; border: 1px solid rgba(148, 163, 184, .17); background: linear-gradient(145deg, rgba(38, 48, 63, .95), rgba(23, 33, 47, .95)); transition: transform .22s ease, border-color .22s ease, box-shadow .22s ease; animation: rise .55s ease both; }
      .metro-card:hover { transform: translateY(-4px); border-color: rgba(81, 214, 255, .6); box-shadow: 0 18px 35px rgba(0,0,0,.25); }
      .metro-icon { float: left; width: 44px; height: 44px; display: grid; place-items: center; font-size: 1.6rem; margin: .1rem .75rem 0 0; border-radius: 12px; background: rgba(99, 190, 255, .1); }
      .metro-city { color: #f7fbff; font-size: 1rem; font-weight: 800; margin: 0; }.metro-aqi { margin: .18rem 0; font-size: 1.7rem; line-height: 1; font-weight: 850; }.metro-meta { color: #a9bad0; font-size: .76rem; margin-top: .65rem; }
      .aqi-scale { display: flex; overflow: hidden; border-radius: 999px; height: 9px; margin-top: .45rem; }.aqi-scale span { flex: 1; }.aqi-labels { display: flex; justify-content: space-between; color: #9aacbf; font-size: .72rem; margin-top: .38rem; }.legend-shell { padding: 1rem 1.25rem; border: 1px solid rgba(148, 163, 184, .2); border-radius: 14px; background: rgba(14, 26, 42, .75); margin: .8rem 0 1.2rem; }.legend-top { display: flex; justify-content: space-between; color: #c9d8e8; font-size: .78rem; font-weight: 700; }
      .health-panel { padding: 1.35rem; border-radius: 16px; border: 1px solid rgba(81, 214, 255, .24); background: linear-gradient(135deg, rgba(18, 67, 91, .45), rgba(31, 37, 57, .68)); }.health-panel h3 { margin-top: 0; }.health-panel p { color: #c5d4e4; line-height: 1.6; }
      .live-command { position: relative; overflow: hidden; margin: .45rem 0 1.1rem; padding: 1.15rem 1.35rem; border: 1px solid rgba(81, 214, 255, .25); border-radius: 16px; background: linear-gradient(112deg, rgba(22, 73, 105, .58), rgba(22, 33, 55, .86)); }
      .live-command:after { content: ''; position: absolute; right: -42px; top: -80px; width: 210px; height: 210px; border-radius: 50%; border: 30px solid rgba(81, 214, 255, .09); }
      .live-command h3 { position: relative; margin: .15rem 0 .28rem; color: #f7fbff; }.live-command p { position: relative; margin: 0; color: #bad1e5; }.live-command b { color: #8ff4c6; }
      .city-rank { display: flex; align-items: center; gap: .75rem; padding: .65rem 0; border-bottom: 1px solid rgba(148, 163, 184, .13); }.city-rank:last-child { border-bottom: 0; }.rank-number { width: 25px; color: #7cdfff; font-weight: 800; }.rank-city { flex: 1; color: #edf7ff; font-weight: 700; }.rank-aqi { font-weight: 850; }
      @keyframes rise { from { opacity: 0; transform: translateY(12px); } to { opacity: 1; transform: translateY(0); } }
      @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: .42; } }
      @media (max-width: 700px) { .hero { padding: 1.3rem; } .status { position: relative; right: auto; bottom: auto; display: block; margin-top: .8rem; } }
    </style>
    """,
    unsafe_allow_html=True,
)


def setting(name: str) -> str:
    """Prefer Streamlit secrets; otherwise read an environment variable."""
    try:
        value = st.secrets.get(name, "")
    except FileNotFoundError:
        value = ""
    return str(value or os.getenv(name, ""))


@st.cache_data(ttl=600, show_spinner="Loading Databricks Gold tables…")
def run_query(
    hostname: str, http_path: str, auth_method: str, token: str, query: str
) -> pd.DataFrame:
    connection_options: dict[str, str] = {
        "server_hostname": hostname.removeprefix("https://").rstrip("/"),
        "http_path": http_path.strip(),
    }
    if auth_method == "Sign in with Databricks":
        connection_options["auth_type"] = "databricks-oauth"
    else:
        connection_options["access_token"] = token.strip()

    with sql.connect(**connection_options) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()


def load_data(
    hostname: str, http_path: str, auth_method: str, token: str
) -> dict[str, pd.DataFrame]:
    return {
        "daily": run_query(hostname, http_path, auth_method, token, f"SELECT * FROM {GOLD}.gold_city_daily_aqi"),
        "hotspots": run_query(hostname, http_path, auth_method, token, f"SELECT * FROM {GOLD}.gold_pollution_hotspots"),
        "pollutants": run_query(hostname, http_path, auth_method, token, f"SELECT * FROM {GOLD}.gold_pollutant_contribution"),
        "hourly": run_query(hostname, http_path, auth_method, token, f"SELECT * FROM {GOLD}.gold_hourly_pattern"),
        "monthly": run_query(hostname, http_path, auth_method, token, f"SELECT * FROM {GOLD}.gold_city_monthly_trend"),
        "weather": run_query(hostname, http_path, auth_method, token, f"SELECT * FROM {GOLD}.gold_weather_aqi_correlation"),
        "pipeline": run_query(hostname, http_path, auth_method, token, f"SELECT * FROM {CATALOG}.monitoring.pipeline_run_log"),
        "quality": run_query(hostname, http_path, auth_method, token, f"SELECT * FROM {CATALOG}.monitoring.dq_results"),
    }


def aqi_colour(aqi: float) -> str:
    if aqi <= 50:
        return "#00b050"
    if aqi <= 100:
        return "#92d050"
    if aqi <= 200:
        return "#ffd600"
    if aqi <= 300:
        return "#ff9900"
    if aqi <= 400:
        return "#ff3b30"
    return "#b71c1c"


CITY_COORDINATES = {
    "Delhi": (28.6139, 77.2090), "Gurugram": (28.4595, 77.0266),
    "Mumbai": (19.0760, 72.8777), "Kolkata": (22.5726, 88.3639),
    "Chennai": (13.0827, 80.2707), "Bengaluru": (12.9716, 77.5946),
    "Hyderabad": (17.3850, 78.4867), "Pune": (18.5204, 73.8567),
    "Ahmedabad": (23.0225, 72.5714), "Lucknow": (26.8467, 80.9462),
    "Jaipur": (26.9124, 75.7873), "Patna": (25.5941, 85.1376),
}
CITY_STATES = {
    "Delhi": "Delhi", "Gurugram": "Haryana", "Mumbai": "Maharashtra",
    "Kolkata": "West Bengal", "Chennai": "Tamil Nadu", "Bengaluru": "Karnataka",
    "Hyderabad": "Telangana", "Pune": "Maharashtra", "Ahmedabad": "Gujarat",
    "Lucknow": "Uttar Pradesh", "Jaipur": "Rajasthan", "Patna": "Bihar",
}


def dark_chart(fig: object, title: str) -> object:
    fig.update_layout(
        title=title, paper_bgcolor="#1e293b", plot_bgcolor="#1e293b",
        font_color="#f1f5f9", margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def us_aqi_category(value: float) -> str:
    if value <= 50:
        return "Good"
    if value <= 100:
        return "Moderate"
    if value <= 150:
        return "Unhealthy for sensitive groups"
    if value <= 200:
        return "Unhealthy"
    if value <= 300:
        return "Very unhealthy"
    return "Hazardous"


@st.cache_data(ttl=300, show_spinner="Fetching the latest city conditions from Open-Meteo…")
def fetch_live_conditions() -> pd.DataFrame:
    """Fetch model-based current air quality and weather for all project cities."""
    cities = list(CITY_COORDINATES)
    latitudes = ",".join(str(CITY_COORDINATES[city][0]) for city in cities)
    longitudes = ",".join(str(CITY_COORDINATES[city][1]) for city in cities)
    common = {"latitude": latitudes, "longitude": longitudes, "timezone": "Asia/Kolkata"}
    air = requests.get(
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        params={
            **common,
            "current": "us_aqi,european_aqi,pm2_5,pm10,nitrogen_dioxide,sulphur_dioxide,ozone,carbon_monoxide",
        },
        timeout=25,
    )
    weather = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            **common,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
        },
        timeout=25,
    )
    air.raise_for_status()
    weather.raise_for_status()
    air_items = air.json()
    weather_items = weather.json()
    if isinstance(air_items, dict):
        air_items = [air_items]
    if isinstance(weather_items, dict):
        weather_items = [weather_items]

    rows = []
    for city, air_item, weather_item in zip(cities, air_items, weather_items):
        a = air_item.get("current", {})
        w = weather_item.get("current", {})
        aqi = float(a.get("us_aqi") or 0)
        rows.append(
            {
                "city": city,
                "state": CITY_STATES[city],
                "latitude": CITY_COORDINATES[city][0],
                "longitude": CITY_COORDINATES[city][1],
                "us_aqi": aqi,
                "european_aqi": a.get("european_aqi"),
                "aqi_category": us_aqi_category(aqi),
                "pm2_5": a.get("pm2_5"), "pm10": a.get("pm10"),
                "no2": a.get("nitrogen_dioxide"), "so2": a.get("sulphur_dioxide"),
                "o3": a.get("ozone"), "co": a.get("carbon_monoxide"),
                "temperature_c": w.get("temperature_2m"),
                "humidity_pct": w.get("relative_humidity_2m"),
                "wind_speed_kmh": w.get("wind_speed_10m"),
                "precipitation_mm": w.get("precipitation"),
                "observed_at": a.get("time") or w.get("time"),
            }
        )
    return pd.DataFrame(rows)


AQI_COLORS = {
    "Good": "#56c724", "Moderate": "#f1d239", "Unhealthy for sensitive groups": "#fb9a28",
    "Unhealthy": "#f15476", "Very unhealthy": "#b74ac6", "Hazardous": "#8d2537",
}
CITY_SYMBOLS = {"Delhi": "🏛️", "Gurugram": "🏙️", "Mumbai": "🌉", "Kolkata": "🚋", "Chennai": "🌊", "Bengaluru": "🌳", "Hyderabad": "🏰", "Pune": "⛰️", "Ahmedabad": "🪁", "Lucknow": "🕌", "Jaipur": "🩷", "Patna": "🌉"}


def aqi_legend() -> None:
    st.markdown(
        """
        <section class="legend-shell">
          <div class="legend-top"><span>Good</span><span>Moderate</span><span>Sensitive groups</span><span>Unhealthy</span><span>Very unhealthy</span><span>Hazardous</span></div>
          <div class="aqi-scale"><span style="background:#56c724"></span><span style="background:#f1d239"></span><span style="background:#fb9a28"></span><span style="background:#f15476"></span><span style="background:#b74ac6"></span><span style="background:#8d2537"></span></div>
          <div class="aqi-labels"><span>0</span><span>50</span><span>100</span><span>150</span><span>200</span><span>300</span><span>301+</span></div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def health_message(aqi: float) -> str:
    if aqi <= 50:
        return "Air quality is generally satisfactory. Normal outdoor activity is appropriate for most people."
    if aqi <= 100:
        return "Air quality is acceptable. People unusually sensitive to pollution may reduce prolonged or heavy outdoor exertion."
    if aqi <= 150:
        return "Sensitive groups may be affected. Consider reducing prolonged or heavy outdoor exertion if you have heart or lung conditions, or are an older adult or child."
    if aqi <= 200:
        return "Air quality is unhealthy. Consider reducing prolonged outdoor exertion; sensitive groups should be especially cautious."
    if aqi <= 300:
        return "Health alert conditions: sensitive groups should avoid prolonged outdoor exertion, and others should limit it."
    return "Health warning conditions: limit outdoor exposure and follow local public-health guidance."


def render_live_page(page: str, live: pd.DataFrame) -> None:
    """Render pages that use the latest provider values without Databricks credentials."""
    with st.sidebar:
        st.divider()
        city_focus = st.selectbox(
            "Cities",
            ["All cities", *live["city"].tolist()],
            help="View all cities together, or focus every live chart on one city.",
        )
    live = live.copy() if city_focus == "All cities" else live.query("city == @city_focus").copy()
    if live.empty:
        st.warning("The selected city is not available in the latest live update. Click Refresh live data and try again.")
        return

    timestamp = live["observed_at"].dropna().iat[0] if live["observed_at"].notna().any() else "latest provider update"
    st.markdown(f'<p class="section-kicker">LIVE OPEN-METEO FEED · UPDATED {timestamp}</p><h2>{page}</h2>', unsafe_allow_html=True)

    if page == "Dashboard":
        worst = live.loc[live["us_aqi"].idxmax()]
        best = live.loc[live["us_aqi"].idxmin()]
        top_pm = live[["pm2_5", "pm10", "no2", "so2", "o3"]].mean().idxmax().replace("pm2_5", "PM2.5").upper()
        category_counts = live["aqi_category"].value_counts()
        affected_cities = int((live["us_aqi"] > 100).sum())
        st.markdown(
            f"""<section class='live-command'>
            <p class='section-kicker'>LIVE COMMAND CENTER</p>
            <h3>{worst.city} currently has the highest AQI: <b>{worst.us_aqi:.0f}</b> ({worst.aqi_category})</h3>
            <p>{affected_cities} of {len(live)} monitored cities are above AQI 100. The main average pollutant signal is <b>{top_pm}</b>.</p>
            </section>""",
            unsafe_allow_html=True,
        )
        a, b, c, d = st.columns(4)
        a.metric("Cities monitored", len(live))
        b.metric("Highest US AQI", f"{worst.us_aqi:.0f}", worst.city)
        c.metric("Lowest US AQI", f"{best.us_aqi:.0f}", best.city)
        d.metric("Dominant average pollutant", top_pm)
        st.markdown("<p class='section-kicker'>INDIA'S METRO CITIES</p><h3>Current air quality at a glance</h3>", unsafe_allow_html=True)
        for start in range(0, len(live), 4):
            cards = st.columns(min(4, len(live.iloc[start:start + 4])))
            for column, (_, row) in zip(cards, live.iloc[start:start + 4].iterrows()):
                colour = AQI_COLORS.get(row.aqi_category, "#f1f5f9")
                with column:
                    st.markdown(
                        f"""<article class='metro-card'>
                        <span class='metro-icon'>{CITY_SYMBOLS.get(row.city, '📍')}</span>
                        <p class='metro-city'>{row.city}</p>
                        <p class='metro-aqi' style='color:{colour}'>{row.us_aqi:.0f}</p>
                        <p class='metro-meta'>Temp. <b>{row.temperature_c:.0f}°C</b> &nbsp;·&nbsp; Hum. <b>{row.humidity_pct:.0f}%</b></p>
                        </article>""",
                        unsafe_allow_html=True,
                    )
        aqi_legend()
        left, right = st.columns((3, 2))
        with left:
            chart = px.bar(live.sort_values("us_aqi"), x="city", y="us_aqi", color="aqi_category", text="us_aqi", color_discrete_map=AQI_COLORS)
            chart.update_traces(texttemplate="%{text:.0f}", textposition="outside")
            st.plotly_chart(dark_chart(chart, "Latest US AQI by city"), use_container_width=True)
        with right:
            ranking = live.sort_values("us_aqi", ascending=False).head(5)
            ranking_rows = "".join(
                f"<div class='city-rank'><span class='rank-number'>{position}</span><span class='rank-city'>{row.city}</span><span class='rank-aqi' style='color:{AQI_COLORS.get(row.aqi_category, '#f7fbff')}'>{row.us_aqi:.0f}</span></div>"
                for position, (_, row) in enumerate(ranking.iterrows(), start=1)
            )
            st.markdown(f"<section class='health-panel'><p class='section-kicker'>TOP 5 CURRENT AQI</p><h3>City ranking</h3>{ranking_rows}</section>", unsafe_allow_html=True)
            category_frame = category_counts.rename_axis("category").reset_index(name="cities")
            donut = px.pie(category_frame, names="category", values="cities", hole=.62, color="category", color_discrete_map=AQI_COLORS)
            donut.update_layout(margin=dict(l=5, r=5, t=30, b=5), paper_bgcolor="#1e293b", font_color="#f1f5f9", title="AQI categories")
            st.plotly_chart(donut, use_container_width=True)
        st.dataframe(live[["city", "us_aqi", "european_aqi", "pm2_5", "pm10", "no2", "temperature_c", "observed_at"]], use_container_width=True, hide_index=True)

    elif page == "City Explorer":
        city = st.selectbox("Choose a city", live["city"].tolist())
        row = live.query("city == @city").iloc[0]
        a, b, c, d = st.columns(4)
        a.metric("US AQI", f"{row.us_aqi:.0f}", row.aqi_category)
        b.metric("PM2.5", f"{row.pm2_5:.1f} µg/m³")
        c.metric("Temperature", f"{row.temperature_c:.1f} °C")
        d.metric("Humidity", f"{row.humidity_pct:.0f}%")
        pollutants = pd.DataFrame({"Pollutant": ["PM2.5", "PM10", "NO₂", "SO₂", "O₃", "CO"], "Concentration": [row.pm2_5, row.pm10, row.no2, row.so2, row.o3, row.co]})
        st.plotly_chart(dark_chart(px.bar(pollutants, x="Pollutant", y="Concentration", color="Pollutant"), f"{city}: current pollutant profile"), use_container_width=True)
        st.markdown(
            f"""<section class='health-panel'>
            <p class='section-kicker'>AQI-BASED OUTDOOR ACTIVITY NOTE</p>
            <h3>{row.aqi_category}</h3>
            <p>{health_message(float(row.us_aqi))}</p>
            <p style='font-size:.78rem;color:#9eafc4'>General public-information guidance only—not medical advice. Consult a qualified professional for personal health concerns.</p>
            </section>""",
            unsafe_allow_html=True,
        )
        aqi_legend()

    elif page == "Pollution Map":
        fig = px.scatter_map(live, lat="latitude", lon="longitude", size="us_aqi", color="us_aqi", hover_name="city", hover_data=["aqi_category", "pm2_5", "pm10"], color_continuous_scale=["#00b050", "#ffd600", "#ff9900", "#b71c1c"], zoom=3.6, center={"lat": 22.5, "lon": 79.0}, height=610)
        fig.update_layout(map_style="carto-darkmatter", margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)

    elif page == "Pollutant Analysis":
        pollutant_columns = {"pm2_5": "PM2.5", "pm10": "PM10", "no2": "NO₂", "so2": "SO₂", "o3": "O₃", "co": "CO"}
        long = live.melt(id_vars="city", value_vars=list(pollutant_columns), var_name="pollutant", value_name="concentration")
        long["pollutant"] = long["pollutant"].map(pollutant_columns)
        st.plotly_chart(dark_chart(px.bar(long, x="city", y="concentration", color="pollutant", barmode="group"), "Current pollutant concentrations by city"), use_container_width=True)

    elif page == "Weather & Pollution":
        left, right = st.columns(2)
        with left:
            st.plotly_chart(dark_chart(px.scatter(live, x="temperature_c", y="us_aqi", size="humidity_pct", color="city", hover_name="city"), "Temperature versus current AQI"), use_container_width=True)
        with right:
            st.plotly_chart(dark_chart(px.scatter(live, x="wind_speed_kmh", y="us_aqi", size="precipitation_mm", color="city", hover_name="city"), "Wind speed versus current AQI"), use_container_width=True)

    elif page == "Historical Trends":
        st.info("The Live mode shows the latest conditions only. Switch to Databricks Gold mode to explore historical trends produced by your pipeline.")
        st.dataframe(live[["city", "observed_at", "us_aqi", "aqi_category"]], use_container_width=True, hide_index=True)

    else:
        st.success("Live provider connection is healthy.")
        st.write(f"Last provider observation: **{timestamp}** · Refresh interval: **5 minutes**")
        st.info("Databricks pipeline run logs and data-quality checks are available in Databricks Gold mode.")

    st.caption("Live values are Open-Meteo/CAMS model estimates, updated by the provider. They are not official CPCB station observations.")


st.markdown(
    """
    <section class="hero">
      <p class="eyebrow">SMART CITY OBSERVATORY</p>
      <h1>Air Quality & Pollution Intelligence</h1>
      <p class="copy">Explore city-level pollution patterns, weather signals, and data-pipeline health across India.</p>
      <span class="status">LOCAL DASHBOARD</span>
    </section>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown("### 🌿 AirSight")
    st.caption("Environmental intelligence platform")
    st.divider()
    data_mode = st.radio("Data mode", ("Current AQI · Live", "Databricks Gold · Historical"))
    hostname = http_path = ""
    token = ""
    auth_method = "Personal access token"
    if data_mode == "Databricks Gold · Historical":
        st.markdown("#### Databricks connection")
        hostname = st.text_input(
            "Server hostname", value=setting("DATABRICKS_SERVER_HOSTNAME"),
            placeholder="dbc-xxxxxxxx.cloud.databricks.com",
        )
        http_path = st.text_input(
            "HTTP path", value=setting("DATABRICKS_HTTP_PATH"),
            placeholder="/sql/1.0/warehouses/…",
        )
        auth_method = st.radio("Sign-in method", ("Personal access token", "Sign in with Databricks"))
        if auth_method == "Personal access token":
            token = st.text_input("Personal access token", value=setting("DATABRICKS_TOKEN"), type="password")
        st.caption("Connection values are used only by this local browser session.")
    else:
        st.caption("Current Open-Meteo/CAMS model estimates. Updates on refresh and is cached for up to five minutes.")
    refresh = st.button("Refresh current AQI" if data_mode == "Current AQI · Live" else "Refresh Databricks data")
    st.divider()
    page = st.radio("Navigate", PAGES)

if refresh:
    st.cache_data.clear()

if data_mode == "Current AQI · Live":
    try:
        render_live_page(page, fetch_live_conditions())
        components.html("<script>setTimeout(() => window.parent.location.reload(), 300000);</script>", height=0)
    except requests.RequestException:
        st.error("The live data provider is temporarily unavailable. Click Refresh live data to try again.")
    st.stop()

if not all([hostname, http_path]) or (auth_method == "Personal access token" and not token):
    st.markdown(
        """
        <section class="empty-state">
          <p class="eyebrow">CONNECT YOUR DATA</p>
          <h2>Your live dashboard is ready</h2>
          <p>Enter the Databricks SQL Warehouse server hostname and HTTP path in the sidebar, then select a sign-in method. Your seven dashboard sections will populate with your Gold and monitoring tables.</p>
        </section>
        """,
        unsafe_allow_html=True,
    )
    st.stop()

try:
    data = load_data(hostname, http_path, auth_method, token)
except Exception:
    st.markdown(
        """
        <section class="empty-state">
          <p class="eyebrow">CONNECTION NEEDS ATTENTION</p>
          <h2>We could not load your tables</h2>
          <p>Confirm that the server hostname ends in <b>.cloud.databricks.com</b>, copy the HTTP path again from Databricks Connection details, and ensure the SQL warehouse is running.</p>
          <p>If browser sign-in hangs, select <b>Personal access token</b> and paste a freshly generated token.</p>
        </section>
        """,
        unsafe_allow_html=True,
    )
    st.stop()

daily = data["daily"].copy()
daily["observed_date"] = pd.to_datetime(daily["observed_date"])
cities = sorted(daily["city"].dropna().unique().tolist())

with st.sidebar:
    st.divider()
    city_focus = st.selectbox(
        "Cities",
        ["All cities", *cities],
        help="View all available history, or focus charts on one city.",
    )
    start_date = st.date_input("From", value=daily["observed_date"].min().date())
    end_date = st.date_input("To", value=daily["observed_date"].max().date())

filtered = daily[
    ((daily["city"] == city_focus) if city_focus != "All cities" else daily["city"].isin(cities))
    & daily["observed_date"].between(pd.Timestamp(start_date), pd.Timestamp(end_date))
].copy()

if filtered.empty:
    st.warning("No AQI records match these filters.")
    st.stop()

active_cities = cities if city_focus == "All cities" else [city_focus]
hotspots = data["hotspots"].query("city in @active_cities").copy()
pollutants = data["pollutants"].query("city in @active_cities").copy()
hourly = data["hourly"].query("city in @active_cities").copy()
monthly = data["monthly"].query("city in @active_cities").copy()
weather = data["weather"].query("city in @active_cities").copy()

PAGE_DESCRIPTIONS = {
    "Dashboard": "A clear executive view of AQI, pollution patterns, and city comparisons.",
    "City Explorer": "Investigate a single city and compare its AQI profile.",
    "Pollution Map": "Locate the cities with the highest average pollution load.",
    "Pollutant Analysis": "See which pollutant drives the greatest normalized severity.",
    "Weather & Pollution": "Explore the observed relationship between weather and AQI.",
    "Historical Trends": "Follow month-level movement and hour-by-weekday patterns.",
    "Pipeline Health": "Monitor data-quality checks and Databricks orchestration steps.",
}
st.markdown(f'<p class="section-kicker">LIVE ANALYTICS</p><h2>{page}</h2>', unsafe_allow_html=True)
st.caption(PAGE_DESCRIPTIONS[page])

if page == "Dashboard":
    avg_aqi = filtered["aqi"].mean()
    worst_row = filtered.loc[filtered["aqi"].idxmax()]
    poor_days = int((filtered["aqi"] >= 201).sum())
    dominant_label = pollutants["dominant_pollutant"].mode().iat[0] if not pollutants.empty else "—"
    metric1, metric2, metric3, metric4 = st.columns(4)
    metric1.metric("Average AQI", f"{avg_aqi:.0f}")
    metric2.metric("Worst reading", f"{worst_row['aqi']:.0f}", worst_row["city"])
    metric3.metric("Poor+ observations", poor_days)
    metric4.metric("Most common dominant pollutant", dominant_label)
    left, right = st.columns((3, 2))
    with left:
        trend = filtered.groupby(["observed_date", "city"], as_index=False)["aqi"].mean()
        st.plotly_chart(dark_chart(px.line(trend, x="observed_date", y="aqi", color="city", markers=True), "Daily AQI trend"), use_container_width=True)
    with right:
        hotspot_chart = hotspots.sort_values("avg_aqi", ascending=True)
        fig = px.bar(hotspot_chart, x="avg_aqi", y="city", orientation="h", color="avg_aqi", color_continuous_scale=["#00b050", "#ffd600", "#ff9900", "#b71c1c"])
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(dark_chart(fig, "Pollution hotspot ranking"), use_container_width=True)

elif page == "City Explorer":
    city = st.selectbox("Choose a city", active_cities)
    city_daily = filtered.query("city == @city")
    city_hotspot = hotspots.query("city == @city").iloc[0]
    a, b, c = st.columns(3)
    a.metric("Average AQI", f"{city_daily.aqi.mean():.0f}")
    b.metric("Maximum AQI", f"{city_daily.aqi.max():.0f}")
    c.metric("Poor+ observations", int((city_daily.aqi >= 201).sum()))
    st.plotly_chart(dark_chart(px.line(city_daily, x="observed_date", y="aqi", markers=True), f"{city}: daily AQI"), use_container_width=True)
    st.dataframe(pd.DataFrame([city_hotspot]).drop(columns=["state"], errors="ignore"), use_container_width=True, hide_index=True)

elif page == "Pollution Map":
    mapped = hotspots.copy()
    mapped["latitude"] = mapped["city"].map(lambda city: CITY_COORDINATES.get(city, (None, None))[0])
    mapped["longitude"] = mapped["city"].map(lambda city: CITY_COORDINATES.get(city, (None, None))[1])
    mapped = mapped.dropna(subset=["latitude", "longitude"])
    fig = px.scatter_map(
        mapped, lat="latitude", lon="longitude", size="avg_aqi", color="avg_aqi", hover_name="city",
        hover_data=["max_aqi", "days_poor_plus"], color_continuous_scale=["#00b050", "#ffd600", "#ff9900", "#b71c1c"], zoom=3.6, center={"lat": 22.5, "lon": 79.0}, height=600,
    )
    fig.update_layout(map_style="carto-darkmatter", margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Bubble size and colour both represent average AQI. Select a city in the sidebar to filter the map.")

elif page == "Pollutant Analysis":
    pollutant_counts = pollutants["dominant_pollutant"].value_counts().rename_axis("pollutant").reset_index(name="cities")
    left, right = st.columns(2)
    with left:
        st.plotly_chart(dark_chart(px.bar(pollutant_counts, x="pollutant", y="cities", color="pollutant"), "Dominant pollutant by city"), use_container_width=True)
    with right:
        st.dataframe(pollutants.sort_values("avg_severity_index", ascending=False), use_container_width=True, hide_index=True)
    st.info("Dominance is calculated from normalized concentration severity, not from direct comparison of different pollutant units.")

elif page == "Weather & Pollution":
    left, right = st.columns(2)
    with left:
        fig = px.scatter(weather, x="temperature_c", y="aqi", color="city", size="humidity_pct", hover_data=["wind_speed_kmh", "observed_date"])
        st.plotly_chart(dark_chart(fig, "Temperature versus AQI"), use_container_width=True)
    with right:
        fig = px.scatter(weather, x="humidity_pct", y="aqi", color="city", size="wind_speed_kmh", hover_data=["temperature_c", "observed_date"])
        st.plotly_chart(dark_chart(fig, "Humidity versus AQI"), use_container_width=True)
    correlations = weather.groupby("city", as_index=False).agg(temperature_correlation=("aqi_temperature_correlation", "mean"), humidity_correlation=("aqi_humidity_correlation", "mean"), wind_correlation=("aqi_wind_correlation", "mean"))
    st.dataframe(
        correlations.style.format(
            {
                "temperature_correlation": "{:.2f}",
                "humidity_correlation": "{:.2f}",
                "wind_correlation": "{:.2f}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )
    st.caption("Correlation indicates association in this dataset; it does not prove that weather causes AQI changes.")

elif page == "Historical Trends":
    if monthly.empty:
        st.info("Monthly trend data is not available for the selected filters.")
    else:
        monthly["month_start_date"] = pd.to_datetime(monthly["month_start_date"])
        st.plotly_chart(dark_chart(px.line(monthly, x="month_start_date", y="avg_aqi", color="city", markers=True), "Monthly average AQI"), use_container_width=True)
        pivot = hourly.pivot_table(index="weekday_name", columns="hour_of_day", values="avg_aqi", aggfunc="mean")
        fig = px.imshow(pivot, color_continuous_scale=["#00b050", "#ffd600", "#ff9900", "#b71c1c"], aspect="auto", labels={"x": "Hour of day", "y": "Weekday", "color": "AQI"})
        st.plotly_chart(dark_chart(fig, "Hourly AQI pattern"), use_container_width=True)

else:
    pipeline = data["pipeline"].copy()
    quality = data["quality"].copy()
    pipeline["completed_at"] = pd.to_datetime(pipeline["completed_at"])
    quality["checked_at"] = pd.to_datetime(quality["checked_at"])
    latest_run = pipeline.sort_values("completed_at", ascending=False).head(5)
    passed = int((quality["status"] == "PASS").sum())
    failed = int((quality["status"] == "FAIL").sum())
    skipped = int((quality["status"] == "SKIPPED").sum())
    a, b, c = st.columns(3)
    a.metric("Passed checks", passed)
    b.metric("Failed checks", failed)
    c.metric("Skipped checks", skipped)
    st.subheader("Latest pipeline steps")
    st.dataframe(latest_run[["step_name", "status", "started_at", "completed_at", "duration_seconds"]], use_container_width=True, hide_index=True)
    st.subheader("Latest data-quality checks")
    st.dataframe(quality.sort_values("checked_at", ascending=False).head(20), use_container_width=True, hide_index=True)

st.caption("AQI category colours follow the CPCB-style thresholds used in the Databricks Gold pipeline.")
