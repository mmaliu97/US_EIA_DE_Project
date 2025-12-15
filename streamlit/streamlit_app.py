import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import os
# -----------------------------
# DATABASE CONNECTION
# -----------------------------
DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", 5432))
DB_NAME = os.environ.get("DB_NAME", "db")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")

if not all([DB_USER, DB_PASS]):
    raise RuntimeError("Database credentials are not set in environment variables")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
@st.cache_data
def load_data():
    query = """
        SELECT 
            month,
            fueltype,
            fueltype_monthly_value,
            monthly_total,
            share_pct
        FROM dev.monthly_share   
        ORDER BY month;
    """
    return pd.read_sql(query, engine)

monthly_totals = load_data()

st.title("⚡ Energy Dashboard — Streamlit + Postgres + dbt")

# -----------------------------
# CHART 1 — Monthly Totals Heatmap
# -----------------------------


fig1 = px.bar(
    monthly_totals,
    x="month",
    y="fueltype_monthly_value",
    color="fueltype",
    title="Monthly Energy Consumption by Fuel Type",
)
st.plotly_chart(fig1, use_container_width=True)

# -----------------------------
# CHART 2 — Percentage Share
# -----------------------------
fig2 = px.line(
    monthly_totals,
    x="month",
    y="share_pct",
    color="fueltype",
    title="Share % of Each Fuel Type Over Time",
)
st.plotly_chart(fig2, use_container_width=True)

# -----------------------------
# FILTER WIDGETS
# -----------------------------
# st.subheader("🔍 Explore by Fuel Type")
# fuel = st.selectbox("Fuel Type", sorted(df["fueltype"].unique()))

# filtered = monthly_totals[monthly_totals["fueltype"] == fuel]

# fig3 = px.bar(
#     filtered,
#     x="month",
#     y="share_pct",
#     title=f"{fuel} — Monthly Share %",
# )
# st.plotly_chart(fig3, use_container_width=True)

# st.success("Dashboard loaded successfully.")
