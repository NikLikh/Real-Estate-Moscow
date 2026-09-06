import json
import os
import sys

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.figure_factory as ff
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from app.frontend.api import get_json

st.set_page_config(page_title="Дашборд", layout="wide")
st.title("Аналитика цен")

ROOM_ORDER = ["Студия", "1", "2", "3", "4", "5+", "Не указано"]

GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "..", "ru_regions.json")
REGION_TO_GEOJSON = {
    "Еврейская автономная область": "Еврейская АО",
    "Кемеровская область": "Кемеровская область - Кузбасс",
    "Республика Северная Осетия - Алания": "Республика Северная Осетия — Алания",
    "Ханты-Мансийский автономный округ": "Ханты-Мансийский АО — Югра",
    "Ямало-Ненецкий автономный округ": "Ямало-Ненецкий АО",
}


@st.cache_data
def load_regions_geojson():
    with open(GEOJSON_PATH, encoding="utf-8") as fh:
        return json.load(fh)

regions_df = pd.DataFrame(get_json("/dashboard/regions"))
region_names = (
    sorted(regions_df["region"].dropna().tolist()) if "region" in regions_df else []
)
region_choice = st.sidebar.selectbox("Регион", ["Вся Россия"] + region_names)

region = None if region_choice == "Вся Россия" else region_choice
base_params = {}
if region:
    base_params["region"] = region

geo = pd.DataFrame(get_json("/dashboard/geo", base_params))
municipalities = (
    sorted(geo["municipality"].dropna().tolist()) if "municipality" in geo else []
)
choice = st.sidebar.selectbox("Округ / муниципалитет", ["Все"] + municipalities)
new_only = st.sidebar.selectbox("Тип", ["Все", "Новостройка", "Вторичка"])

params = dict(base_params)
if choice != "Все":
    params["municipality"] = choice
if new_only == "Новостройка":
    params["is_new_building"] = "true"
elif new_only == "Вторичка":
    params["is_new_building"] = "false"

if region:
    st.caption(f"Регион: {region}")
else:
    st.subheader("Карта регионов: медианная цена за м2, руб")
    if not regions_df.empty:
        rr = regions_df.dropna(subset=["median_ppm2"]).copy()
        rr["geo_name"] = rr["region"].map(lambda r: REGION_TO_GEOJSON.get(r, r))
        fig = px.choropleth_mapbox(
            rr,
            geojson=load_regions_geojson(),
            locations="geo_name",
            featureidkey="properties.name",
            color="median_ppm2",
            color_continuous_scale="YlOrRd",
            mapbox_style="carto-positron",
            zoom=2,
            center=dict(lat=64, lon=99),
            opacity=0.7,
            hover_name="region",
            hover_data={"geo_name": False, "median_ppm2": ":,.0f", "n_active": True},
            labels={"median_ppm2": "Медиана за м2, руб", "n_active": "Квартир в продаже"},
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=560)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Медианная цена за м2 по регионам, руб")
    if not regions_df.empty:
        rr2 = regions_df.dropna(subset=["median_ppm2"]).sort_values(
            "median_ppm2", ascending=False
        )
        fig = px.bar(rr2, x="region", y="median_ppm2", height=600, hover_data=["n_active"])
        fig.update_layout(xaxis_title="Регион", yaxis_title="Медианная цена за м2, руб")
        st.plotly_chart(fig, use_container_width=True)

st.subheader("A. Динамика медианной цены за м2, руб")
st.caption(
    "Survivorship bias: история цен только по квартирам, активным сейчас. Период: с 01.01.2026."
)
idx = pd.DataFrame(get_json("/dashboard/price-index", params))
if not idx.empty:
    idx["month"] = pd.to_datetime(idx["month"])
    idx = idx.sort_values("month")
    fig = px.bar(idx, x="month", y="median_ppm2", text="median_ppm2")
    fig.update_traces(texttemplate="%{text:,.0f} руб", textposition="outside")
    fig.update_layout(xaxis_title="Месяц", yaxis_title="Медианная цена за м2, руб")
    st.plotly_chart(fig, use_container_width=True)

st.subheader("B. Сегментация цены за м2")
segm = get_json("/dashboard/segmentation", base_params)
by_rooms = pd.DataFrame(segm["by_rooms"])
if not by_rooms.empty:
    by_rooms["order"] = by_rooms["room_group"].map(
        {v: i for i, v in enumerate(ROOM_ORDER)}
    )
    by_rooms = by_rooms.sort_values("order")
    fig = go.Figure(
        go.Box(
            x=by_rooms["room_group"],
            q1=by_rooms["q1"],
            median=by_rooms["median"],
            q3=by_rooms["q3"],
            lowerfence=by_rooms["p05"],
            upperfence=by_rooms["p95"],
        )
    )
    fig.update_layout(
        title="Цена за м2 по комнатности",
        xaxis_title="Комнатность",
        yaxis_title="Цена за м2, руб",
    )
    st.plotly_chart(fig, use_container_width=True)

nvs = pd.DataFrame(segm["new_vs_secondary"])
if not nvs.empty:
    nvs["label"] = nvs["is_new_building"].map({True: "Новостройка", False: "Вторичка"})
    fig = go.Figure(
        go.Box(
            x=nvs["label"],
            q1=nvs["q1"],
            median=nvs["median"],
            q3=nvs["q3"],
            lowerfence=nvs["p05"],
            upperfence=nvs["p95"],
        )
    )
    fig.update_layout(
        title="Цена за м2: новостройка vs вторичка",
        xaxis_title="Тип",
        yaxis_title="Цена за м2, руб",
    )
    st.plotly_chart(fig, use_container_width=True)

st.subheader("C. География и распределение")
pts = pd.DataFrame(get_json("/dashboard/geo-points", base_params))
if not pts.empty:
    center = dict(lat=pts["lat"].median(), lon=pts["lon"].median())
    fig = ff.create_hexbin_mapbox(
        data_frame=pts,
        lat="lat",
        lon="lon",
        color="price_per_m2",
        agg_func=np.median,
        nx_hexagon=40,
        opacity=0.6,
        zoom=9 if region else 3,
        center=center,
        mapbox_style="open-street-map",
        labels={"color": "Медиана цены за м2, руб"},
    )
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

if not geo.empty:
    geo_sorted = geo.dropna(subset=["median_ppm2"]).sort_values(
        "median_ppm2", ascending=False
    )
    if not region:
        geo_sorted = geo_sorted.head(40)
        st.caption("Топ-40 муниципалитетов по стране; выберите регион для полного среза")
    fig = px.bar(geo_sorted, x="municipality", y="median_ppm2", height=700)
    fig.update_layout(
        xaxis_title="Муниципалитет", yaxis_title="Медианная цена за м2, руб"
    )
    st.plotly_chart(fig, use_container_width=True)

dist = pd.DataFrame(get_json("/dashboard/distribution", base_params))
if not dist.empty:
    fig = px.bar(dist, x="ppm2_from", y="n")
    fig.update_layout(
        xaxis_title="Цена за м2, руб", yaxis_title="Количество квартир"
    )
    st.plotly_chart(fig, use_container_width=True)
