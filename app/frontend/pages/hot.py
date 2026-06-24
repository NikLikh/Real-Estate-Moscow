import os
import sys

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from app.frontend.api import download_export, get_json

st.set_page_config(page_title="Горячие объявления", layout="wide")
st.title("Горячие объявления")
st.caption("Ранжированы по вероятности продажи в первые 14 дней.")

CIAN_URL = "https://www.cian.ru/sale/flat/{}/"

meta = get_json("/model/meta") or {}
if meta.get("trained_at"):
    st.caption(
        f"Модель обучена: {meta['trained_at'][:10]} | PR-AUC {meta.get('pr_auc', 0):.3f} | "
        f"признаков: {meta.get('n_features', 0)}"
    )

geo = pd.DataFrame(get_json("/dashboard/geo"))
municipalities = (
    sorted(geo["municipality"].dropna().tolist()) if "municipality" in geo else []
)
choice = st.sidebar.selectbox("Округ", ["Все"] + municipalities)
rooms = st.sidebar.selectbox("Комнат", ["Любое", 1, 2, 3, 4])
limit = st.sidebar.selectbox("Количество объявлений", [25, 50, 100], index=1)
price_min = st.sidebar.number_input("Цена от, руб", min_value=0, value=0, step=1000000)
price_max = st.sidebar.number_input("Цена до, руб", min_value=0, value=0, step=1000000)

params = {"limit": limit}
if choice != "Все":
    params["municipality"] = choice
if rooms != "Любое":
    params["rooms"] = rooms
if price_min > 0:
    params["price_min"] = int(price_min)
if price_max > 0:
    params["price_max"] = int(price_max)

rows = get_json("/listings/hot", params)
df = pd.DataFrame(rows)
if not df.empty:
    df["cian_id"] = df["cian_id"].map(CIAN_URL.format)

st.dataframe(
    df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "cian_id": st.column_config.LinkColumn(
            "Объявление", display_text=r"flat/(\d+)/"
        ),
        "municipality": "Округ",
        "rooms": "Комнат",
        "total_area": "Площадь, м2",
        "price": "Цена, руб",
        "price_per_m2": "Цена за м2, руб",
        "nearest_metro": "Метро",
        "hot_score": "Вероятность продажи (14 дн)",
    },
)


@st.cache_data(ttl=3600)
def _export_bytes():
    return download_export()


st.download_button(
    "Скачать текущие объявления (Excel)",
    data=_export_bytes(),
    file_name="current_listings.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
