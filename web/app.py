"""
Streamlit dashboard — อ่านข้อมูลจาก PostgreSQL โดยตรง (ตาราง products)
"""

import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title="ราคาหูฟังไร้สาย", page_icon="🎧", layout="wide")


def get_conn(dbname=None):
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5433")),
        dbname=dbname or os.getenv("DB_NAME", "headphones"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


@st.cache_data(ttl=60)
def load_products():
    with get_conn() as conn:
        return pd.read_sql(
            "SELECT source, product_id, name, brand, price::float AS price, url, scraped_at "
            "FROM products ORDER BY price",
            conn,
        )


st.title("🎧 ราคาหูฟังไร้สาย (TWS / ครอบหู) 500–5,000 บาท")
st.caption("แหล่งข้อมูล: JIB · ข้อมูลอ่านจาก PostgreSQL โดยตรง (รีเฟรชทุก 60 วินาที)")

try:
    df = load_products()
except Exception as exc:  # ยังไม่มีตาราง (DAG ยังไม่เคยรัน) หรือ DB ยังไม่พร้อม
    st.warning(f"ยังอ่านข้อมูลไม่ได้ — ให้รัน DAG `headphone_price_dag` ก่อน ({exc.__class__.__name__})")
    st.stop()

if df.empty:
    st.info("ตาราง products ยังว่าง")
    st.stop()

st.subheader(f"ตารางสินค้า ({len(df):,} รายการ)")
st.dataframe(
    df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "price": st.column_config.NumberColumn("price (บาท)", format="%.0f"),
        "url": st.column_config.LinkColumn("url"),
        "scraped_at": st.column_config.DatetimeColumn("scraped_at", format="YYYY-MM-DD HH:mm"),
    },
)

st.subheader("ราคาเฉลี่ยตามแบรนด์ (15 แบรนด์ที่มีสินค้ามากที่สุด)")
top = (
    df.groupby("brand")
    .agg(avg_price=("price", "mean"), items=("product_id", "count"))
    .sort_values("items", ascending=False)
    .head(15)
    .sort_values("avg_price")
    .reset_index()
)
fig = px.bar(top, x="avg_price", y="brand", orientation="h", text="items",
             labels={"avg_price": "ราคาเฉลี่ย (บาท)", "brand": "แบรนด์", "items": "จำนวน"})
fig.update_traces(texttemplate="%{text} รายการ", textposition="outside")
st.plotly_chart(fig, use_container_width=True)
