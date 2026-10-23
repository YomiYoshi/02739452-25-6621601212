"""
Streamlit dashboard — อ่านข้อมูลจาก PostgreSQL โดยตรง
- หน้า "ราคาหูฟัง": KPI + ตัวกรอง + ตาราง + กราฟ (ตาราง products / วิว products_dedup)
- หน้า "Pipeline Status": ประวัติการรัน (pipeline_runs) + สถานะ task ล่าสุดจาก metadata DB ของ Airflow
"""

import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title="เปรียบเทียบราคาหูฟังไร้สาย", page_icon="🎧", layout="wide")

SOURCE_LABELS = {"jib": "JIB", "banana": "Banana IT", "speedcom": "Speed Computer"}


def get_conn(dbname=None):
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5433")),
        dbname=dbname or os.getenv("DB_NAME", "headphones"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


@st.cache_data(ttl=60)
def query(sql, dbname=None):
    with get_conn(dbname) as conn:
        return pd.read_sql(sql, conn)


def page_prices():
    st.title("🎧 วิเคราะห์และเปรียบเทียบราคาหูฟังไร้สาย (TWS / ครอบหู) 500–5,000 บาท")
    st.caption("แหล่งข้อมูล: JIB · Banana IT · Speed Computer — อ่านจาก PostgreSQL โดยตรง (cache 60 วินาที)")

    dedup = st.sidebar.toggle("ตัดรายการซ้ำข้ามร้าน (เลือกร้านที่ถูกสุด)", value=False,
                              help="ใช้วิว products_dedup: รุ่นเดียวกันจากหลายร้านจะเหลือแถวเดียว")
    table = "products_dedup" if dedup else "products"
    try:
        df = query(f"SELECT source, product_id, name, brand, price::float AS price, url, scraped_at, match_key "
                   f"FROM {table} ORDER BY price")
    except Exception as exc:  # ยังไม่มีตาราง (DAG ยังไม่เคยรัน) หรือ DB ยังไม่พร้อม
        st.warning(f"ยังอ่านข้อมูลไม่ได้ — ให้รัน DAG `headphone_price_dag` ก่อน ({exc.__class__.__name__})")
        st.stop()
    if df.empty:
        st.info("ตารางยังว่าง")
        st.stop()
    df["shop"] = df["source"].map(SOURCE_LABELS).fillna(df["source"])

    # ---------- ตัวกรอง ----------
    st.sidebar.header("ตัวกรอง")
    shops = st.sidebar.multiselect("ร้าน", sorted(df["shop"].unique()), default=sorted(df["shop"].unique()))
    brands = st.sidebar.multiselect("แบรนด์", sorted(df["brand"].dropna().unique()))
    lo, hi = st.sidebar.slider("ช่วงราคา (บาท)", 500, 5000, (500, 5000), step=100)
    keyword = st.sidebar.text_input("ค้นหาชื่อรุ่น")

    view = df[df["shop"].isin(shops) & df["price"].between(lo, hi)]
    if brands:
        view = view[view["brand"].isin(brands)]
    if keyword:
        view = view[view["name"].str.contains(keyword, case=False, na=False, regex=False)]
    if view.empty:
        st.info("ไม่มีสินค้าตรงตัวกรอง")
        st.stop()

    # ---------- KPI ----------
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("จำนวนรายการ", f"{len(view):,}")
    c2.metric("ราคาเฉลี่ย", f"฿{view['price'].mean():,.0f}")
    c3.metric("ราคามัธยฐาน", f"฿{view['price'].median():,.0f}")
    c4.metric("ถูกที่สุด", f"฿{view['price'].min():,.0f}")
    c5.metric("จำนวนแบรนด์", f"{view['brand'].nunique():,}")
    last = pd.to_datetime(view["scraped_at"]).max()
    st.caption(f"อัปเดตล่าสุด: {last.tz_convert('Asia/Bangkok'):%Y-%m-%d %H:%M} (เวลาไทย)")

    # ---------- กราฟ ----------
    left, right = st.columns(2)
    with left:
        st.subheader("ราคาเฉลี่ยตามแบรนด์ (15 แบรนด์ที่มีสินค้ามากสุด)")
        top = (view.groupby("brand").agg(avg_price=("price", "mean"), items=("product_id", "count"))
               .sort_values("items", ascending=False).head(15).sort_values("avg_price").reset_index())
        fig = px.bar(top, x="avg_price", y="brand", orientation="h", text="items",
                     labels={"avg_price": "ราคาเฉลี่ย (บาท)", "brand": "แบรนด์"})
        fig.update_traces(texttemplate="%{text} รายการ", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
    with right:
        st.subheader("การกระจายราคาแยกตามร้าน")
        fig2 = px.histogram(view, x="price", color="shop", nbins=20, barmode="overlay",
                            labels={"price": "ราคา (บาท)", "shop": "ร้าน"})
        st.plotly_chart(fig2, use_container_width=True)

    # รุ่นที่มีขายหลายร้าน -> เทียบราคา
    multi = df.groupby("match_key").filter(lambda g: g["source"].nunique() > 1) if not dedup else pd.DataFrame()
    if not multi.empty:
        st.subheader(f"รุ่นที่พบในหลายร้าน ({multi['match_key'].nunique()} รุ่น) — เทียบราคา")
        pivot = multi.pivot_table(index="match_key", columns="shop", values="price", aggfunc="min")
        st.dataframe(pivot, use_container_width=True)

    # ---------- ตาราง ----------
    st.subheader(f"ตารางสินค้า ({len(view):,} รายการ)")
    st.dataframe(
        view[["shop", "brand", "name", "price", "url", "scraped_at"]],
        use_container_width=True, hide_index=True,
        column_config={
            "price": st.column_config.NumberColumn("price (บาท)", format="%.0f"),
            "url": st.column_config.LinkColumn("url"),
            "scraped_at": st.column_config.DatetimeColumn("scraped_at", format="YYYY-MM-DD HH:mm"),
        },
    )


def page_status():
    st.title("⚙️ Pipeline Status")
    st.caption("ประวัติการรัน DAG `headphone_price_dag` (ตาราง pipeline_runs) และสถานะ task ล่าสุดจาก Airflow")
    try:
        runs = query("SELECT run_id, status, started_at, finished_at, raw_counts::text AS raw_counts, "
                     "failed_sources::text AS failed_sources, rows_clean, cross_dupes, rows_in_table, "
                     "problems::text AS problems FROM pipeline_runs ORDER BY finished_at DESC LIMIT 50")
    except Exception as exc:
        st.warning(f"ยังไม่มีประวัติการรัน ({exc.__class__.__name__})")
        return
    if runs.empty:
        st.info("ยังไม่มีประวัติการรัน")
        return

    latest = runs.iloc[0]
    icon = {"success": "🟢", "partial": "🟡", "quarantined": "🔴"}.get(latest["status"], "⚪")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("สถานะล่าสุด", f"{icon} {latest['status']}")
    c2.metric("แถวที่ผ่าน transform", f"{latest['rows_clean']:,}")
    c3.metric("แถวในตาราง products", f"{latest['rows_in_table']:,}")
    c4.metric("รุ่นที่ซ้ำข้ามร้าน", f"{latest['cross_dupes']:,}")
    st.write(f"แหล่งที่ล่มในรอบล่าสุด: `{latest['failed_sources']}` · ปัญหาคุณภาพข้อมูล: `{latest['problems']}`")

    st.subheader("ประวัติการรัน")
    st.dataframe(runs, use_container_width=True, hide_index=True)

    st.subheader("สถานะ task ของ run ล่าสุด (จาก Airflow metadata DB)")
    try:
        tis = query(
            "SELECT task_id, map_index, state, start_date, end_date, try_number FROM task_instance "
            "WHERE dag_id = 'headphone_price_dag' AND run_id = ("
            "  SELECT run_id FROM dag_run WHERE dag_id = 'headphone_price_dag' ORDER BY start_date DESC LIMIT 1) "
            "ORDER BY start_date NULLS LAST, task_id, map_index",
            dbname=os.getenv("AIRFLOW_DB_NAME", "airflow"),
        )
        st.dataframe(tis, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.info(f"อ่าน metadata ของ Airflow ไม่ได้ ({exc.__class__.__name__})")


page = st.sidebar.radio("หน้า", ["ราคาหูฟัง", "Pipeline Status"])
if st.sidebar.button("รีเฟรชข้อมูล"):
    st.cache_data.clear()
(page_prices if page == "ราคาหูฟัง" else page_status)()
