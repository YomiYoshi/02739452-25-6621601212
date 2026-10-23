"""
headphone_price_dag.py — ราคาหูฟังไร้สาย (TWS / ครอบหู) ช่วง 500-5,000 บาท

ระดับ 1:  extract_jib -> transform -> create_table -> load (UPSERT) -> verify
Connection: "headphones_db" (ตั้งผ่าน env AIRFLOW_CONN_HEADPHONES_DB ใน docker-compose.yml)
"""

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from scrapers import scrape_jib

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "headphones_db"
TABLE = "products"
PRICE_MIN, PRICE_MAX = 500, 5000
COLUMNS = ["source", "product_id", "name", "brand", "price", "url", "scraped_at"]

default_args = {
    "owner": "6621601212",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def extract_jib(**context):
    records = scrape_jib()
    if not records:
        raise ValueError("JIB: ไม่พบสินค้า — โครงสร้างเว็บอาจเปลี่ยน")
    logger.info("extract_jib: %d rows", len(records))
    return records


def transform(**context):
    """กรองช่วงราคา, ลบแถวซ้ำด้วย (source, product_id), เติม scraped_at"""
    records = context["ti"].xcom_pull(task_ids="extract_jib") or []
    scraped_at = datetime.now(timezone.utc).isoformat()
    clean, seen = [], set()
    for row in records:
        key = (row["source"], row["product_id"])
        if key in seen:
            continue
        price = row.get("price")
        if price is None or not (PRICE_MIN <= price <= PRICE_MAX):
            continue
        seen.add(key)
        clean.append({**row, "name": row["name"].strip(), "scraped_at": scraped_at})
    logger.info("transform: %d -> %d rows (ราคา %d-%d บาท)", len(records), len(clean), PRICE_MIN, PRICE_MAX)
    return clean


def create_table(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            source      TEXT NOT NULL,
            product_id  TEXT NOT NULL,
            name        TEXT NOT NULL,
            brand       TEXT,
            price       NUMERIC(10, 2),
            url         TEXT,
            scraped_at  TIMESTAMPTZ,
            PRIMARY KEY (source, product_id)
        );
    """)


def load(**context):
    """UPSERT: รันซ้ำกี่รอบจำนวนแถวก็ไม่เพิ่ม แต่ราคา/ชื่อ/เวลาจะอัปเดตเป็นค่าล่าสุด"""
    rows = context["ti"].xcom_pull(task_ids="transform") or []
    values = [tuple(r[c] for c in COLUMNS) for r in rows]
    sql = f"""
        INSERT INTO {TABLE} ({", ".join(COLUMNS)}) VALUES %s
        ON CONFLICT (source, product_id) DO UPDATE SET
            name = EXCLUDED.name,
            brand = EXCLUDED.brand,
            price = EXCLUDED.price,
            url = EXCLUDED.url,
            scraped_at = EXCLUDED.scraped_at;
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, values)
    logger.info("load: upserted %d rows", len(values))


def verify(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    per_source = hook.get_records(f"SELECT source, COUNT(*) FROM {TABLE} GROUP BY source ORDER BY source;")
    total = sum(n for _, n in per_source)
    logger.info("verify: total=%d per_source=%s", total, per_source)
    if total == 0:
        raise ValueError(f"ไม่มีข้อมูลในตาราง {TABLE}")


with DAG(
    dag_id="headphone_price_dag",
    default_args=default_args,
    description="ราคาหูฟังไร้สาย 500-5,000 บาท: scrape -> transform -> PostgreSQL",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["exam", "headphones"],
) as dag:
    start = EmptyOperator(task_id="start")
    t_extract = PythonOperator(task_id="extract_jib", python_callable=extract_jib)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_create = PythonOperator(task_id="create_table", python_callable=create_table)
    t_load = PythonOperator(task_id="load", python_callable=load)
    t_verify = PythonOperator(task_id="verify", python_callable=verify)
    end = EmptyOperator(task_id="end")

    start >> t_extract >> t_transform >> t_create >> t_load >> t_verify >> end
