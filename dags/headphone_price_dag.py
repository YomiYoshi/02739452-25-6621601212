"""
headphone_price_dag.py — ราคาหูฟังไร้สาย (TWS / ครอบหู) ช่วง 500-5,000 บาท

start
  -> [TaskGroup extract]  extract_jib (Dynamic Task Mapping ตามหน้า) || extract_banana || extract_speedcom
                          (รันขนานกัน)
  -> transform            (trigger_rule=all_done: แหล่งไหนล่มก็รวมจากแหล่งที่เหลือต่อ)
  -> quality_check        (Branch: ผ่าน -> load_path / ไม่ผ่าน -> quarantine)
       -> create_table -> load (UPSERT) -> refresh_dedup -> verify
       -> quarantine
  -> log_run              (บันทึกสถานะลง pipeline_runs ให้หน้า Pipeline Status)
  -> end

Connection: "headphones_db" (ตั้งผ่าน env AIRFLOW_CONN_HEADPHONES_DB ใน docker-compose.yml)
จำลองเว็บล่ม: ตั้ง Airflow Variable `simulate_down` = "banana" (หรือ "jib", คั่นด้วย ,)
ทดสอบ quarantine: ตั้ง Airflow Variable `min_rows` = 1000 (ค่าปกติ 100)
"""

import json
import logging
import re
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from psycopg2.extras import execute_values

from scrapers import JIB_OFFSETS, make_match_key, scrape_banana, scrape_jib_page, scrape_speedcom

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "headphones_db"
TABLE = "products"
PRICE_MIN, PRICE_MAX = 500, 5000
MIN_ROWS = 100
SOURCES = ["jib", "banana", "speedcom"]
COLUMNS = ["source", "product_id", "name", "brand", "price", "url", "scraped_at", "match_key"]
# Banana หมวด "หูฟัง" มีหูฟังแบบมีสายปนอยู่ -> ตัดออกให้เหลือเฉพาะไร้สาย
WIRED_PATTERN = re.compile(r"\bWIRED\b|มีสาย|3\.5\s?MM|USB-C EARPHONE", re.IGNORECASE)

default_args = {
    "owner": "6621601212",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def _check_simulated_down(source):
    """ใช้ทดสอบระดับ 3: ถ้า source อยู่ใน Variable simulate_down ให้ทำเหมือนเว็บล่ม"""
    down = [s.strip() for s in Variable.get("simulate_down", default_var="").split(",") if s.strip()]
    if source in down:
        raise ConnectionError(f"{source}: simulated outage (Variable simulate_down={down})")


# ---------------- extract ----------------
def extract_jib(offset, **context):
    _check_simulated_down("jib")
    records = scrape_jib_page(offset)
    if not records:
        # หน้าแรกว่าง = เว็บผิดปกติ; หน้าถัดไปว่าง = สินค้าในหมวดมีไม่ถึงหน้านั้น (จบรายการ ไม่ใช่ error)
        if offset == 0:
            raise ValueError("JIB: หน้าแรกไม่พบสินค้า — โครงสร้างเว็บอาจเปลี่ยน")
        logger.warning("extract_jib offset=%s: หน้าว่าง (สินค้าในหมวดมีไม่ถึงหน้านี้)", offset)
    logger.info("extract_jib offset=%s: %d rows", offset, len(records))
    return records


def extract_banana(**context):
    _check_simulated_down("banana")
    records = scrape_banana()
    if not records:
        raise ValueError("Banana: ไม่พบสินค้า — โครงสร้างเว็บอาจเปลี่ยน")
    logger.info("extract_banana: %d rows", len(records))
    return records


def extract_speedcom(**context):
    _check_simulated_down("speedcom")
    records = scrape_speedcom()
    if not records:
        raise ValueError("Speedcom: ไม่พบสินค้า — โครงสร้างเว็บอาจเปลี่ยน")
    logger.info("extract_speedcom: %d rows", len(records))
    return records


# ---------------- transform ----------------
def _pull_source(ti, task_id):
    """ดึง XCom ของแหล่งหนึ่ง; task ที่ fail จะไม่มี XCom -> ได้ list ว่าง"""
    value = ti.xcom_pull(task_ids=task_id)
    if value is None:
        return []
    # mapped task คืน list ของผลแต่ละหน้า (หน้าที่ fail เป็น None)
    if task_id == "extract.extract_jib":
        return [row for page in value if page for row in page]
    return list(value)


def transform(**context):
    """รวมทุกแหล่งเป็น schema เดียว, กรองช่วงราคา + หูฟังมีสาย, ลบซ้ำ, สร้าง match_key"""
    ti = context["ti"]
    raw = {
        "jib": _pull_source(ti, "extract.extract_jib"),
        "banana": _pull_source(ti, "extract.extract_banana"),
        "speedcom": _pull_source(ti, "extract.extract_speedcom"),
    }
    raw_counts = {src: len(rows) for src, rows in raw.items()}
    logger.info("transform: raw rows per source = %s", raw_counts)
    if not any(raw.values()):
        raise ValueError("ทุกแหล่งล่ม — ไม่มีข้อมูลให้รวม")

    scraped_at = datetime.now(timezone.utc).isoformat()
    clean, seen = [], set()
    for row in (r for rows in raw.values() for r in rows):
        key = (row["source"], str(row["product_id"]))
        price = row.get("price")
        if key in seen or price is None or not (PRICE_MIN <= price <= PRICE_MAX):
            continue
        if WIRED_PATTERN.search(row["name"]):
            continue
        seen.add(key)
        name = " ".join(row["name"].split())
        brand = (row.get("brand") or "UNKNOWN").strip().upper()
        if "SOUNDCORE" in name.upper():  # Anker Soundcore: ร้านใช้ชื่อแบรนด์ต่างกัน
            brand = "SOUNDCORE"
        clean.append({
            "source": row["source"],
            "product_id": str(row["product_id"]),
            "name": name,
            "brand": brand,
            "price": float(price),
            "url": row.get("url"),
            "scraped_at": scraped_at,
            "match_key": make_match_key(brand, name),
        })

    # สถิติรายการซ้ำข้ามร้าน (ตัวเลือกที่ถูกสุดจะอยู่ในวิว products_dedup)
    sources_per_key = {}
    for row in clean:
        sources_per_key.setdefault(row["match_key"], set()).add(row["source"])
    cross_dupes = sum(1 for s in sources_per_key.values() if len(s) > 1)
    logger.info("transform: %d -> %d rows, unique models=%d, models found in >1 shop=%d",
                sum(raw_counts.values()), len(clean), len(sources_per_key), cross_dupes)
    ti.xcom_push(key="raw_counts", value=raw_counts)
    ti.xcom_push(key="cross_dupes", value=cross_dupes)
    return clean


# ---------------- quality check (branch) ----------------
def quality_check(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform") or []
    # ปรับเกณฑ์ได้ผ่าน Variable min_rows (ใช้ทดสอบเส้นทาง quarantine)
    min_rows = int(Variable.get("min_rows", default_var=MIN_ROWS))
    problems = []
    if len(rows) < min_rows:
        problems.append(f"rows={len(rows)} < {min_rows}")
    bad_price = [r for r in rows if r["price"] is None or not (PRICE_MIN <= r["price"] <= PRICE_MAX)]
    if bad_price:
        problems.append(f"price out of range/null: {len(bad_price)}")
    bad_text = [r for r in rows if not r["name"] or not (r["url"] or "").startswith("http")]
    if bad_text:
        problems.append(f"empty name / invalid url: {len(bad_text)}")
    keys = [(r["source"], r["product_id"]) for r in rows]
    if len(keys) != len(set(keys)):
        problems.append("duplicate (source, product_id)")

    ti.xcom_push(key="problems", value=problems)
    if problems:
        logger.warning("quality_check: FAIL -> quarantine: %s", problems)
        return "quarantine"
    logger.info("quality_check: PASS (%d rows)", len(rows))
    return "load_path.create_table"


# ---------------- load path ----------------
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
        ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS match_key TEXT;
        CREATE TABLE IF NOT EXISTS quarantine_batches (
            id SERIAL PRIMARY KEY,
            run_id TEXT, created_at TIMESTAMPTZ DEFAULT NOW(),
            problems JSONB, row_count INT, rows JSONB
        );
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ,
            status TEXT, raw_counts JSONB, failed_sources JSONB,
            rows_clean INT, cross_dupes INT, rows_in_table INT, problems JSONB
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
            scraped_at = EXCLUDED.scraped_at,
            match_key = EXCLUDED.match_key;
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, values)
    logger.info("load: upserted %d rows", len(values))


def refresh_dedup(**context):
    """คำนวณ match_key ใหม่ให้ทุกแถว แล้วสร้างวิวตัดรายการซ้ำข้ามร้าน (รุ่นเดียวกันเก็บร้านที่ถูกสุด)"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    # แถวเก่าที่รอบนี้ไม่ได้ดึงซ้ำ (เช่น สินค้าหลุดจากหน้ารายการ) ก็ได้ key ตามกฎล่าสุดด้วย
    rows = hook.get_records(f"SELECT source, product_id, brand, name FROM {TABLE};")
    keys = [(make_match_key(brand, name), source, pid) for source, pid, brand, name in rows]
    with hook.get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, f"""
            UPDATE {TABLE} AS p SET match_key = v.match_key
            FROM (VALUES %s) AS v(match_key, source, product_id)
            WHERE p.source = v.source AND p.product_id = v.product_id
              AND p.match_key IS DISTINCT FROM v.match_key;
        """, keys)
    hook.run(f"""
        CREATE OR REPLACE VIEW products_dedup AS
        SELECT DISTINCT ON (match_key)
               p.*,
               COUNT(*) OVER (PARTITION BY match_key) AS offers,
               MAX(price) OVER (PARTITION BY match_key) AS max_price
        FROM {TABLE} p
        ORDER BY match_key, price ASC, source;
    """)


def verify(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    per_source = hook.get_records(f"SELECT source, COUNT(*) FROM {TABLE} GROUP BY source ORDER BY source;")
    total = sum(n for _, n in per_source)
    dedup = hook.get_first("SELECT COUNT(*) FROM products_dedup;")[0]
    logger.info("verify: total=%d per_source=%s dedup=%d", total, per_source, dedup)
    if total == 0:
        raise ValueError(f"ไม่มีข้อมูลในตาราง {TABLE}")
    return {"total": total, "dedup": dedup}


# ---------------- quarantine path ----------------
def quarantine(**context):
    """ข้อมูลไม่ผ่านการตรวจคุณภาพ: ไม่โหลดเข้า products แต่เก็บ batch ไว้ตรวจสอบ"""
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform") or []
    problems = ti.xcom_pull(task_ids="quality_check", key="problems") or []
    create_table()
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        "INSERT INTO quarantine_batches (run_id, problems, row_count, rows) VALUES (%s, %s, %s, %s);",
        parameters=(context["run_id"], json.dumps(problems), len(rows), json.dumps(rows, ensure_ascii=False)),
    )
    logger.warning("quarantine: %d rows held back, problems=%s", len(rows), problems)


# ---------------- run log (หน้า Pipeline Status) ----------------
def log_run(**context):
    ti, dag_run = context["ti"], context["dag_run"]
    failed = sorted({
        t.task_id.split(".")[-1].replace("extract_", "")
        for t in dag_run.get_task_instances()
        if t.task_id.startswith("extract.") and t.state in ("failed", "upstream_failed")
    })
    problems = ti.xcom_pull(task_ids="quality_check", key="problems") or []
    rows = ti.xcom_pull(task_ids="transform") or []
    status = "quarantined" if problems else ("partial" if failed else "success")
    create_table()
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    in_table = hook.get_first(f"SELECT COUNT(*) FROM {TABLE};")[0]
    hook.run(
        """
        INSERT INTO pipeline_runs (run_id, started_at, finished_at, status, raw_counts, failed_sources,
                                   rows_clean, cross_dupes, rows_in_table, problems)
        VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE SET
            finished_at = NOW(), status = EXCLUDED.status, raw_counts = EXCLUDED.raw_counts,
            failed_sources = EXCLUDED.failed_sources, rows_clean = EXCLUDED.rows_clean,
            cross_dupes = EXCLUDED.cross_dupes, rows_in_table = EXCLUDED.rows_in_table,
            problems = EXCLUDED.problems;
        """,
        parameters=(
            context["run_id"], dag_run.start_date, status,
            json.dumps(ti.xcom_pull(task_ids="transform", key="raw_counts") or {}),
            json.dumps(failed), len(rows),
            ti.xcom_pull(task_ids="transform", key="cross_dupes") or 0,
            in_table, json.dumps(problems),
        ),
    )
    logger.info("log_run: status=%s failed_sources=%s rows_in_table=%d", status, failed, in_table)


with DAG(
    dag_id="headphone_price_dag",
    default_args=default_args,
    description="เปรียบเทียบราคาหูฟังไร้สาย 500-5,000 บาท: JIB + Banana IT + Speed Computer -> PostgreSQL",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["exam", "headphones"],
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("extract", tooltip="ดึงข้อมูลทุกแหล่งพร้อมกัน") as extract_group:
        # Dynamic Task Mapping: 1 task instance ต่อ 1 หน้าของ JIB (/0 /100 /200)
        t_jib = PythonOperator.partial(
            task_id="extract_jib", python_callable=extract_jib,
        ).expand(op_kwargs=[{"offset": o} for o in JIB_OFFSETS])
        t_banana = PythonOperator(task_id="extract_banana", python_callable=extract_banana)
        t_speedcom = PythonOperator(task_id="extract_speedcom", python_callable=extract_speedcom)

    t_transform = PythonOperator(
        task_id="transform", python_callable=transform,
        trigger_rule=TriggerRule.ALL_DONE,  # แหล่งใดล่ม ยังรวมจากแหล่งที่เหลือต่อได้
    )
    t_quality = BranchPythonOperator(task_id="quality_check", python_callable=quality_check)

    with TaskGroup("load_path", tooltip="ผ่านการตรวจคุณภาพ -> โหลดเข้า PostgreSQL") as load_path:
        t_create = PythonOperator(task_id="create_table", python_callable=create_table)
        t_load = PythonOperator(task_id="load", python_callable=load)
        t_dedup = PythonOperator(task_id="refresh_dedup", python_callable=refresh_dedup)
        t_verify = PythonOperator(task_id="verify", python_callable=verify)
        t_create >> t_load >> t_dedup >> t_verify

    t_quarantine = PythonOperator(task_id="quarantine", python_callable=quarantine)
    t_log = PythonOperator(
        task_id="log_run", python_callable=log_run,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    end = EmptyOperator(task_id="end")

    start >> extract_group >> t_transform >> t_quality >> [load_path, t_quarantine]
    [load_path, t_quarantine] >> t_log >> end
