# ราคาหูฟังไร้สาย (TWS / ครอบหู) ช่วง 500–5,000 บาท

รายวิชา 02739452 การแพลตฟอร์มข้อมูลขนาดใหญ่และการวิเคราะห์ — สอบปฏิบัติปลายภาค 1/2569
ลำดับที่ 25 · รหัสนิสิต 6621601212

ระบบรวบรวมราคาหูฟังไร้สายจากร้านค้าออนไลน์ ด้วย **Apache Airflow DAG** (LocalExecutor)
→ เก็บใน **PostgreSQL** → แสดงผลบน **Streamlit dashboard** ที่อ่านจาก PostgreSQL โดยตรง

## แหล่งข้อมูล

| source | ร้าน | หน้าที่ดึง | วิธี |
|---|---|---|---|
| `jib` | JIB | https://www.jib.co.th/web/product/product_list/2/1494 (เปลี่ยนหน้า `/0` `/100` `/200`) | requests + BeautifulSoup |

**จริยธรรมในการ scrape**
- ตรวจ `robots.txt` ของทุกเว็บแล้ว หน้าที่ดึงเป็นหน้ารายการสินค้าซึ่งไม่อยู่ในรายการ `Disallow`
- ใส่ User-Agent ที่ระบุตัวตน (`KU-BigData-Exam-Scraper/1.0; academic use`), timeout และหน่วงเวลา 2 วินาทีระหว่าง request
- จำกัดจำนวนหน้าที่ดึง และไม่ใช้ Selenium
- PDPA: เก็บเฉพาะข้อมูลสินค้า (ชื่อ แบรนด์ ราคา URL) ไม่เก็บรีวิว ชื่อผู้ซื้อ/ผู้ขาย หรือข้อมูลส่วนบุคคลใด ๆ

## Schema กลาง (ตาราง `products` ใน database `headphones`)

| column | type | หมายเหตุ |
|---|---|---|
| source | TEXT | `jib` |
| product_id | TEXT | รหัสสินค้าของร้าน (จาก URL) |
| name | TEXT | ชื่อสินค้า |
| brand | TEXT | แบรนด์ (จาก label ของร้าน หรือเดาจากชื่อ) |
| price | NUMERIC | ราคา (บาท) กรองเฉพาะ 500–5,000 |
| url | TEXT | ลิงก์สินค้า |
| scraped_at | TIMESTAMPTZ | เวลาที่ดึง |

Primary key = `(source, product_id)` โหลดด้วย `INSERT ... ON CONFLICT (source, product_id) DO UPDATE`
(UPSERT) จึงรันซ้ำแล้วจำนวนแถวไม่เพิ่ม

## DAG `headphone_price_dag`

```
start → extract_jib → transform → create_table → load (UPSERT) → verify → end
```

## โครงสร้าง repo

```
docker-compose.yml   postgres + airflow-init + airflow-webserver + airflow-scheduler + web
.env.example         ตัวอย่างค่า env (คัดลอกเป็น .env — ไม่ commit .env)
requirements.txt
db/init.sql          สร้าง database "headphones"
dags/
  headphone_price_dag.py
  scrapers.py        ฟังก์ชัน scrape แต่ละร้าน
web/
  Dockerfile
  app.py             Streamlit dashboard
```

## วิธีรัน

ต้องมี Docker Desktop (หรือ Docker Engine + Compose v2)

```bash
git clone <repo-url> && cd 02739452-25-6621601212
cp .env.example .env            # Windows: copy .env.example .env
docker compose up -d --build    # ครั้งแรกประมาณ 2–4 นาที
```

| บริการ | URL |
|---|---|
| Airflow UI | http://localhost:8080 (user/pass = `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` ใน `.env`) |
| Dashboard | http://localhost:8501 |
| PostgreSQL | `localhost:5433` db `headphones` (user/pass ตาม `.env`) |

DAG ถูกตั้งให้ไม่ pause ตอนสร้าง และ schedule เป็น `@daily` จึงรันเองหนึ่งครั้งหลังระบบขึ้น
ถ้าต้องการรันเองให้กด Trigger ใน UI หรือใช้คำสั่ง:

```bash
docker compose exec airflow-scheduler airflow dags trigger headphone_price_dag
```

ตรวจข้อมูลในฐานข้อมูล:

```bash
docker compose exec postgres psql -U airflow -d headphones -c "SELECT source, COUNT(*) FROM products GROUP BY source;"
```

ปิดระบบ: `docker compose down` (ข้อมูลยังอยู่) หรือ `docker compose down -v` (ลบข้อมูลทั้งหมด)
