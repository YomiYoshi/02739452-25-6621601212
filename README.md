# วิเคราะห์และเปรียบเทียบราคาหูฟังไร้สาย (TWS / ครอบหู) ช่วง 500–5,000 บาท

รายวิชา 02739452 การแพลตฟอร์มข้อมูลขนาดใหญ่และการวิเคราะห์ — สอบปฏิบัติปลายภาค 1/2569
ลำดับที่ 25 · รหัสนิสิต 6621601212

ระบบรวบรวมราคาหูฟังไร้สายจากร้าน IT ออนไลน์หลายร้านด้วย **Apache Airflow DAG** (LocalExecutor)
แล้วเก็บใน **PostgreSQL** และแสดงผลบน **Streamlit dashboard** ซึ่งอ่านจาก PostgreSQL โดยตรง
dashboard ใช้วิเคราะห์ราคาตามแบรนด์ ดูการกระจายราคาของแต่ละร้าน และเทียบราคารุ่นเดียวกันข้ามร้าน

## แหล่งข้อมูล

| source | ร้าน | หน้าที่ดึง | วิธี |
|---|---|---|---|
| `jib` | JIB | https://www.jib.co.th/web/product/product_list/2/1494 (หมวดหูฟังไร้สาย, หน้า `/0` `/100` `/200`) | requests + BeautifulSoup |
| `banana` | Banana IT | https://www.bnn.in.th/th/p/home-entertainment/headphone (`?page=1..12`, รวม TWS + ครอบหู) | requests + BeautifulSoup |
| `speedcom` | Speed Computer | https://www.speedcom.co.th/collections/headphone (`?page=1..12`, เก็บเฉพาะรุ่นไร้สาย) | requests + BeautifulSoup |

**หมายเหตุ: เปลี่ยนแหล่งที่ 3 จาก Advice เป็น Speed Computer**
หน้า category ของ Advice (https://www.advice.co.th/product/small-talk-bluetooth/true-wireless-in-ear-) ไม่มีรายการสินค้าอยู่ใน HTML ที่ server ส่งมา
เพราะ browser โหลดรายการสินค้าภายหลังผ่าน API `POST /_advice-api/api/v1.0.0/product/get` ซึ่งต้องใช้ Bearer token
(ถ้ายิงโดยไม่มี token จะได้ `401 Unauthorized`) จึงดึงด้วย requests + BeautifulSoup ไม่ได้ และเราไม่หลบการยืนยันตัวตนของเว็บ
ส่วน Speed Computer เป็นร้าน IT ที่ render รายการสินค้ามาใน HTML จาก server แล้ว จึงใช้แทน

**จริยธรรมในการ scrape**
- ตรวจ `robots.txt` ของทุกเว็บแล้ว:
  - JIB `Disallow` เฉพาะ `/login`, `/signin`, `/backoffice` ฯลฯ
  - Banana IT ระบุ `Disallow:` ว่าง
  - Speed Computer ห้ามเฉพาะ `/cart`, `/checkout`, `/account` และ URL ที่มี `sort_by` (เราใช้แค่ `?page=N`)
  - หน้าที่เราดึงเป็นหน้ารายการสินค้า ไม่อยู่ในส่วนที่ห้าม
- ใส่ User-Agent ที่บอกตัวตน (`KU-BigData-Exam-Scraper/1.0; academic use`), timeout และหน่วงเวลา **2 วินาที** ระหว่าง request
- จำกัดจำนวนหน้าที่ดึง และไม่ใช้ Selenium
- **PDPA:** เก็บเฉพาะข้อมูลสินค้า (ชื่อ แบรนด์ ราคา URL) ไม่เก็บรีวิว ชื่อผู้ซื้อ/ผู้ขาย หรือข้อมูลส่วนบุคคลใด ๆ

## Schema กลาง (database `headphones`)

ตาราง `products`

| column | type | หมายเหตุ |
|---|---|---|
| source | TEXT | `jib` / `banana` / `speedcom` |
| product_id | TEXT | รหัสสินค้าของร้าน (ดึงจาก URL) |
| name | TEXT | ชื่อสินค้า |
| brand | TEXT | แบรนด์ (ใช้ label ของร้าน ถ้าไม่มีให้เดาจากชื่อ) |
| price | NUMERIC | ราคา (บาท) เฉพาะช่วง 500–5,000 |
| url | TEXT | ลิงก์สินค้า |
| scraped_at | TIMESTAMPTZ | เวลาที่ดึง |
| match_key | TEXT | `BRAND\|รุ่น` ที่ normalize แล้ว ใช้จับคู่สินค้าข้ามร้าน |

- Primary key คือ `(source, product_id)` โหลดด้วย `INSERT ... ON CONFLICT (source, product_id) DO UPDATE` (**UPSERT**) รันซ้ำแล้วจำนวนแถวจึงไม่เพิ่ม
- วิว `products_dedup` ตัดรายการซ้ำข้ามร้าน: รุ่นเดียวกัน (`match_key` เดียวกัน) เหลือ 1 แถว คือร้านที่ราคาถูกสุด พร้อมคอลัมน์ `offers` (จำนวนร้าน/ตัวเลือก) และ `max_price`
- ตาราง `pipeline_runs` เก็บสถานะของแต่ละรอบการรัน ส่วน `quarantine_batches` เก็บ batch ที่ไม่ผ่านการตรวจคุณภาพ

## DAG `headphone_price_dag`

```
start
 └─ [TaskGroup extract]  (รันขนานกัน)
      ├─ extract_jib      ← Dynamic Task Mapping: 1 task ต่อ 1 หน้า (/0 /100 /200)
      ├─ extract_banana
      └─ extract_speedcom
 └─ transform            trigger_rule=all_done → แหล่งใดล่ม ยังรวมข้อมูลจากแหล่งที่เหลือต่อได้
 └─ quality_check        BranchPythonOperator
      ├─ ผ่าน → [TaskGroup load_path] create_table → load (UPSERT) → refresh_dedup → verify
      └─ ไม่ผ่าน → quarantine (ไม่โหลด เก็บ batch ไว้ตรวจ)
 └─ log_run              trigger_rule=none_failed_min_one_success → เขียน pipeline_runs
 └─ end
```

**transform:** รวมทุกแหล่งให้เป็น schema เดียว ตัดราคานอกช่วง 500–5,000 ตัดหูฟังแบบมีสาย ลบแถวซ้ำด้วย `(source, product_id)` และสร้าง `match_key`

**quality_check:** ไม่ผ่านเมื่อเจอกรณีใดกรณีหนึ่งต่อไปนี้
- จำนวนแถวน้อยกว่า 100
- มีราคาเป็น null หรืออยู่นอกช่วง
- มีชื่อว่างหรือ URL ไม่ถูกต้อง
- มี key ซ้ำ

### การทดสอบ (ผลที่รันจริง)

| run | ผลลัพธ์ |
|---|---|
| ปกติ (3 แหล่ง) | JIB 175 + Banana 143 + Speed Computer 47 = **365 แถว** · วิว dedup เหลือ 288 รุ่น |
| รัน `load` ซ้ำด้วย batch เดิม 2 ครั้ง | upsert 318 แถวทั้ง 2 ครั้ง → ตารางยังมี **318 แถว** (ไม่เพิ่ม) |
| ตั้ง Variable `simulate_down=banana` | `extract_banana` failed แต่ DAG รวม JIB ต่อจนจบ → สถานะ `partial` |
| ตั้ง Variable `min_rows=1000` | `quality_check` แตกไปที่ `quarantine` ไม่มีการโหลด → สถานะ `quarantined` |

ทดสอบเองได้ด้วยคำสั่ง:

```bash
docker compose exec airflow-scheduler airflow variables set simulate_down banana   # จำลองเว็บล่ม
docker compose exec airflow-scheduler airflow variables set min_rows 1000          # บังคับให้ quality ไม่ผ่าน
docker compose exec airflow-scheduler airflow dags trigger headphone_price_dag
docker compose exec airflow-scheduler airflow variables delete simulate_down       # คืนค่า
```

## Dashboard (http://localhost:8501)

- **หน้าราคาหูฟัง**
  - KPI: จำนวนรายการ, ราคาเฉลี่ย, มัธยฐาน, ถูกสุด, จำนวนแบรนด์
  - ตัวกรอง: ร้าน, แบรนด์, ช่วงราคา, ค้นชื่อรุ่น และปุ่มสลับ "ตัดรายการซ้ำข้ามร้าน"
  - กราฟราคาเฉลี่ยตามแบรนด์ และ histogram การกระจายราคาแยกร้าน
  - ตารางเทียบราคารุ่นเดียวกันข้ามร้าน และตารางสินค้า
- **หน้า Pipeline Status** (โบนัส): สถานะรอบล่าสุด ประวัติจาก `pipeline_runs` และสถานะ task ของ run ล่าสุด ซึ่งอ่านจาก metadata DB ของ Airflow

## โครงสร้าง repo

```
docker-compose.yml   postgres + airflow-init + airflow-webserver + airflow-scheduler + web
.env.example         ตัวอย่างค่า env (คัดลอกเป็น .env — ไม่ commit .env)
requirements.txt
db/init.sql          สร้าง database "headphones"
dags/
  headphone_price_dag.py
  scrapers.py        ฟังก์ชัน scrape แต่ละร้าน + parse_price / guess_brand / make_match_key
web/
  Dockerfile
  app.py             Streamlit dashboard
```

## วิธีรัน

ต้องมี Docker Desktop (หรือ Docker Engine + Compose v2)

```bash
git clone https://github.com/YomiYoshi/02739452-25-6621601212.git
cd 02739452-25-6621601212
cp .env.example .env            # Windows: copy .env.example .env
docker compose up -d --build    # ครั้งแรกประมาณ 3–5 นาที
```

| บริการ | URL |
|---|---|
| Airflow UI | http://localhost:8080 (user/pass = `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` ใน `.env`) |
| Dashboard | http://localhost:8501 |
| PostgreSQL | `localhost:5433` db `headphones` (user/pass ตาม `.env`) |

DAG ไม่ถูก pause ตอนสร้าง และ schedule เป็น `@daily` จึงรันเองหนึ่งครั้งหลังระบบขึ้น (ใช้เวลาประมาณ 1 นาที)
ถ้าจะรันเอง ให้กด Trigger ใน UI หรือใช้คำสั่ง:

```bash
docker compose exec airflow-scheduler airflow dags trigger headphone_price_dag
```

ตรวจข้อมูลในฐานข้อมูล:

```bash
docker compose exec postgres psql -U airflow -d headphones -c "SELECT source, COUNT(*) FROM products GROUP BY source;"
```

ปิดระบบ: `docker compose down` (ข้อมูลยังอยู่) หรือ `docker compose down -v` (ลบข้อมูลทั้งหมด)
