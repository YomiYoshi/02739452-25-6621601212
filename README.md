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
      ├─ ผ่าน → [TaskGroup load_path] create_table → load (UPSERT) → refresh_dedup (คำนวณ match_key ใหม่ + สร้างวิว) → verify
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
| ปกติ (3 แหล่ง) | JIB 175 + Banana 143 + Speed Computer 47 = **365 แถว** · วิว dedup เหลือ 281 รุ่น · พบรุ่นที่ขายหลายร้าน 8 รุ่น |
| รัน `load` ซ้ำด้วย batch เดิม 2 ครั้ง | upsert 318 แถวทั้ง 2 ครั้ง → ตารางยังมี **318 แถว** (ไม่เพิ่ม) |
| ตั้ง Variable `simulate_down=banana` | `extract_banana` failed (สีแดง) แต่ DAG รวมแหล่งที่เหลือต่อจนจบ → สถานะ `partial` |
| ตั้ง Variable `min_rows=1000` | `quality_check` แตกไปที่ `quarantine` ไม่มีการโหลด → สถานะ `quarantined` |
| clone repo ใหม่แล้วรันตาม "วิธีรัน" บน volume ว่าง | ระบบขึ้นครบใน 85 วินาที DAG รันเองอัตโนมัติ ทุก task เขียว ได้ 365 แถว dashboard ใช้งานได้ |

ทดสอบเองได้ด้วยคำสั่ง (ทดสอบเสร็จแล้วอย่าลืมลบ Variable คืน):

```bash
docker compose exec airflow-scheduler airflow variables set simulate_down banana   # จำลองเว็บล่ม
docker compose exec airflow-scheduler airflow dags trigger headphone_price_dag
docker compose exec airflow-scheduler airflow variables delete simulate_down       # คืนค่า

docker compose exec airflow-scheduler airflow variables set min_rows 1000          # บังคับให้ quality ไม่ผ่าน
docker compose exec airflow-scheduler airflow dags trigger headphone_price_dag
docker compose exec airflow-scheduler airflow variables delete min_rows            # คืนค่า
```

ตั้ง Variable ผ่านหน้า UI ก็ได้ที่ **Admin → Variables**

## รายละเอียดการประมวลผลข้อมูล

- **การส่งข้อมูลระหว่าง task:** ใช้ XCom ซึ่งเก็บใน metadata DB ของ Airflow เหมาะกับข้อมูลหลักร้อยแถวแบบนี้
- **ราคา:** ใช้ราคาที่แสดงบนหน้ารายการสินค้า ถ้ามีส่วนลดจะเป็นราคาหลังลด แล้วแปลงเป็นตัวเลข เช่น `"1,499.-"` / `"฿1,499"` / `"790.00 ฿"` → `1499.0`
- **ช่วงราคา:** เก็บเฉพาะ 500–5,000 บาท แถวนอกช่วงหรือแถวที่ไม่มีราคาจะถูกตัดใน transform
- **เฉพาะหูฟังไร้สาย:**
  - JIB ดึงจากหมวดหูฟังไร้สายโดยตรง
  - Banana IT ตัดชื่อที่มีคำว่า `มีสาย` / `WIRED` / `3.5MM`
  - Speed Computer เก็บเฉพาะชื่อที่มีคำว่า `ไร้สาย` / `WIRELESS` / `TWS` / `BLUETOOTH` / `LIGHTSPEED`
- **แบรนด์:**
  - Banana IT ใช้ label แบรนด์ของร้าน
  - JIB และ Speed Computer เดาจากชื่อสินค้า โดยข้ามคำบอกประเภท เช่น `TRUE WIRELESS HEADPHONE` แล้วเทียบกับรายชื่อแบรนด์ที่รู้จัก
  - ชื่อที่มี `Soundcore` จะถูกจัดเป็นแบรนด์ `SOUNDCORE` ทุกร้าน (บางร้านเขียนว่า Anker)
- **match_key (จับคู่ข้ามร้าน):** สร้างจากแบรนด์ + ชื่อรุ่น ตามขั้นตอนนี้
  1. ตัดวงเล็บ ภาษาไทย สี รหัสสินค้า/บาร์โค้ด และคำทั่วไป (เช่น `TRUE WIRELESS`, `ANC`, `OPEN-EAR`, `ANKER`)
  2. ตัดคำบอกสเปกที่บางร้านต่อท้ายชื่อ เช่น `4Mic`, `PT10/45Hr`, `iPX54`, `BT5.3`, `13mm`
  3. **เรียงคำตามตัวอักษร** เพื่อให้ลำดับคำในชื่อไม่มีผล
  - ตัวอย่างที่ได้คีย์เดียวกัน:

    | JIB / Banana IT | Speed Computer | match_key |
    |---|---|---|
    | `... SOUNDCORE R50I NC - BLACK A3959H11` | `หูฟังอินเอียร์ไร้สาย Anker Soundcore R50i NC ANC 4Mic PT10/45Hr iPX54` | `SOUNDCORE\|NC R50I` |
    | `หูฟังไร้สาย Soundcore Sleep A20` (Banana) | `Anker Soundcore A20 Sleep PT14` | `SOUNDCORE\|A20 SLEEP` |

  - สีต่าง ๆ ของรุ่นเดียวกันจึงถูกนับเป็นรุ่นเดียวในวิว `products_dedup` ด้วย
  - task `refresh_dedup` คำนวณ `match_key` ใหม่ให้**ทุกแถว**ในตารางทุกรอบ แถวเก่าที่รอบนี้ไม่ได้ดึงซ้ำจึงใช้กฎล่าสุดเหมือนกัน
  - ผลล่าสุด: พบรุ่นที่ขายมากกว่า 1 ร้าน **8 รุ่น** มี 5 รุ่นที่ Speed Computer ขายด้วย (Soundcore K20i, R50i NC, R60i NC, AeroClip, Sleep A20)
- **ข้อจำกัด:**
  - การจับคู่ข้ามร้านใช้กฎจากชื่อ (rule-based) ถ้าแต่ละร้านตั้งชื่อรุ่นต่างกันมากจะจับคู่ไม่ได้ เช่น JIB ใช้รหัส Philips `TAT2139` แต่ Speed Computer ใช้ชื่อ `2000 Series`
  - การเดาแบรนด์อาจผิดถ้าสะกดผิดตั้งแต่ต้นทาง เช่น `STEELESRIES`
- **สินค้าที่หายจากหน้าเว็บ:** แถวเดิมยังอยู่ในตาราง เพราะ UPSERT ไม่ลบแถว ดูได้จาก `scraped_at` ว่าพบครั้งล่าสุดเมื่อไร
- **หน้า JIB ที่ว่าง:**
  - จำนวนสินค้าในหมวดเปลี่ยนตลอด เช่น เคยลดเหลือ 182 รายการจนหน้า `/200` ว่าง
  - หน้าแรก (`/0`) ว่าง = เว็บผิดปกติ → task fail
  - หน้าถัดไปว่าง = รายการหมดแล้ว → log เตือนแล้วจบ task ปกติ ไม่นับเป็นแหล่งล่ม

## Dashboard (http://localhost:8501)

เมนูและตัวกรองทั้งหมดอยู่บน**หน้าหลัก** (ไม่ใช้ sidebar) เรียงจากบนลงล่างดังนี้

```
┌───────────────────────────────────────────────────────────────────────────┐
│ 🎧 วิเคราะห์และเปรียบเทียบราคาหูฟังไร้สาย ...                                 │
├───────────────────────────────────────────────────────────────────────────┤
│ [📊 ราคาหูฟัง] [⚙️ Pipeline Status]                ← แท็บสลับหน้า           │
│ ◯ ตัดรายการซ้ำข้ามร้าน                              อัปเดตล่าสุด: ...        │
│ ┌ 🔎 ตัวกรอง ─────────────────────────────────────────────────────────────┐ │
│ │ ร้าน [JIB][Banana IT][Speed Computer] │ แบรนด์ [...] │ ช่วงราคา 500–5000 │ │
│ │ ค้นหาชื่อรุ่น [__________________]                                      │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│ KPI ×5 → กราฟ 2 กราฟ → ตารางเทียบราคาข้ามร้าน → ตารางสินค้า                  │
└───────────────────────────────────────────────────────────────────────────┘
```

- **ส่วนหัว:** ชื่อหัวข้อและแหล่งข้อมูล ข้อมูลถูก cache ไว้ 60 วินาที แล้วอ่านจาก PostgreSQL ใหม่เองเมื่อโหลดหน้าเว็บ
- **แท็บ 📊 ราคาหูฟัง**
  - สวิตช์ "ตัดรายการซ้ำข้ามร้าน": เปิดแล้วจะอ่านจากวิว `products_dedup` แทนตาราง `products` และแสดงเวลาอัปเดตล่าสุด (เวลาไทย)
  - แผง **🔎 ตัวกรอง**:
    - ร้าน (เลือกได้หลายร้าน)
    - แบรนด์ (เลือกได้หลายแบรนด์ ถ้าไม่เลือกจะแสดงทุกแบรนด์)
    - ช่วงราคา (slider ทีละ 100 บาท)
    - ค้นหาชื่อรุ่น (ไม่สนตัวพิมพ์เล็ก/ใหญ่)
  - KPI 5 ตัว (คำนวณตามตัวกรอง): จำนวนรายการ, ราคาเฉลี่ย, มัธยฐาน, ถูกสุด, จำนวนแบรนด์
  - กราฟราคาเฉลี่ยตามแบรนด์ (15 แบรนด์ที่มีสินค้ามากสุด) และ histogram การกระจายราคาแยกร้าน
  - ตาราง **"รุ่นที่พบในหลายร้าน (N รุ่น) — เทียบราคา JIB · Banana IT · Speed Computer"** (แสดงเมื่อปิดสวิตช์ตัดรายการซ้ำ)
    - มีคอลัมน์ราคาของ**ทั้ง 3 ร้านเสมอ** ร้านที่ไม่มีรุ่นนั้นแสดง `–`
    - บอก**ร้านที่ถูกสุด**และ**ส่วนต่างราคา** (แพงสุด − ถูกสุด)
  - ตารางสินค้าที่คลิกลิงก์ไปหน้าร้านได้
- **แท็บ ⚙️ Pipeline Status** (โบนัส):
  - KPI สถานะรอบล่าสุด (🟢 success / 🟡 partial / 🔴 quarantined), จำนวนแถว และแหล่งที่ล่ม
  - ประวัติการรันจาก `pipeline_runs`
  - สถานะ task ของ run ล่าสุด อ่านจาก metadata DB ของ Airflow (`task_instance`) โดยตรง

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

### สิ่งที่ต้องมี
- Docker Desktop (หรือ Docker Engine + Compose v2) โดยให้ Docker ใช้ RAM ได้อย่างน้อยประมาณ 4 GB
- อินเทอร์เน็ต สำหรับ pull image และ scrape เว็บ
- port ว่าง 3 ตัว: **8080** (Airflow), **8501** (Dashboard), **5433** (PostgreSQL) ถ้าชนกับโปรแกรมอื่นให้แก้ฝั่งซ้ายของ `ports:` ใน `docker-compose.yml`

### ขั้นตอน

```bash
git clone https://github.com/YomiYoshi/02739452-25-6621601212.git
cd 02739452-25-6621601212
cp .env.example .env            # Windows: copy .env.example .env
docker compose up -d --build
```

- **ครั้งแรก**ใช้เวลาประมาณ 3–5 นาที (pull image `apache/airflow:2.10.4` และ `postgres:16`, build image ของ dashboard และ migrate DB ของ Airflow) ครั้งต่อไปประมาณ 30 วินาที
- เช็กสถานะได้ด้วย `docker compose ps` เมื่อพร้อมแล้ว `airflow-webserver` และ `airflow-scheduler` ต้องเป็น `healthy` ส่วน `airflow-init` ต้องเป็น `exited (0)` (เป็นงานที่รันครั้งเดียวแล้วจบ)

### ไฟล์ `.env`
- **ต้องมีตอนรัน** เพราะ `docker-compose.yml` อ่านรหัสผ่านจากไฟล์นี้ ถ้าไม่มีไฟล์ compose จะหยุดพร้อมข้อความ `copy .env.example to .env first`
- **ไม่ถูก commit** (อยู่ใน `.gitignore`) ใน repo จึงมีแค่ `.env.example`

| ตัวแปร | ค่าใน `.env.example` | ใช้ทำอะไร |
|---|---|---|
| `POSTGRES_USER` | `airflow` | user ของ PostgreSQL (ใช้ทั้ง DB `airflow` และ `headphones`) |
| `POSTGRES_PASSWORD` | `changeme` | รหัสผ่าน PostgreSQL |
| `AIRFLOW_ADMIN_USER` | `admin` | user สำหรับ login Airflow UI |
| `AIRFLOW_ADMIN_PASSWORD` | `changeme` | รหัสผ่าน Airflow UI |

- รหัสผ่านไม่ควรมีอักขระ `@ : / #` เพราะจะถูกนำไปต่อเป็น connection URL
- user และรหัสผ่านถูกสร้างครั้งแรกตอนสร้าง volume ถ้าเปลี่ยนค่าใน `.env` ทีหลัง ต้องรัน `docker compose down -v` แล้ว `up` ใหม่ (ข้อมูลเดิมจะหายทั้งหมด)

### บริการ

| บริการ | URL | เข้าสู่ระบบ |
|---|---|---|
| Airflow UI | http://localhost:8080 | `admin` / `changeme` (ตาม `.env`) |
| Dashboard | http://localhost:8501 | ไม่ต้อง login |
| PostgreSQL | `localhost:5433` | db `headphones` (ข้อมูล) / `airflow` (metadata) · user `airflow` / `changeme` |

### การรัน DAG
- DAG `headphone_price_dag` ไม่ถูก pause ตอนสร้าง และตั้ง schedule เป็น `@daily` (`catchup=False`, เวลาของระบบเป็น `Asia/Bangkok`)
- จึง**รันเองอัตโนมัติ 1 ครั้งหลังระบบขึ้น** แต่ละรอบใช้เวลาประมาณ 50–60 วินาที ส่วนใหญ่เป็นการหน่วงเวลาระหว่าง request
- ถ้า task ล้มเหลวจะ retry 1 ครั้งหลังรอ 30 วินาที
- รันเองได้โดยกดปุ่ม ▶ Trigger ใน UI หรือใช้คำสั่ง:

```bash
docker compose exec airflow-scheduler airflow dags trigger headphone_price_dag
```

- ภาพที่ใช้ตรวจงาน: หน้า **Graph** (โครง DAG, TaskGroup และกิ่งของ branch) และหน้า **Grid** (ประวัติการรัน, task ที่ fail/skipped) ส่วน log ของแต่ละ task ดูได้โดยคลิกที่ task แล้วเลือก **Logs**

### ตรวจข้อมูลในฐานข้อมูล

```bash
docker compose exec postgres psql -U airflow -d headphones -c "SELECT source, COUNT(*) FROM products GROUP BY source;"
docker compose exec postgres psql -U airflow -d headphones -c "SELECT run_id, status, raw_counts, rows_in_table FROM pipeline_runs ORDER BY finished_at;"
```

ถ้าจะต่อจากโปรแกรมอื่น เช่น DBeaver หรือ pgAdmin ให้ใช้ host `localhost`, port `5433`, database `headphones`

| object | ประเภท | เนื้อหา |
|---|---|---|
| `products` | table | ข้อมูลหลัก (schema กลาง + `match_key`) |
| `products_dedup` | view | ตัดรายการซ้ำข้ามร้าน เหลือร้านที่ถูกสุดต่อรุ่น |
| `pipeline_runs` | table | สถานะแต่ละรอบการรัน (`success` / `partial` / `quarantined`) |
| `quarantine_batches` | table | batch ที่ไม่ผ่านการตรวจคุณภาพ (เก็บเป็น JSON) |

### ปิดระบบ
- `docker compose down` ปิด container แต่ข้อมูลและประวัติการรันยังอยู่
- `docker compose down -v` ลบข้อมูลทั้งหมด รวมถึง volume ของ PostgreSQL

## การตั้งค่าเพิ่มเติม

**Airflow Variables** (ไม่ต้องตั้งก็ได้ ใช้สำหรับทดสอบ):

| Variable | ค่าเริ่มต้น | ผล |
|---|---|---|
| `simulate_down` | (ว่าง) | ชื่อแหล่งคั่นด้วย `,` เช่น `banana` หรือ `jib,speedcom` ทำให้ task extract ของแหล่งนั้นทำเหมือนเว็บล่ม |
| `min_rows` | `100` | จำนวนแถวขั้นต่ำที่ `quality_check` ยอมรับ |

**ค่าคงที่ในโค้ด:**

| ค่า | ไฟล์ | ค่าเริ่มต้น |
|---|---|---|
| `PRICE_MIN`, `PRICE_MAX` | `dags/headphone_price_dag.py` | 500, 5000 |
| `REQUEST_DELAY_SEC`, `TIMEOUT_SEC` | `dags/scrapers.py` | 2 วินาที, 30 วินาที |
| `JIB_OFFSETS` | `dags/scrapers.py` | `[0, 100, 200]` (1 ค่า = 1 mapped task) |
| `BANANA_MAX_PAGES`, `SPEEDCOM_MAX_PAGES` | `dags/scrapers.py` | 12 (หยุดเองเมื่อเจอหน้าที่ว่าง) |

## การแก้ปัญหาเบื้องต้น

| อาการ | สาเหตุ / วิธีแก้ |
|---|---|
| `copy .env.example to .env first` | ยังไม่ได้สร้าง `.env` → รัน `cp .env.example .env` |
| `port is already allocated` | port 8080/8501/5433 ถูกโปรแกรมอื่นใช้อยู่ → ปิดโปรแกรมนั้น หรือแก้ `ports:` ใน `docker-compose.yml` |
| Dashboard ขึ้นว่า "ยังอ่านข้อมูลไม่ได้" | DAG ยังรันไม่จบรอบแรก (ตาราง `products` ยังไม่ถูกสร้าง) → รอประมาณ 1 นาที แล้ว reload หน้าเว็บ (F5) |
| ข้อมูลบน Dashboard ยังไม่อัปเดต | หน้าเว็บ cache ไว้ 60 วินาที → รอให้ครบ 60 วินาทีแล้ว reload หน้าเว็บ (F5) |
| ไม่เห็น DAG ใน Airflow UI | scheduler สแกนโฟลเดอร์ dags ทุก 10 วินาที → รอสักครู่ แล้วเช็ก import error ด้วยคำสั่ง `docker compose exec airflow-scheduler airflow dags list-import-errors` |
| login Airflow ไม่ได้หลังเปลี่ยนรหัสใน `.env` | user ถูกสร้างไว้ตั้งแต่รอบแรก → `docker compose down -v` แล้ว `up -d` ใหม่ |
| task extract ของบางร้าน fail | เว็บร้านนั้นล่มหรือเปลี่ยนโครงสร้าง HTML → DAG ยังรวมแหล่งที่เหลือต่อ (สถานะ `partial`) ดูรายละเอียดใน Logs ของ task |

## เทคโนโลยีที่ใช้

| ส่วน | เวอร์ชัน |
|---|---|
| Apache Airflow | 2.10.4 (image `apache/airflow:2.10.4`, LocalExecutor, Python 3.12) |
| PostgreSQL | 16 |
| Scraping | requests 2.32, beautifulsoup4 4.12 |
| Dashboard | Streamlit 1.40, pandas 2.2, plotly 5.24, psycopg2 2.9 (image `python:3.12-slim`) |

แพ็กเกจที่ DAG ใช้มีอยู่ใน image ของ Airflow แล้ว จึงไม่ต้อง build image ของ Airflow เอง
ส่วน `requirements.txt` ใช้ build image ของ dashboard
