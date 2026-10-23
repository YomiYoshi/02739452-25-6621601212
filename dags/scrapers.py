"""
scrapers.py — ฟังก์ชัน scrape ราคาหูฟังแยกตามร้าน (requests + BeautifulSoup เท่านั้น)

ทุกฟังก์ชันคืน list[dict] ตาม schema กลาง:
    source, product_id, name, brand, price, url
(scraped_at เติมใน transform ของ DAG)

จริยธรรม: ใส่ User-Agent ที่บอกตัวตน, หน่วงเวลาระหว่าง request, ดึงเฉพาะหน้ารายการสินค้า
ที่ robots.txt อนุญาต และไม่เก็บข้อมูลส่วนบุคคล (รีวิว/ชื่อผู้ขาย/ผู้ซื้อ)
"""

import logging
import re
import time

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; KU-BigData-Exam-Scraper/1.0; academic use)",
    "Accept-Language": "th,en;q=0.8",
}
REQUEST_DELAY_SEC = 2.0
TIMEOUT_SEC = 30

# ---------------- JIB ----------------
# หมวด "หูฟังไร้สาย" — เปลี่ยนหน้าด้วย offset /0 /100 /200 (100 รายการต่อหน้า)
JIB_CATEGORY_URL = "https://www.jib.co.th/web/product/product_list/2/1494"
JIB_OFFSETS = [0, 100, 200]

KNOWN_BRANDS = [
    "1MORE", "ACER", "ADATA", "AKG", "ANKER", "AUDIO-TECHNICA", "AUKEY", "BASEUS", "BEATS",
    "BOSE", "CMF", "EDIFIER", "FANTECH", "HUAWEI", "HYPERX", "JABRA", "JBL", "LENOVO",
    "LOGITECH", "MARSHALL", "MONSTER", "NOTHING", "NUBWO", "ONIKUMA", "OPPO", "QCY", "RAZER",
    "REALME", "REDMI", "SAMSUNG", "SENNHEISER", "SHOKZ", "SKULLCANDY", "SONY", "SOUNDCORE",
    "SOUNDPEATS", "STEELSERIES", "TECHPRO", "TRUE", "VIVO", "XIAOMI", "ZTE", "APPLE", "HONOR",
    "ASUS", "CORSAIR", "SIGNO", "OKER", "HAVIT", "TOZO", "UGREEN", "PHILIPS", "PANASONIC",
    "AIWA", "HIFUTURE", "KZ", "MOONDROP", "TRN", "XO", "REMAX", "ROBOT", "NAKAMICHI",
]
# คำหัวชื่อสินค้าที่ไม่ใช่แบรนด์ (ร้านมักขึ้นต้นด้วยประเภทสินค้า)
_PREFIX_WORDS = {
    "TRUE", "WIRELESS", "HEADPHONE", "HEADPHONES", "HEADSET", "EARPHONE", "EARPHONES",
    "EARBUDS", "BLUETOOTH", "GAMING", "IN-EAR", "ON-EAR", "OVER-EAR", "TWS", "หูฟัง",
}


def polite_get(url):
    """GET พร้อม UA + timeout แล้วหน่วงเวลาหลังทุก request"""
    response = requests.get(url, headers=HEADERS, timeout=TIMEOUT_SEC)
    response.raise_for_status()
    time.sleep(REQUEST_DELAY_SEC)
    return response


def parse_price(text):
    """'1,499.-' / '฿1,499' -> 1499.0 ; ไม่มีตัวเลข -> None"""
    if text is None:
        return None
    digits = re.sub(r"[^0-9.]", "", str(text).replace(",", ""))
    digits = digits.strip(".")
    return float(digits) if digits else None


def guess_brand(name):
    """เดาแบรนด์จากชื่อสินค้า: หาแบรนด์ที่รู้จักก่อน ถ้าไม่เจอใช้คำแรกที่ไม่ใช่คำบอกประเภท"""
    if not name:
        return "UNKNOWN"
    clean = re.sub(r"\([^)]*\)", " ", name.upper())  # ตัดวงเล็บ เช่น (หูฟังทรูไวร์เลส) (WHITE)
    words = re.findall(r"[A-Z0-9][A-Z0-9\-&.]*", clean)
    # "TRUE WIRELESS" เป็นประเภทสินค้า ไม่ใช่แบรนด์ TRUE จึงข้ามคำนำหน้าออกก่อน
    idx = 0
    while idx < len(words) and words[idx] in _PREFIX_WORDS:
        idx += 1
    rest = words[idx:]
    if "SOUNDCORE" in rest[:3]:  # "Anker Soundcore ..." -> ให้ตรงกับร้านที่ใช้แบรนด์ SOUNDCORE
        return "SOUNDCORE"
    for word in rest[:3]:
        if word in KNOWN_BRANDS:
            return word
    return rest[0] if rest else "UNKNOWN"


_COLOR_WORDS = {
    "BLACK", "WHITE", "BLUE", "RED", "GREEN", "PINK", "PURPLE", "GRAY", "GREY", "SILVER", "GOLD",
    "BEIGE", "NAVY", "YELLOW", "ORANGE", "BROWN", "CREAM", "MINT", "LAVENDER", "IVORY", "TITANIUM",
    "MIDNIGHT", "STARLIGHT", "SAND", "STONE", "ROSE", "DARK", "LIGHT", "SKY", "COLOR",
}
_GENERIC_WORDS = _PREFIX_WORDS | {"WITH", "MIC", "NOISE", "CANCELLING", "CANCELING", "ANC", "THE"}


def make_match_key(brand, name):
    """คีย์สำหรับจับคู่สินค้าข้ามร้าน: BRAND|รุ่น (ตัดวงเล็บ ภาษาไทย สี รหัสสินค้า และคำทั่วไป)

    เช่น JIB  'TRUE WIRELESS HEADPHONE (หูฟังทรูไวร์เลส) REDMI BUDS 6 PLAY (WHITE) (57676)'
         Banana 'หูฟังไร้สาย Redmi Buds 6 Play'
    -> 'REDMI|BUDS 6 PLAY' ทั้งคู่
    """
    brand = (brand or "UNKNOWN").upper()
    text = re.sub(r"\([^)]*\)", " ", (name or "").upper())
    text = re.split(r"\s-\s", text)[0]  # ตัดส่วนท้าย ' - WHITE BHR08UHGL'
    tokens = re.findall(r"[A-Z0-9][A-Z0-9+.]*", text)
    model = [
        t for t in tokens
        if t != brand and t not in _GENERIC_WORDS and t not in _COLOR_WORDS
        and not re.fullmatch(r"\d{5,}", t)  # รหัสสินค้า/บาร์โค้ด
    ]
    return f"{brand}|{' '.join(model[:4])}"


def scrape_jib_page(offset):
    """ดึง 1 หน้ารายการหมวดหูฟังไร้สายของ JIB (ใช้กับ Dynamic Task Mapping)"""
    return scrape_jib(offsets=[offset])


def scrape_jib(offsets=None):
    """ดึงหน้ารายการหมวดหูฟังไร้สายของ JIB"""
    records = []
    for offset in offsets or JIB_OFFSETS:
        url = f"{JIB_CATEGORY_URL}/{offset}"
        soup = BeautifulSoup(polite_get(url).text, "html.parser")
        cards = soup.select("div.divboxpro")
        for card in cards:
            link = card.select_one("a[href*='/web/product/readProduct/']")
            name_el = card.select_one("span.promo_name")
            price_el = card.select_one("p.price_total")
            if not (link and name_el):
                continue
            match = re.search(r"/readProduct/(\d+)/", link["href"])
            if not match:
                continue
            name = name_el.get_text(" ", strip=True)
            records.append({
                "source": "jib",
                "product_id": match.group(1),
                "name": name,
                "brand": guess_brand(name),
                "price": parse_price(price_el.get_text(strip=True) if price_el else None),
                "url": link["href"],
            })
        logger.info("jib offset=%s -> %d cards", offset, len(cards))
        if not cards:  # หน้าหมดแล้ว ไม่ต้องยิงหน้าถัดไป
            break
    return records


# ---------------- Banana IT (bnn.in.th) ----------------
# หมวด "หูฟัง" รวม True Wireless + Full-size (ครอบหู) — 24 รายการต่อหน้า เปลี่ยนหน้าด้วย ?page=N
BANANA_CATEGORY_URL = "https://www.bnn.in.th/th/p/home-entertainment/headphone"
BANANA_MAX_PAGES = 12
BANANA_BASE = "https://www.bnn.in.th"


def scrape_banana(max_pages=BANANA_MAX_PAGES):
    """ดึงหน้ารายการหมวดหูฟังของ Banana IT (Nuxt SSR — การ์ดสินค้าอยู่ใน HTML แล้ว)"""
    records = []
    for page in range(1, max_pages + 1):
        url = f"{BANANA_CATEGORY_URL}?page={page}"
        soup = BeautifulSoup(polite_get(url).text, "html.parser")
        cards = soup.select("a.product-item")
        for card in cards:
            href = card.get("href", "").split("?")[0]
            name_el = card.select_one(".product-name")
            price_el = card.select_one(".product-price .price")
            brand_el = card.select_one(".product-label-brand")
            # URL ลงท้ายด้วย _<รหัสสินค้า> เช่น ...-6942675414642_zge9lq
            match = re.search(r"_([a-z0-9]+)$", href)
            if not (href and name_el and match):
                continue
            name = name_el.get_text(" ", strip=True)
            brand = brand_el.get_text(strip=True).upper() if brand_el else guess_brand(name)
            records.append({
                "source": "banana",
                "product_id": match.group(1),
                "name": name,
                "brand": brand or guess_brand(name),
                "price": parse_price(price_el.get_text(strip=True) if price_el else None),
                "url": requests.compat.urljoin(BANANA_BASE, href),
            })
        logger.info("banana page=%s -> %d cards", page, len(cards))
        if not cards:
            break
    return records


# ---------------- Speed Computer (speedcom.co.th) ----------------
# ใช้แทน Advice (Advice โหลดสินค้าผ่าน API ที่ต้องมี token จึงดึงด้วย requests+BS4 ไม่ได้)
# Shopify collection "headphone" — 12 รายการต่อหน้า เปลี่ยนหน้าด้วย ?page=N (robots ห้ามแค่ sort_by)
SPEEDCOM_COLLECTION_URL = "https://www.speedcom.co.th/collections/headphone"
SPEEDCOM_MAX_PAGES = 12
SPEEDCOM_BASE = "https://www.speedcom.co.th"
# collection นี้มีหูฟังแบบมีสายปน -> เก็บเฉพาะชื่อที่บอกว่าไร้สาย
_WIRELESS_HINT = re.compile(r"ไร้สาย|WIRELESS|TWS|BLUETOOTH|LIGHTSPEED", re.IGNORECASE)


def scrape_speedcom(max_pages=SPEEDCOM_MAX_PAGES):
    """ดึงหน้ารายการหูฟังของ Speed Computer (Shopify SSR)"""
    records = []
    for page in range(1, max_pages + 1):
        url = f"{SPEEDCOM_COLLECTION_URL}?page={page}"
        soup = BeautifulSoup(polite_get(url).text, "html.parser")
        cards = soup.select("product-card.card--product")
        for card in cards:
            link = card.select_one("a[href*='/products/']")
            title_el = card.select_one(".card__title")
            price_el = card.select_one(".price__current")
            if not (link and title_el and price_el):
                continue
            name = title_el.get_text(" ", strip=True)
            if not _WIRELESS_HINT.search(name):
                continue
            href = link["href"].split("?")[0]
            handle = href.rstrip("/").split("/products/")[-1]
            amount = re.search(r"[\d,]+(?:\.\d+)?", price_el.get_text(" ", strip=True))
            records.append({
                "source": "speedcom",
                "product_id": handle,
                "name": name,
                "brand": guess_brand(name),
                "price": parse_price(amount.group(0)) if amount else None,
                "url": requests.compat.urljoin(SPEEDCOM_BASE, f"/products/{handle}"),
            })
        logger.info("speedcom page=%s -> %d cards", page, len(cards))
        if not cards:
            break
    return records
