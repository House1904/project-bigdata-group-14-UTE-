import json
import mysql.connector
from config import MYSQL_CONFIG, DATA_DIR
import os

def save_to_mysql():
    json_path = os.path.join(DATA_DIR, "anilist_cleaned.json")
    if not os.path.exists(json_path):
        print("❌ Không tìm thấy file JSON. Hãy chạy cleaner.py trước.")
        return

    # ===== Đọc dữ liệu =====
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"📦 Đang import {len(data)} dòng từ {json_path}")

    # ===== Kết nối MySQL =====
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    # ===== Tạo bảng nếu chưa có =====
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS anime (
        id INT PRIMARY KEY,
        title_romaji VARCHAR(255),
        title_english VARCHAR(255),
        title_native VARCHAR(255),
        format VARCHAR(50),
        episodes INT,
        duration INT,
        source VARCHAR(100),
        country VARCHAR(10),
        isAdult BOOLEAN,
        score FLOAT,
        meanScore FLOAT,
        popularity INT,
        favourites INT,
        trending INT,
        genres TEXT,
        tags TEXT,
        status VARCHAR(50),
        studio VARCHAR(255),
        season VARCHAR(50),
        start_year INT,
        url TEXT
    )
    """)

    # ===== Ghi từng dòng =====
    for item in data:
        cursor.execute("""
        REPLACE INTO anime (
            id, title_romaji, title_english, title_native, format, episodes, duration,
            source, country, isAdult, score, meanScore, popularity, favourites, trending,
            genres, tags, status, studio, season, start_year, url
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            int(item.get("id")) if item.get("id") not in [None, "", "nan"] else None,
            item.get("title_romaji"),
            item.get("title_english"),
            item.get("title_native"),
            item.get("format"),
            int(item.get("episodes")) if item.get("episodes") not in [None, "", "nan"] else None,
            int(item.get("duration")) if item.get("duration") not in [None, "", "nan"] else None,
            item.get("source"),
            item.get("country"),
            bool(item.get("isAdult")) if item.get("isAdult") not in [None, "", "nan"] else None,
            float(item.get("score")) if item.get("score") not in [None, "", "nan"] else None,
            float(item.get("meanScore")) if item.get("meanScore") not in [None, "", "nan"] else None,
            int(item.get("popularity")) if item.get("popularity") not in [None, "", "nan"] else None,
            int(item.get("favourites")) if item.get("favourites") not in [None, "", "nan"] else None,
            int(item.get("trending")) if item.get("trending") not in [None, "", "nan"] else None,
            item.get("genres"),
            item.get("tags"),
            item.get("status"),
            item.get("studio"),
            item.get("season"),
            int(item.get("start_year")) if item.get("start_year") not in [None, "", "nan"] else None,
            item.get("url")
        ))

    # ===== Hoàn tất =====
    conn.commit()
    conn.close()
    print(f"✅ Đã lưu {len(data)} anime vào MySQL thành công!")

if __name__ == "__main__":
    save_to_mysql()
