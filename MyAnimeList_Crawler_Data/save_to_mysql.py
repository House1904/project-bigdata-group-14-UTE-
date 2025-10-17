# save_to_mysql.py
import json
import mysql.connector
from config import MYSQL_CONFIG
import os

def save_to_mysql():
    # ==== 1️⃣ Đường dẫn file JSON ====
    json_path = os.path.join("data", "mal_all_cleaned.json")
    if not os.path.exists(json_path):
        print("❌ Không tìm thấy file mal_all_cleaned.json. Hãy chạy cleaner.py trước.")
        return

    # ==== 2️⃣ Đọc dữ liệu JSON ====
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"📦 Đang import {len(data)} anime từ {json_path}")

    # ==== 3️⃣ Kết nối MySQL ====
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    # ==== 4️⃣ Tạo bảng nếu chưa có ====
    cur.execute("""
    CREATE TABLE IF NOT EXISTS mal_anime (
        mal_id INT PRIMARY KEY,
        title VARCHAR(255),
        approved BOOLEAN,
        type VARCHAR(50),
        episodes INT,
        year INT,
        season VARCHAR(50),
        status VARCHAR(50),
        score FLOAT,
        scored_by INT,
        `rank` INT,
        popularity INT,
        members INT,
        favorites INT,
        genres TEXT,
        studios TEXT,
        demographics VARCHAR(100),
        url TEXT
    )
    """)

    # ==== 5️⃣ Ghi từng dòng ====
    inserted = 0
    for item in data:
        try:
            cur.execute("""
            REPLACE INTO mal_anime (
                mal_id, title, approved, type, episodes, year, season,
                status, score, scored_by, `rank`, popularity, members, favorites,
                genres, studios, demographics, url
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                int(item.get("mal_id")) if item.get("mal_id") else None,
                item.get("title"),
                bool(item.get("approved")) if item.get("approved") is not None else None,
                item.get("type"),
                int(item.get("episodes")) if item.get("episodes") else None,
                int(item.get("year")) if item.get("year") else None,
                item.get("season"),
                item.get("status"),
                float(item.get("score")) if item.get("score") else None,
                int(item.get("scored_by")) if item.get("scored_by") else None,
                int(item.get("rank")) if item.get("rank") else None,
                int(item.get("popularity")) if item.get("popularity") else None,
                int(item.get("members")) if item.get("members") else None,
                int(item.get("favorites")) if item.get("favorites") else None,
                item.get("genres"),
                item.get("studios"),
                item.get("demographics"),
                item.get("url")
            ))
            inserted += 1
            if inserted % 200 == 0:
                conn.commit()
                print(f"💾 Đã ghi {inserted} dòng...")

        except Exception as e:
            print(f"⚠️ Lỗi khi ghi mal_id={item.get('mal_id')}: {e}")

    # ==== 6️⃣ Hoàn tất ====
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Hoàn tất! Đã lưu {inserted}/{len(data)} anime vào bảng mal_anime.")

if __name__ == "__main__":
    save_to_mysql()
