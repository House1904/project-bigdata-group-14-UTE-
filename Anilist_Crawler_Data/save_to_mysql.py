# save_to_mysql.py
import pandas as pd
import mysql.connector
from config import MYSQL_CONFIG, DATA_DIR
import os

def save_to_mysql():
    csv_path = os.path.join(DATA_DIR, "anilist_cleaned.csv")
    if not os.path.exists(csv_path):
        print("❌ Không tìm thấy file CSV. Hãy chạy cleaner.py trước.")
        return

    df = pd.read_csv(csv_path)

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS anime (
        id INT PRIMARY KEY,
        title_romaji VARCHAR(255),
        title_english VARCHAR(255),
        title_native VARCHAR(255),
        episodes INT,
        score FLOAT,
        popularity INT,
        genres TEXT,
        status VARCHAR(50),
        studio VARCHAR(255),
        season VARCHAR(50),
        start_year INT,
        url TEXT
    )
    """)

    for _, row in df.iterrows():
        cursor.execute("""
        REPLACE INTO anime (id, title_romaji, title_english, title_native, episodes,
                            score, popularity, genres, status, studio, season, start_year, url)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            int(row["id"]) if not pd.isna(row["id"]) else None,
            row["title_romaji"], row["title_english"], row["title_native"],
            int(row["episodes"]) if not pd.isna(row["episodes"]) else None,
            float(row["score"]) if not pd.isna(row["score"]) else None,
            int(row["popularity"]) if not pd.isna(row["popularity"]) else None,
            row["genres"], row["status"], row["studio"],
            row["season"], int(row["start_year"]) if not pd.isna(row["start_year"]) else None,
            row["url"]
        ))

    conn.commit()
    conn.close()
    print(f"✅ Đã lưu {len(df)} anime vào MySQL!")

if __name__ == "__main__":
    save_to_mysql()
