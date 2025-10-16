# cleaner.py
import os
import json
import pandas as pd
from config import DATA_DIR

def clean_json_files():
    """Gộp và lọc dữ liệu JSON AniList (định dạng mới) thành 1 DataFrame + JSON tổng hợp"""
    files = [f for f in os.listdir(DATA_DIR) if f.endswith(".json")]
    all_records = []

    for file in sorted(files):
        path = os.path.join(DATA_DIR, file)
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        for item in data:
            all_records.append({
                "id": item.get("id"),
                "title_english": item.get("title_english"),
                "format": item.get("format"),
                "episodes": item.get("episodes"),
                "duration": item.get("duration"),
                "source": item.get("source"),
                "country": item.get("country"),
                "isAdult": item.get("isAdult"),
                "score": item.get("score"),
                "meanScore": item.get("meanScore"),
                "popularity": item.get("popularity"),
                "favourites": item.get("favourites"),
                "trending": item.get("trending"),
                "genres": item.get("genres"),
                "tags": item.get("tags"),
                "status": item.get("status"),
                "studio": item.get("studio"),
                "season": item.get("season"),
                "start_year": item.get("start_year"),
                "url": item.get("url")
            })

    # ===== Lưu file CSV =====
    df = pd.DataFrame(all_records)
    csv_path = os.path.join(DATA_DIR, "anilist_cleaned.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"✅ Đã lưu file CSV: {csv_path} ({len(df)} dòng)")

    # ===== Lưu file JSON =====
    json_path = os.path.join(DATA_DIR, "anilist_cleaned.json")
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(all_records, jf, ensure_ascii=False, indent=4)
    print(f"✅ Đã lưu file JSON: {json_path}")

    # Trả về list (dùng khi import trong Python khác)
    return all_records


if __name__ == "__main__":
    data = clean_json_files()
    print(f"🎉 Hoàn tất! Tổng cộng {len(data)} anime được làm sạch.")
