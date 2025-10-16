import os
import json
import pandas as pd
from config import DATA_DIR

INPUT_FILE = os.path.join(DATA_DIR, "anilist_raw.json")
CSV_OUT = os.path.join(DATA_DIR, "anilist_cleaned.csv")
JSON_OUT = os.path.join(DATA_DIR, "anilist_cleaned.json")


def validate_record(item):
    """Kiểm tra tính hợp lệ của 1 anime record (AniList)"""
    required_fields = ["id", "title_english", "url"]

    # ===== Kiểm tra các trường bắt buộc =====
    for field in required_fields:
        if field not in item or item[field] in [None, ""]:
            return False

    # ===== Kiểm tra ID =====
    if not isinstance(item["id"], int) or item["id"] <= 0:
        return False

    # ===== Kiểm tra tiêu đề =====
    if not isinstance(item["title_english"], str) or not item["title_english"].strip():
        return False

    # ===== Kiểm tra URL =====
    if not str(item["url"]).startswith("https://anilist.co/anime/"):
        return False

    # ===== Kiểm tra điểm số =====
    for field in ["score", "meanScore"]:
        val = item.get(field)
        if val is not None:
            try:
                v = float(val)
                if v < 0 or v > 100:
                    return False
            except Exception:
                return False

    # ===== Kiểm tra các giá trị số nguyên không âm =====
    for field in ["popularity", "favourites", "trending"]:
        val = item.get(field)
        if val is not None and (not isinstance(val, int) or val < 0):
            return False

    # ===== Gán None cho các trường có thể thiếu =====
    optional_fields = [
        "title_romaji", "title_native", "format", "episodes", "duration", "source",
        "country", "isAdult", "genres", "tags", "status", "studio",
        "season", "start_year"
    ]
    for f in optional_fields:
        if f not in item:
            item[f] = None

    return True


def clean_json_files():
    """Làm sạch dữ liệu AniList và xuất file CSV + JSON"""
    if not os.path.exists(INPUT_FILE):
        print(f"⚠️ Không tìm thấy file: {INPUT_FILE}")
        return

    print(f"🔍 Đang làm sạch dữ liệu trong: {INPUT_FILE}")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    cleaned = []
    seen_ids = set()
    invalid = 0
    missing_stats = {
        "season": 0,
        "start_year": 0,
        "studio": 0,
        "title_english": 0,
    }

    for item in data:
        aid = item.get("id")

        # Loại bỏ ID trùng
        if not aid or aid in seen_ids:
            continue

        # Đếm số trường thiếu để thống kê
        for key in missing_stats.keys():
            if item.get(key) in [None, "", []]:
                missing_stats[key] += 1

        if validate_record(item):
            cleaned.append(item)
            seen_ids.add(aid)
        else:
            invalid += 1

    # ===== Xuất CSV =====
    df = pd.DataFrame(cleaned)
    df.to_csv(CSV_OUT, index=False, encoding="utf-8-sig", na_rep="null")

    # ===== Xuất JSON =====
    with open(JSON_OUT, "w", encoding="utf-8") as jf:
        json.dump(cleaned, jf, ensure_ascii=False, indent=4)

    # ===== Thống kê =====
    print(f"\n✅ Đã làm sạch dữ liệu:")
    print(f"  - Giữ lại {len(cleaned)} / {len(data)} anime hợp lệ.")
    print(f"  - Bỏ qua {invalid} anime lỗi hoặc trùng ID.")
    print(f"  - Xuất CSV:  {CSV_OUT}")
    print(f"  - Xuất JSON: {JSON_OUT}")

    print("\n📊 Thống kê trường bị thiếu:")
    for key, count in missing_stats.items():
        pct = (count / len(data)) * 100 if len(data) > 0 else 0
        print(f"  • {key:<12}: {count:>5} ({pct:.2f}%) thiếu dữ liệu")

    return cleaned


if __name__ == "__main__":
    data = clean_json_files()
    print(f"\n🎉 Hoàn tất! Tổng cộng {len(data)} anime hợp lệ sau khi kiểm tra.")
