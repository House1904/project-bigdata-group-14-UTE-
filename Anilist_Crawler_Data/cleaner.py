import os
import json
import pandas as pd
from config import DATA_DIR

# ===== Đường dẫn file =====
INPUT_FILE = os.path.join(DATA_DIR, "anilist_raw.json")
CSV_OUT = os.path.join(DATA_DIR, "anilist_cleaned.csv")
JSON_OUT = os.path.join(DATA_DIR, "anilist_cleaned.json")


# ==============================================
# ⚙️ HÀM XỬ LÝ TỪNG RECORD (MỘT BẢN GHI ANIME)
# ==============================================
def validate_and_clean_record(item):
    """
    Làm sạch và xác thực một record AniList.
    Bao gồm:
      - Kiểm tra trường bắt buộc
      - Chuẩn hóa dữ liệu text & list
      - Chuẩn hóa score về thang 10
      - Ép kiểu và loại bỏ giá trị bất thường
    Trả về record hợp lệ hoặc None nếu không hợp lệ.
    """

    # ===== Trường bắt buộc =====
    required = ["id", "title_english", "title_native", "title_romaji", "url"]
    for f in required:
        if f not in item or item[f] in [None, ""]:
            return None

    # ===== Kiểm tra ID =====
    if not isinstance(item["id"], int) or item["id"] <= 0:
        return None

    # ===== Kiểm tra URL =====
    if not str(item["url"]).startswith("https://anilist.co/anime/"):
        return None

    # ===== Chuẩn hóa score về thang 10 =====
    for field in ["score", "meanScore"]:
        val = item.get(field)
        if val is None:
            item[field] = None
            continue
        try:
            v = float(val)
            if v < 0 or v > 100:
                return None
            item[field] = round(v / 10, 2)  # ⚙️ chuyển về /10
        except Exception:
            return None

    # ===== Kiểm tra các trường số nguyên không âm =====
    for f in ["episodes", "duration", "popularity", "favourites", "trending", "start_year"]:
        if f in item and item[f] not in [None, ""]:
            try:
                val = int(item[f])
                if val < 0:
                    item[f] = None
                else:
                    item[f] = val
            except Exception:
                item[f] = None
        else:
            item[f] = None

    # ===== Chuẩn hóa các trường văn bản danh mục =====
    def normalize_str(s):
        if not s:
            return None
        s = str(s).strip().upper()
        return s if s else None

    item["format"] = normalize_str(item.get("format"))
    item["source"] = normalize_str(item.get("source"))
    item["country"] = normalize_str(item.get("country"))
    item["season"] = normalize_str(item.get("season"))
    item["status"] = normalize_str(item.get("status"))

    # ===== Chuẩn hóa tên studio =====
    item["studio"] = str(item.get("studio") or "").strip() or None

    # ===== Chuẩn hóa list (genres, tags) =====
    def clean_list_field(val):
        """Làm sạch danh sách: loại trùng, viết hoa đầu từ, xóa trống."""
        if not val:
            return None
        if isinstance(val, list):
            vals = val
        else:
            vals = str(val).split(",")
        vals = [v.strip().title() for v in vals if v.strip()]
        return ", ".join(sorted(set(vals))) if vals else None

    item["genres"] = clean_list_field(item.get("genres"))
    item["tags"] = clean_list_field(item.get("tags"))

    # ===== Kiểu boolean =====
    if "isAdult" in item:
        val = item["isAdult"]
        if isinstance(val, str):
            item["isAdult"] = val.lower() == "true"
        elif isinstance(val, bool):
            pass
        else:
            item["isAdult"] = False

    return item


# ==============================================
# 🧹 HÀM LÀM SẠCH TOÀN BỘ FILE JSON
# ==============================================
def clean_json_files():
    """Làm sạch toàn bộ dữ liệu AniList raw và xuất ra file CSV + JSON."""
    if not os.path.exists(INPUT_FILE):
        print(f"⚠️ Không tìm thấy file: {INPUT_FILE}")
        return

    print(f"🔍 Đang làm sạch dữ liệu trong: {INPUT_FILE}")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    cleaned = []
    seen_ids = set()
    invalid = 0

    # Thống kê các trường bị thiếu
    missing_stats = {"season": 0, "start_year": 0, "studio": 0, "title_english": 0}

    for item in raw_data:
        aid = item.get("id")

        # Bỏ qua trùng ID
        if aid in seen_ids:
            continue

        # Đếm trường bị thiếu
        for k in missing_stats.keys():
            if item.get(k) in [None, "", []]:
                missing_stats[k] += 1

        # Làm sạch từng record
        valid = validate_and_clean_record(item)
        if valid:
            cleaned.append(valid)
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
    print(f"  - Tổng số record: {len(raw_data)}")
    print(f"  - Hợp lệ: {len(cleaned)}")
    print(f"  - Bỏ qua: {invalid} record lỗi hoặc trùng ID")
    print(f"  - Xuất CSV:  {CSV_OUT}")
    print(f"  - Xuất JSON: {JSON_OUT}")

    print("\n📊 Thống kê trường bị thiếu:")
    for key, count in missing_stats.items():
        pct = (count / len(raw_data)) * 100 if len(raw_data) > 0 else 0
        print(f"  • {key:<12}: {count:>5} ({pct:.2f}%) thiếu dữ liệu")

    return cleaned


# ==============================================
# 🚀 CHẠY TRỰC TIẾP
# ==============================================
if __name__ == "__main__":
    data = clean_json_files()
    print(f"\n🎉 Hoàn tất! Tổng cộng {len(data)} anime hợp lệ sau khi làm sạch.")
