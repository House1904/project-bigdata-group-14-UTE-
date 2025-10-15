import json
import os

INPUT_FILE = "data/mal_all_filtered.json"
OUTPUT_FILE = "data/mal_all_cleaned.json"

def validate_record(item):
    """Kiểm tra định dạng dữ liệu anime"""
    required_fields = [
        "mal_id", "title", "type", "episodes",
        "status", "score", "members", "url"
    ]
    for field in required_fields:
        if field not in item or item[field] in [None, ""]:
            return False

    if not isinstance(item["mal_id"], int) or item["mal_id"] <= 0:
        return False
    if not isinstance(item["title"], str) or not item["title"].strip():
        return False
    if not isinstance(item["type"], str):
        return False
    if item["episodes"] is not None and not isinstance(item["episodes"], int):
        return False

    try:
        score = float(item["score"])
        if score < 0 or score > 10:
            return False
    except Exception:
        return False

    if not isinstance(item["members"], int) or item["members"] < 0:
        return False

    if not str(item["url"]).startswith("https://myanimelist.net/anime/"):
        return False

    return True


def clean_file():
    if not os.path.exists(INPUT_FILE):
        print(f"⚠️ Không tìm thấy file: {INPUT_FILE}")
        return

    print(f"🔍 Đang làm sạch dữ liệu trong: {INPUT_FILE}")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    cleaned = []
    seen_ids = set()

    for item in data:
        mid = item.get("mal_id")
        if not mid or mid in seen_ids:
            continue
        if validate_record(item):
            cleaned.append(item)
            seen_ids.add(mid)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    print(f"✅ Làm sạch hoàn tất → {OUTPUT_FILE}")
    print(f"🧹 Giữ lại {len(cleaned)} / {len(data)} bản ghi hợp lệ.")


if __name__ == "__main__":
    clean_file()
