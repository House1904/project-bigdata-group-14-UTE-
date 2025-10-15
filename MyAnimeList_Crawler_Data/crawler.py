import requests
import json
import os
import time
import random
import config

os.makedirs("data", exist_ok=True)

def extract_fields(item):
    """Chọn lọc các trường cần thiết từ JSON API Jikan"""
    return {
        "mal_id": item.get("mal_id"),
        "title": item.get("title"),
        "approved": item.get("approved"),
        "type": item.get("type"),
        "episodes": item.get("episodes"),
        "year": item.get("year"),
        "season": item.get("season"),
        "status": item.get("status"),
        "score": item.get("score"),
        "scored_by": item.get("scored_by"),
        "rank": item.get("rank"),
        "popularity": item.get("popularity"),
        "members": item.get("members"),
        "favorites": item.get("favorites"),
        "genres": ", ".join([g["name"] for g in item.get("genres", [])]) if item.get("genres") else None,
        "studios": ", ".join([s["name"] for s in item.get("studios", [])]) if item.get("studios") else None,
        "demographics": ", ".join([d["name"] for d in item.get("demographics", [])]) if item.get("demographics") else None,
        "url": item.get("url")
    }

def crawl_page(page):
    url = f"{config.BASE_URL}?page={page}"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            print(f"⚠️ Lỗi HTTP {r.status_code} ở trang {page}")
            return []

        data = r.json().get("data", [])
        if not data:
            return []

        filtered = [extract_fields(d) for d in data]

        with open(f"data/mal_page_{page}.json", "w", encoding="utf-8") as f:
            json.dump(filtered, f, ensure_ascii=False, indent=2)

        print(f"📄 Trang {page}: {len(filtered)} anime đã lưu → data/mal_page_{page}.json")
        return filtered

    except Exception as e:
        print(f"❌ Lỗi khi crawl trang {page}: {e}")
        return []


if __name__ == "__main__":
    total = 0
    all_data = []

    print("🚀 Bắt đầu crawl dữ liệu từ MyAnimeList (Jikan API)...")

    for p in range(1, config.MAX_PAGE + 1):
        print(f"\n📦 Đang crawl trang {p} ...")
        items = crawl_page(p)
        if not items:
            print("✅ Hết dữ liệu hoặc lỗi kết nối. Dừng crawl.")
            break

        all_data.extend(items)
        total += len(items)
        time.sleep(random.uniform(1.5, 2.5))

    with open("data/mal_all_filtered.json", "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"\n🎉 Hoàn tất! Đã lưu {total} anime vào ./data/mal_all_filtered.json")
