import requests
import json
import time
import os
from config import GRAPHQL_URL, DATA_DIR, PER_PAGE, MAX_RECORDS, DELAY

# ================= GraphQL Query (cập nhật) =================
QUERY = """
query ($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo {
      total
      currentPage
      lastPage
      hasNextPage
    }
    media(type: ANIME, sort: POPULARITY_DESC) {
      id
      title {
        romaji
        english
        native
      }
      format
      episodes
      duration
      source
      countryOfOrigin
      isAdult
      averageScore
      meanScore
      popularity
      favourites
      trending
      genres
      tags { name }
      status
      studios(isMain: true) { nodes { name } }
      season
      startDate { year }
      siteUrl
    }
  }
}
"""
# ============================================================


def fetch_page(page: int):
    """Gửi request GraphQL và lấy dữ liệu 1 trang"""
    variables = {"page": page, "perPage": PER_PAGE}
    try:
        res = requests.post(GRAPHQL_URL, json={"query": QUERY, "variables": variables})
        res.raise_for_status()
    except Exception as e:
        print(f"❌ Lỗi khi tải trang {page}: {e}")
        return None
    return res.json()


def save_page(page: int, data):
    """Lưu JSON từng trang (đã lọc và chuẩn hóa)"""
    anime_list = []
    media_list = data["data"]["Page"]["media"]

    for item in media_list:
        studios = item.get("studios", {}).get("nodes")
        studio_name = studios[0]["name"] if studios and len(studios) > 0 else None

        tags = [tag["name"] for tag in item.get("tags", []) if tag.get("name")]
        tag_str = ", ".join(tags)

        anime_list.append({
            "id": item.get("id"),
            "title_romaji": item.get("title", {}).get("romaji"),
            "title_english": item.get("title", {}).get("english"),
            "title_native": item.get("title", {}).get("native"),
            "format": item.get("format"),
            "episodes": item.get("episodes"),
            "duration": item.get("duration"),
            "source": item.get("source"),
            "country": item.get("countryOfOrigin"),
            "isAdult": item.get("isAdult"),
            "score": item.get("averageScore"),
            "meanScore": item.get("meanScore"),
            "popularity": item.get("popularity"),
            "favourites": item.get("favourites"),
            "trending": item.get("trending"),
            "genres": ", ".join(item.get("genres", [])),
            "tags": tag_str,
            "status": item.get("status"),
            "studio": studio_name,
            "season": item.get("season"),
            "start_year": item.get("startDate", {}).get("year"),
            "url": item.get("siteUrl")
        })

    # Lưu từng trang riêng
    path = os.path.join(DATA_DIR, f"anime_page_{page}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(anime_list, f, ensure_ascii=False, indent=4)
    print(f"✅ Đã lưu: {path} ({len(anime_list)} anime)")

    return anime_list


def merge_all_pages():
    """Gộp tất cả file anime_page_X.json thành 1 file raw tổng"""
    merged = []
    files = [f for f in os.listdir(DATA_DIR) if f.startswith("anime_page_") and f.endswith(".json")]
    if not files:
        print("⚠️ Không có file nào để gộp.")
        return None

    files.sort(key=lambda x: int(x.split("_")[2].split(".")[0]))  # sắp theo số trang

    for file in files:
        path = os.path.join(DATA_DIR, file)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            merged.extend(data)

    raw_path = os.path.join(DATA_DIR, "anilist_raw.json")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=4)

    print(f"\n📦 Đã gộp tất cả file thành: {raw_path} ({len(merged)} anime tổng cộng)")
    return raw_path


def crawl_all():
    """Cào dữ liệu toàn bộ (giới hạn MAX_RECORDS)"""
    total = 0
    page = 1

    while total < MAX_RECORDS:
        print(f"\n🔹 Cào trang {page}...")
        data = fetch_page(page)
        if not data:
            print(f"⚠️ Dừng lại ở trang {page} do lỗi khi tải dữ liệu.")
            break

        count = len(data["data"]["Page"]["media"])
        save_page(page, data)
        total += count

        page_info = data["data"]["Page"]["pageInfo"]
        if not page_info["hasNextPage"] or total >= MAX_RECORDS:
            print(f"\n🎉 Hoàn tất cào {total} anime!")
            break

        page += 1
        time.sleep(DELAY)

    # Gộp tất cả lại thành 1 file raw
    merge_all_pages()


if __name__ == "__main__":
    crawl_all()
