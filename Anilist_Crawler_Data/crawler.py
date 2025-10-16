# crawler.py
import requests
import json
import time
import os
from config import GRAPHQL_URL, DATA_DIR, PER_PAGE, MAX_RECORDS, DELAY

# ================= GraphQL Query (mới) =================
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
      title { english }
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

# =======================================================

def fetch_page(page: int):
    """Gửi request GraphQL và lấy dữ liệu 1 trang"""
    variables = {"page": page, "perPage": PER_PAGE}
    res = requests.post(GRAPHQL_URL, json={"query": QUERY, "variables": variables})
    if res.status_code != 200:
        print(f"❌ Lỗi {res.status_code}: {res.text}")
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
            "title_english": item.get("title", {}).get("english"),
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

    path = os.path.join(DATA_DIR, f"anime_page_{page}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(anime_list, f, ensure_ascii=False, indent=4)
    print(f"✅ Đã lưu: {path} ({len(anime_list)} anime)")


def crawl_all():
    """Cào dữ liệu toàn bộ (giới hạn MAX_RECORDS)"""
    total = 0
    page = 1
    while total < MAX_RECORDS:
        print(f"\n🔹 Cào trang {page}...")
        data = fetch_page(page)
        if not data:
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


if __name__ == "__main__":
    crawl_all()
