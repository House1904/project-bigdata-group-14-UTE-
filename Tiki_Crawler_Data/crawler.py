import requests
import json
import time
import os
import config


def crawl_products(page=1):
    """Cào dữ liệu sản phẩm của 1 trang"""
    url = (
        f"https://tiki.vn/api/personalish/v1/blocks/listings"
        f"?limit={config.LIMIT}&sort=top_seller"
        f"&page={page}&urlKey={config.URL_KEY}&category={config.CATEGORY_ID}"
    )
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}

    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        data = res.json().get("data", [])
        return data
    except Exception as e:
        print(f"⚠️ Trang {page} lỗi: {e}")
        return []


def extract_fields(product):
    """Lấy các trường cần thiết"""
    amp = product.get("visible_impression_info", {}).get("amplitude", {})
    return {
        "id": product.get("id"),
        "title": product.get("name"),
        "price": product.get("price"),
        "discount": product.get("discount_rate"),
        "rating": float(product.get("rating_average") or 0),
        "reviews": product.get("review_count"),
        "quantity_sold": (product.get("quantity_sold") or {}).get("value", 0),
        "original_price": product.get("original_price", 0),
        "category_l1": amp.get("category_l1_name", ""),
        "category_l2": amp.get("category_l2_name", ""),
        "category_l3": amp.get("category_l3_name", ""),
        "category_l4": amp.get("category_l4_name", ""),
        "primary_category": amp.get("primary_category_name", ""),
        "url": f"https://tiki.vn/{product.get('url_path', '')}"
    }


def save_crawled_data():
    """Cào toàn bộ danh mục và lưu JSON"""
    os.makedirs(config.DATA_DIR, exist_ok=True)
    total = 0
    all_data = []

    print(f"🚀 Bắt đầu crawl danh mục: {config.URL_KEY} (ID={config.CATEGORY_ID})")

    for page in range(1, config.MAX_PAGE + 1):
        print(f"\n📦 Đang crawl trang {page} ...")
        products = crawl_products(page)
        if not products:
            print("✅ Hết dữ liệu hoặc bị chặn, dừng crawl.")
            break

        filtered = [extract_fields(p) for p in products]

        # Lưu từng trang riêng
        page_file = os.path.join(config.DATA_DIR, f"tiki_page_{page}.json")
        with open(page_file, "w", encoding="utf-8") as f:
            json.dump(filtered, f, ensure_ascii=False, indent=2)
        print(f"💾 Đã lưu {len(filtered)} sản phẩm → {page_file}")

        all_data.extend(filtered)
        total += len(filtered)
        time.sleep(1.5)

    # Gộp toàn bộ vào file tổng
    all_file = os.path.join(config.DATA_DIR, "tiki_all_books.json")
    with open(all_file, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"\n🎉 Crawl hoàn tất ({total} sản phẩm). File tổng: {all_file}")


if __name__ == "__main__":
    save_crawled_data()
