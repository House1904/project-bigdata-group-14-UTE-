import json
import os
import config


def merge_and_clean_json():
    """
    Gộp & làm sạch dữ liệu JSON trong /data:
    - Loại trùng ID
    - Bỏ sản phẩm thiếu id/title/price
    - Kiểm tra định dạng giá trị
    """
    files = sorted([f for f in os.listdir(config.DATA_DIR) if f.startswith("tiki_page_")])
    if not files:
        print("⚠️ Không có file tiki_page_*.json trong thư mục data/.")
        return

    merged = []
    seen_ids = set()

    print(f"🔍 Đang quét {len(files)} file JSON trong {config.DATA_DIR}/...")
    for fname in files:
        fpath = os.path.join(config.DATA_DIR, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
                for item in data:
                    pid = item.get("id")
                    title = item.get("title")
                    price = item.get("price")

                    if not pid or not title or price is None:
                        continue
                    if pid in seen_ids:
                        continue
                    if not isinstance(pid, (int, str)) or not isinstance(price, (int, float)):
                        continue
                    merged.append(item)
                    seen_ids.add(pid)
        except Exception as e:
            print(f"⚠️ Lỗi khi đọc {fname}: {e}")

    output_file = os.path.join(config.DATA_DIR, "tiki_all_books_cleaned.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"✅ Đã làm sạch & gộp {len(merged)} sản phẩm → {output_file}")


if __name__ == "__main__":
    merge_and_clean_json()
