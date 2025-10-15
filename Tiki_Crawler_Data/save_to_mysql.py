import os
import json
import mysql.connector
import config


def save_to_mysql():
    """Đọc file tiki_all_books_cleaned.json và ghi vào MySQL"""
    file_path = os.path.join(config.DATA_DIR, "tiki_all_books_cleaned.json")
    if not os.path.exists(file_path):
        print("❌ Không tìm thấy file tiki_all_books_cleaned.json. Hãy chạy cleaner.py trước.")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = mysql.connector.connect(**config.MYSQL_DSN)
    cur = conn.cursor()

    # Làm mới bảng để đồng bộ hoàn toàn
    cur.execute("TRUNCATE TABLE tiki_books;")
    conn.commit()

    query = """
        INSERT INTO tiki_books (
            id, title, price, discount, rating, reviews,
            quantity_sold, original_price,
            category_l1, category_l2, category_l3, category_l4, primary_category, url
        ) VALUES (
            %(id)s, %(title)s, %(price)s, %(discount)s, %(rating)s, %(reviews)s,
            %(quantity_sold)s, %(original_price)s,
            %(category_l1)s, %(category_l2)s, %(category_l3)s, %(category_l4)s, %(primary_category)s, %(url)s
        );
    """

    count = 0
    for item in data:
        try:
            cur.execute(query, item)
            count += 1
            if count % 200 == 0:
                conn.commit()
        except Exception as e:
            print(f"⚠️ Bỏ qua ID={item.get('id')}: {e}")

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Đã lưu {count} sản phẩm vào bảng tiki_books.")


if __name__ == "__main__":
    save_to_mysql()
