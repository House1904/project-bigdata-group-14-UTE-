import json
import mysql.connector
import config

def save_to_mysql():
    conn = mysql.connector.connect(**config.MYSQL_DSN)
    cur = conn.cursor()

    # Làm mới dữ liệu mỗi lần import
    cur.execute("TRUNCATE TABLE mal_anime;")
    conn.commit()

    with open("data/mal_all_cleaned.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    query = """
        INSERT INTO mal_anime (
            mal_id, title, approved, type, episodes, year, season,
            status, score, scored_by, `rank`, popularity, members, favorites,
            genres, studios, demographics, url
        ) VALUES (
            %(mal_id)s, %(title)s, %(approved)s, %(type)s, %(episodes)s, %(year)s, %(season)s,
            %(status)s, %(score)s, %(scored_by)s, %(rank)s, %(popularity)s, %(members)s, %(favorites)s,
            %(genres)s, %(studios)s, %(demographics)s, %(url)s
        );
    """

    count = 0
    for item in data:
        cur.execute(query, item)
        count += 1
        if count % 200 == 0:
            conn.commit()

    conn.commit()
    print(f"✅ Đã lưu {count} bản ghi vào bảng mal_anime.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    save_to_mysql()
