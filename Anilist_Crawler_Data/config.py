# config.py
import os

# URL GraphQL endpoint
GRAPHQL_URL = "https://graphql.anilist.co"

# Cấu hình thư mục
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Giới hạn dữ liệu
PER_PAGE = 50          # số anime mỗi trang
MAX_RECORDS = 2000     # tổng số anime tối đa
DELAY = 1.5            # nghỉ giữa các request (giây)

# MySQL Connection (nếu dùng)
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Hao@23133020",
    "database": "anilist",
    "charset": "utf8mb4"
}

