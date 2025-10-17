# config.py

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Hao@23133020",   
    "database": "mal",
    "port": 3306,
    "charset": "utf8mb4",
    "use_pure": True,
    "autocommit": True
}

# Nếu bạn muốn dùng DSN dạng dict (cho các file khác)
MYSQL_DSN = MYSQL_CONFIG

# Endpoint Jikan API
BASE_URL = "https://api.jikan.moe/v4/top/anime"

# Giới hạn số trang lấy dữ liệu
MAX_PAGE = 50  # mỗi trang có ~25 anime → 50 trang ≈ 1250 anime