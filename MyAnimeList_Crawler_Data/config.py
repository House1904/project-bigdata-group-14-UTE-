# config.py
MYSQL_DSN = {
    "host": "localhost",
    "user": "root",
    "password": "Hao@23133020",  # đổi theo mật khẩu của em
    "database": "mal",
    "charset": "utf8mb4",
    "ssl_disabled": True
}

# Jikan API endpoint
BASE_URL = "https://api.jikan.moe/v4/top/anime"
MAX_PAGE = 50   # mỗi trang có ~25 anime → 50 trang ≈ 1250 anime
