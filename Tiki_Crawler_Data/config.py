# ==============================
# ⚙️ CẤU HÌNH CHUNG CHO DỰ ÁN
# ==============================

# Thông tin danh mục Tiki (sách tiếng Việt)
URL_KEY = "sach-truyen-tieng-viet"
CATEGORY_ID = 316
LIMIT = 20           # số sản phẩm mỗi trang
MAX_PAGE = 500       # giới hạn tối đa (để dừng an toàn)

# Cấu hình MySQL trong WSL
MYSQL_DSN = {
    "host": "localhost",
    "user": "root",
    "password": "Hao@23133020",   # thay bằng mật khẩu của bạn
    "database": "tiki",
    "charset": "utf8mb4",
    "ssl_disabled": True
}

# Thư mục lưu dữ liệu JSON
DATA_DIR = "data"
