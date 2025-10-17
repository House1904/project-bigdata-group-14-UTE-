import happybase

try:
    # Thay 'localhost' bằng IP hoặc hostname của HBase Thrift server nếu khác
    connection = happybase.Connection('localhost', port=9090)

    # Thử kết nối
    connection.open()

    # In ra danh sách bảng hiện có
    tables = connection.tables()
    print("✅ Kết nối thành công tới HBase Thrift Server!")
    print("📋 Danh sách bảng HBase:", tables)

    connection.close()

except Exception as e:
    print("❌ Kết nối thất bại!")
    print("Lỗi chi tiết:", e)
