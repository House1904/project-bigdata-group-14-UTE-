from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# ============================================
# 1️⃣ Khởi tạo SparkSession
# ============================================
spark = SparkSession.builder \
    .appName("TopAnimeByFormat") \
    .getOrCreate()

# ============================================
# 2️⃣ Đọc dữ liệu từ HDFS (chuẩn định dạng tab và quote)
# ============================================
input_path = "hdfs:///user/hive/warehouse/anl_anime/part-m-00000"

df = spark.read \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .option("sep", "\t") \
    .option("quote", '"') \
    .csv(input_path)

# ============================================
# 3️⃣ Gán tên cột (theo cấu trúc bảng AniList)
# ============================================
df = df.toDF(
    "id", "title_romaji", "title_english", "title_native", "format", "episodes",
    "duration", "source", "country", "isAdult", "score", "meanScore", "popularity",
    "favourites", "trending", "genres", "tags", "status", "studio", "season",
    "start_year", "url"
)

# ============================================
# 4️⃣ Lọc dữ liệu hợp lệ (format và popularity có giá trị)
# ============================================
df_clean = df.filter(
    (col("format").isNotNull()) &
    (col("popularity").isNotNull()) &
    (col("format") != "\\N") &
    (col("format") != "")
)

# ============================================
# 5️⃣ Tạo window để xếp hạng theo popularity trong từng format
# ============================================
windowSpec = Window.partitionBy("format").orderBy(col("popularity").desc())

# ============================================
# 6️⃣ Gán thứ hạng và lấy top 5 mỗi format
# ============================================
df_ranked = df_clean.withColumn("rank", row_number().over(windowSpec))
top5_per_format = df_ranked.filter(col("rank") <= 5)

# ============================================
# 7️⃣ Chọn cột cần thiết
# ============================================
result = top5_per_format.select(
    "format", "rank", "title_romaji", "popularity"
).orderBy("format", "rank")

# ============================================
# 8️⃣ Ghi kết quả ra HDFS
# ============================================
output_path = "hdfs:///user/hive/warehouse/output/top_anime_by_format"

result.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

# ============================================
# 9️⃣ Hiển thị kết quả kiểm tra
# ============================================
print("=== 🏆 Top 5 Anime phổ biến nhất theo từng Format ===")
result.show(20, truncate=False)

# ============================================
# 🔟 Dừng SparkSession
# ============================================
spark.stop()
