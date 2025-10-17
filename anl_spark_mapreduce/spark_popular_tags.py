from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, trim, lower, col, count

# ============================================
# 1️⃣ Khởi tạo SparkSession
# ============================================
spark = SparkSession.builder \
    .appName("PopularTags") \
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
# 4️⃣ Tách và xử lý cột tags
# - split(): tách chuỗi tags bằng dấu phẩy
# - explode(): mỗi tag thành 1 dòng
# - trim() + lower(): loại bỏ khoảng trắng, chuẩn hóa chữ thường
# ============================================
tags_df = df.select(explode(split(col("tags"), ",")).alias("tag"))
tags_df = tags_df.select(trim(lower(col("tag"))).alias("tag"))

# ============================================
# 5️⃣ Đếm số lần xuất hiện của mỗi tag
# ============================================
tag_count = tags_df.groupBy("tag").agg(
    count("*").alias("tag_count")
).orderBy(col("tag_count").desc())

# ============================================
# 6️⃣ Ghi kết quả ra HDFS
# ============================================
output_path = "hdfs:///user/hive/warehouse/output/popular_tags"

tag_count.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

# ============================================
# 7️⃣ Hiển thị Top 10 tag phổ biến nhất
# ============================================
print("=== 🔖 Top 10 Popular Tags ===")
tag_count.show(10, truncate=False)

# ============================================
# 8️⃣ Dừng SparkSession
# ============================================
spark.stop()
