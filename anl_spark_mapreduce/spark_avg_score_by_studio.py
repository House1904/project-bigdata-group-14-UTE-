from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round

# ============================================
# 1️⃣ Khởi tạo SparkSession
# ============================================
spark = SparkSession.builder \
    .appName("AvgScoreByStudio") \
    .getOrCreate()

# ============================================
# 2️⃣ Đọc dữ liệu từ HDFS (định dạng tab và quote)
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
# 4️⃣ Làm sạch dữ liệu
# - Chỉ lấy các dòng có studio và score hợp lệ
# ============================================
df_clean = df.filter(
    (col("studio").isNotNull()) & (col("studio") != "") &
    (col("score").isNotNull())
)

# ============================================
# 5️⃣ Tính trung bình điểm (score) theo từng studio
# ============================================
avg_score_df = df_clean.groupBy("studio").agg(
    round(avg(col("score")), 2).alias("avg_score")
).orderBy(col("avg_score").desc())

# ============================================
# 6️⃣ Ghi kết quả ra HDFS
# ============================================
output_path = "hdfs:///user/hive/warehouse/output/avg_score_by_studio"

avg_score_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

# ============================================
# 7️⃣ Hiển thị top 10 studio có điểm trung bình cao nhất
# ============================================
print("=== 🎬 Top 10 Studio có điểm trung bình cao nhất ===")
avg_score_df.show(10, truncate=False)

# ============================================
# 8️⃣ Dừng SparkSession
# ============================================
spark.stop()
