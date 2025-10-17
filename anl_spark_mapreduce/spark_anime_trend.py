from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# ============================================
# 1️⃣ Khởi tạo SparkSession
# ============================================
spark = SparkSession.builder \
    .appName("AnimeTrendByYearSeason") \
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
# 4️⃣ Lọc dữ liệu hợp lệ (có start_year hoặc season)
# ============================================
df_year = df.filter(col("start_year").isNotNull())
df_season = df.filter(col("season").isNotNull())

# ============================================
# 5️⃣ Đếm số lượng anime theo NĂM phát hành
# ============================================
anime_by_year = df_year.groupBy("start_year").agg(
    count("*").alias("anime_count")
).orderBy(col("start_year").asc())

# ============================================
# 6️⃣ Đếm số lượng anime theo MÙA phát sóng
# ============================================
anime_by_season = df_season.groupBy("season").agg(
    count("*").alias("anime_count")
).orderBy(col("anime_count").desc())

# ============================================
# 7️⃣ Ghi kết quả ra HDFS
# ============================================
output_year = "hdfs:///user/hive/warehouse/output/anime_by_year"
output_season = "hdfs:///user/hive/warehouse/output/anime_by_season"

anime_by_year.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_year)

anime_by_season.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_season)

# ============================================
# 8️⃣ Hiển thị vài dòng kiểm tra kết quả
# ============================================
print("=== 🎬 Số lượng Anime theo Năm phát hành ===")
anime_by_year.show(10, truncate=False)

print("=== 🍁 Số lượng Anime theo Mùa phát sóng ===")
anime_by_season.show(10, truncate=False)

# ============================================
# 9️⃣ Dừng SparkSession
# ============================================
spark.stop()
