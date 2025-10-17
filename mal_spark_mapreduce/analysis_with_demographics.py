from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

# ============================================
# 1️⃣ Khởi tạo SparkSession
# ============================================
spark = SparkSession.builder \
    .appName("AnimeStudioFavoritesWithDemographics") \
    .getOrCreate()

# ============================================
# 2️⃣ Đọc file CSV từ HDFS (chuẩn định dạng tab và quote)
# ============================================
df = spark.read \
    .option("inferSchema", "true") \
    .option("header", "false") \
    .option("sep", "\t") \
    .option("quote", '"') \
    .csv("hdfs:///user/hive/warehouse/mal_anime/part-m-00000")

# ============================================
# 3️⃣ Gán tên cột phù hợp
# ============================================
df = df.toDF(
    "mal_id", "title", "approved", "type", "episodes", "year", "season",
    "status", "score", "scored_by", "rank", "popularity",
    "members", "favorites", "genres", "studios", "demographics", "url"
)

# ============================================
# 4️⃣ Tính trung bình favorites theo studio
# ============================================
favorites_by_studio = df.groupBy("studios").agg(
    avg("favorites").alias("avg_favorites")
).orderBy("avg_favorites", ascending=False)

print("=== 🎬 Trung bình Favorites theo Studio ===")
favorites_by_studio.show(10, truncate=False)

# ============================================
# 5️⃣ Tính số lượng anime theo demographics (Shounen, Seinen, Josei, Shoujo, ...)
# ============================================
demographic_count = df.groupBy("demographics").agg(
    count("*").alias("anime_count")
).orderBy("anime_count", ascending=False)

print("=== 📊 Số lượng Anime theo Demographics ===")
demographic_count.show(10, truncate=False)

# ============================================
# 6️⃣ Ghi kết quả ra HDFS
# ============================================
favorites_by_studio.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("hdfs:///user/hive/warehouse/output/studio_avg_favorites")

demographic_count.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("hdfs:///user/hive/warehouse/output/demographics_count")

# ============================================
# 7️⃣ Dừng SparkSession
# ============================================
spark.stop()
