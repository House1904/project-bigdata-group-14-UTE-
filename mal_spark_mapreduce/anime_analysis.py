from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, split, explode, count

# ============================================
# 1️⃣ Khởi tạo SparkSession
# ============================================
spark = SparkSession.builder \
    .appName("AnimeAnalysis") \
    .getOrCreate()

# ============================================
# 2️⃣ Đọc file CSV từ HDFS (định dạng tab và quote)
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
# 4️⃣ Tính trung bình điểm (score) theo trạng thái phát hành (status)
# ============================================
avg_score_by_status = df.groupBy("status").agg(
    avg("score").alias("avg_score")
).orderBy("avg_score", ascending=False)

print("=== 🎬 Trung bình điểm theo trạng thái (Status) ===")
avg_score_by_status.show(10, truncate=False)

# ============================================
# 5️⃣ Phân tích theo thể loại (genre)
# ============================================
# Tách danh sách genre (vd: "Action, Drama, Sci-Fi") thành từng dòng
df_genre = df.withColumn("genre", explode(split(df["genres"], ", ")))

# Tính trung bình score, favorites, và số lượng anime theo từng genre
genre_analysis = df_genre.groupBy("genre").agg(
    avg("score").alias("avg_score"),
    avg("favorites").alias("avg_favorites"),
    count("*").alias("anime_count")
).orderBy("avg_score", ascending=False)

print("=== 📊 Phân tích theo Thể loại (Genre Analysis) ===")
genre_analysis.show(10, truncate=False)

# ============================================
# 6️⃣ Lưu kết quả ra HDFS
# ============================================
avg_score_by_status.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("hdfs:///user/hive/warehouse/output/status_avg_score")

genre_analysis.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("hdfs:///user/hive/warehouse/output/genre_analysis")

# ============================================
# 7️⃣ Dừng Spark
# ============================================
spark.stop()
