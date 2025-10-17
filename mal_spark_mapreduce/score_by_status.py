from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# ============================================
# 1️⃣ Khởi tạo SparkSession
# ============================================
spark = SparkSession.builder \
    .appName("AnimeScoreByStatus") \
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
    "mal_id", "title", "approved", "type", "episodes", "year", "season", "status",
    "score", "scored_by", "rank", "popularity", "members", "favorites",
    "genres", "studios", "demographics", "url"
)

# ============================================
# 4️⃣ Tính trung bình điểm (score) theo trạng thái phát hành (status)
# ============================================
avg_score_by_status = df.groupBy("status").agg(
    avg("score").alias("avg_score")
).orderBy("avg_score", ascending=False)

# ============================================
# 5️⃣ Hiển thị kết quả
# ============================================
print("=== 🎬 Điểm trung bình theo trạng thái phát hành (Status) ===")
avg_score_by_status.show(10, truncate=False)

# ============================================
# 6️⃣ Ghi kết quả ra HDFS
# ============================================
avg_score_by_status.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("hdfs:///user/hive/warehouse/output/status_avg_score")

# ============================================
# 7️⃣ Dừng SparkSession
# ============================================
spark.stop()
