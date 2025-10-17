from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# 1️⃣ Tạo SparkSession
spark = SparkSession.builder.appName("AnimeStudioFavorites").getOrCreate()

# 2️⃣ Đọc file CSV từ HDFS (chuẩn định dạng tab và quote)
df = spark.read \
    .option("inferSchema", "true") \
    .option("header", "false") \
    .option("sep", "\t") \
    .option("quote", '"') \
    .csv("hdfs:///user/hive/warehouse/mal_anime/part-m-00000")

# 3️⃣ Gán tên cột phù hợp
df = df.toDF(
    "mal_id", "title", "approved", "type", "episodes", "year", "season", "status",
    "score", "scored_by", "rank", "popularity", "members", "favorites",
    "genres", "studios", "demographics", "url"
)

# 4️⃣ Loại bỏ dòng studios null hoặc rỗng
df_clean = df.filter(df["studios"].isNotNull() & (df["studios"] != ""))

# 5️⃣ Tính trung bình favorites theo studio
favorites_by_studio = df_clean.groupBy("studios").agg(
    avg("favorites").alias("avg_favorites")
).orderBy("avg_favorites", ascending=False)

# 6️⃣ Hiển thị kết quả
print("=== Trung bình Favorites theo Studio ===")
favorites_by_studio.show(10, truncate=False)

# 7️⃣ Lưu kết quả ra HDFS
favorites_by_studio.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("hdfs:///user/hive/warehouse/output/studio_avg_favorites")

# 8️⃣ Dừng Spark
spark.stop()
