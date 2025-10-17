from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, split, trim, col, lower, count, avg,
    rank, round, desc
)
from pyspark.sql.window import Window

# ============================================================
# 1️⃣ Khởi tạo SparkSession
# ============================================================
spark = SparkSession.builder.appName("GenreAdvancedStats").getOrCreate()

# ============================================================
# 2️⃣ Đọc dữ liệu từ HDFS (chuẩn định dạng tab và quote)
# ============================================================
input_path = "hdfs:///user/hive/warehouse/anl_anime/part-m-00000"

df = (
    spark.read
    .option("header", "false")
    .option("inferSchema", "true")
    .option("sep", "\t")          
    .option("quote", '"')         
    .csv(input_path)
)

# ============================================================
# 3️⃣ Gán tên cột (theo cấu trúc AniList)
# ============================================================
df = df.toDF(
    "id", "title_romaji", "title_english", "title_native", "format",
    "episodes", "duration", "source", "country", "isAdult",
    "score", "meanScore", "popularity", "favourites", "trending",
    "genres", "tags", "status", "studio", "season",
    "start_year", "url"
)

# ============================================================
# 4️⃣ Làm sạch dữ liệu cơ bản
# ============================================================
df_clean = df.filter(
    (col("genres").isNotNull()) &
    (col("score").isNotNull()) &
    (col("popularity").isNotNull()) &
    (col("studio").isNotNull())
)

# ============================================================
# 5️⃣ Xử lý cột "genres": tách & chuẩn hóa từng thể loại
# ============================================================
genres_df = (
    df_clean
    .withColumn("genre", explode(split(col("genres"), ",")))
    .select(
        trim(lower(col("genre"))).alias("genre"),
        col("score").cast("double").alias("score"),
        col("popularity").cast("int").alias("popularity"),
        trim(col("studio")).alias("studio"),
    )
    .filter(col("genre") != "")
)

# ============================================================
# 6️⃣ Thống kê cơ bản theo genre
# ============================================================
genre_stats = (
    genres_df.groupBy("genre")
    .agg(
        count("*").alias("anime_count"),
        round(avg("score"), 2).alias("avg_score"),
        round(avg("popularity"), 0).alias("avg_popularity")
    )
)

# ============================================================
# 7️⃣ Tính studio phổ biến nhất trong từng genre
# ============================================================
studio_count = genres_df.groupBy("genre", "studio").agg(
    count("*").alias("studio_count")
)

windowSpec = Window.partitionBy("genre").orderBy(desc("studio_count"))

top_studios = (
    studio_count
    .withColumn("rank", rank().over(windowSpec))
    .filter(col("rank") == 1)
    .select("genre", col("studio").alias("top_studio"))
)

# ============================================================
# 8️⃣ Gộp kết quả thống kê & studio top
# ============================================================
final_result = (
    genre_stats.join(top_studios, on="genre", how="left")
    .orderBy(col("anime_count").desc())
)

# ============================================================
# 9️⃣ Ghi kết quả ra HDFS
# ============================================================
output_path = "hdfs:///user/hive/warehouse/output/genre_advanced_stats"

final_result.write.mode("overwrite").option("header", "true").csv(output_path)

# ============================================================
# 🔟 Hiển thị top 10 ra console
# ============================================================
print("=== 🎬 Genre Advanced Stats (Top 10) ===")
final_result.show(10, truncate=False)

# ============================================================
# 🔚 Dừng SparkSession
# ============================================================
spark.stop()
