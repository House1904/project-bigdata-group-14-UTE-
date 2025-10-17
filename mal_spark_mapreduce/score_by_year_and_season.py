from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

# 1️⃣ Tạo SparkSession
spark = SparkSession.builder.appName("AnimeScoreByYearAndSeason").getOrCreate()

# 2️⃣ Đọc file CSV từ HDFS (đã làm sạch)
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

# 4️⃣ Bỏ các dòng không có dữ liệu year hoặc season
df_clean = df.filter(df["year"].isNotNull() & df["season"].isNotNull())

# 5️⃣ Tính trung bình score theo từng năm
avg_score_by_year = df_clean.groupBy("year").agg(
    avg("score").alias("avg_score_year"),
    count("*").alias("anime_count_year")
).orderBy("year")

# 6️⃣ Tính trung bình score theo từng mùa
avg_score_by_season = df_clean.groupBy("season").agg(
    avg("score").alias("avg_score_season"),
    count("*").alias("anime_count_season")
).orderBy("avg_score_season", ascending=False)

# 7️⃣ Hiển thị kết quả
print("=== Điểm trung bình theo NĂM ===")
avg_score_by_year.show()

print("=== Điểm trung bình theo MÙA ===")
avg_score_by_season.show()

# 8️⃣ Lưu kết quả ra HDFS
avg_score_by_year.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("hdfs:///user/hive/warehouse/output/year_avg_score")

avg_score_by_season.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("hdfs:///user/hive/warehouse/output/season_avg_score")

# 9️⃣ Dừng Spark
spark.stop()
