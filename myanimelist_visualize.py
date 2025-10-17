import os
import json
import warnings
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib import font_manager

# =====================================================
# 0️⃣ Cấu hình chung
# =====================================================
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
sns.set_theme(style="whitegrid", palette="muted")

# Hạn chế lỗi font khi có emoji
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["font.family"] = "DejaVu Sans"

OUTPUT_DIR = "charts_myanimelist"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =====================================================
# 1️⃣ Đọc dữ liệu từ JSON
# =====================================================
JSON_PATH = "/home/levuhao/ProjectBigdataGroup14/MyAnimeList_Crawler_Data/data/mal_all_cleaned.json"

print(f"Đang đọc dữ liệu từ: {JSON_PATH}")
with open(JSON_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

df = pd.DataFrame(data)
print(f"Đã load {len(df)} bản ghi với {len(df.columns)} cột\n")

# =====================================================
# 2️⃣ Tiền xử lý cơ bản
# =====================================================
df = df.dropna(subset=["score", "genres", "season", "year", "favorites", "studios"])
df["genres"] = df["genres"].str.strip()
df["studios"] = df["studios"].str.strip()
df["season"] = df["season"].str.capitalize()

# =====================================================
# 3️⃣ Biểu đồ 1: Trung bình điểm theo Thể loại
# =====================================================
genres_expanded = (
    df.assign(genre=df["genres"].str.split(", "))
    .explode("genre")
    .dropna(subset=["genre"])
)

avg_score_by_genre = (
    genres_expanded.groupby("genre")["score"]
    .agg(["mean", "count"])
    .reset_index()
    .rename(columns={"mean": "avg_score", "count": "anime_count"})
    .sort_values("avg_score", ascending=False)
    .head(15)
)

plt.figure(figsize=(12, 6))
sns.barplot(data=avg_score_by_genre, x="avg_score", y="genre", palette="rocket", legend=False)
plt.title("Trung bình điểm theo Thể loại (Top 15)")
plt.xlabel("Điểm trung bình")
plt.ylabel("Thể loại")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/mal_avg_score_by_genre.png", bbox_inches="tight")
print("mal_avg_score_by_genre.png")

# =====================================================
# 4️⃣ Biểu đồ 2: Mối quan hệ favourites vs score
# =====================================================
plt.figure(figsize=(8, 6))
sns.scatterplot(
    data=df,
    x="score", y="favorites",
    alpha=0.5, color="royalblue"
)
plt.title("Mối quan hệ giữa Favourites và Score")
plt.xlabel("Score")
plt.ylabel("Favourites")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/mal_favourites_vs_score.png", bbox_inches="tight")
print("mal_favourites_vs_score.png")

# =====================================================
# 5️⃣ Biểu đồ 3: Top 10 Studio có nhiều anime nhất
# =====================================================
top_studio_count = (
    df.groupby("studios")["title"]
    .count()
    .reset_index()
    .rename(columns={"title": "anime_count"})
    .sort_values("anime_count", ascending=False)
    .head(10)
)

plt.figure(figsize=(10, 6))
sns.barplot(data=top_studio_count, x="anime_count", y="studios", palette="mako", legend=False)
plt.title("Top 10 Studio có nhiều anime nhất")
plt.xlabel("Số lượng Anime")
plt.ylabel("Studio")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/mal_top_studio_count.png", bbox_inches="tight")
print("mal_top_studio_count.png")

# =====================================================
# 6️⃣ Biểu đồ 4: Số lượng anime theo Season
# =====================================================
anime_by_season = (
    df["season"]
    .value_counts()
    .reset_index()
)
anime_by_season.columns = ["season", "anime_count"]
anime_by_season = anime_by_season.sort_values("anime_count", ascending=False)
print("\nThống kê season:")
print(anime_by_season)

plt.figure(figsize=(8, 6))
sns.barplot(data=anime_by_season, x="season", y="anime_count", palette="coolwarm", legend=False)
plt.title("Số lượng Anime theo Season")
plt.xlabel("Mùa")
plt.ylabel("Số lượng Anime")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/mal_anime_by_season.png", bbox_inches="tight")
print("mal_anime_by_season.png")

# =====================================================
# 7️⃣ Biểu đồ 5: Điểm trung bình theo Season và Year
# =====================================================
avg_score_by_season_year = (
    df.groupby(["year", "season"])["score"]
    .mean()
    .reset_index()
    .pivot(index="year", columns="season", values="score")
    .sort_index()
)

plt.figure(figsize=(10, 7))
sns.heatmap(avg_score_by_season_year, cmap="YlGnBu", annot=True, fmt=".1f")
plt.title("Điểm trung bình theo Season và Year")
plt.xlabel("Season")
plt.ylabel("Year")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/mal_avg_score_by_season_year.png", bbox_inches="tight")
print("mal_avg_score_by_season_year.png")

# =====================================================
# 8️⃣ Tổng kết
# =====================================================
print("\nHoàn tất! Đã sinh ra 5 biểu đồ trong thư mục /charts_myanimelist:")
for f in sorted(os.listdir(OUTPUT_DIR)):
    if f.startswith("mal_"):
        print(f"  - {f}")
