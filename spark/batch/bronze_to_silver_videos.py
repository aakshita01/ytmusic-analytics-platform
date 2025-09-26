import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, explode
import sys
from pathlib import Path

RAW_DIR = "data/raw/yt/videos/US"
BRONZE_DIR = "data/bronze/yt/videos"
SILVER_DIR = "data/silver/yt/videos"

def main(process_date: str):
    spark = SparkSession.builder.appName("YoutubeTrendingETL").getOrCreate()
    raw_path = f"{RAW_DIR}/{process_date}.json"
    bronze_out = f"{BRONZE_DIR}/{process_date}"
    silver_out = f"{SILVER_DIR}/{process_date}"

    df_raw = spark.read.option("multiline", "true").json(raw_path)

    df_items = df_raw.select(explode("items").alias("item"))

    df_flat = df_items.select(
        col("item.id").alias("video_id"),
        col("item.snippet.title").alias("title"),
        col("item.snippet.channelTitle").alias("channel_title"),
        col("item.snippet.publishedAt").alias("published_at"),
        col("item.statistics.viewCount").cast("long").alias("view_count"),
        col("item.statistics.likeCount").cast("long").alias("like_count"),
        col("item.statistics.commentCount").cast("long").alias("comment_count")
    ).withColumn("process_date", lit(process_date))

    df_items.write.mode("overwrite").parquet(bronze_out)
    df_flat.write.mode("overwrite").parquet(silver_out)

    print(f"Bronze written -> {bronze_out}")
    print(f"silver written -> {silver_out}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python bronze_to_silver_videos.py <process_date YYYY-MM-DD")
        sys.exit(1)
    main(sys.argv[1])
