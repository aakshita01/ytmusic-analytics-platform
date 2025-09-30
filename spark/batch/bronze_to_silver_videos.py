import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, lit, to_timestamp, regexp_extract, dayofweek, when, datediff, explode)
import sys
from pathlib import Path

RAW_DIR = "data/raw/yt/videos/US"
BRONZE_DIR = "data/bronze/yt/videos"
SILVER_DIR = "data/silver/yt/videos"

def parse_duration(df):
    return df.withColumn("duration_seconds",(
        regexp_extract(col("item.contentDetails.duration"), "PT(\\d+)M", 1).cast("int")*60
        + regexp_extract(col("item.contentDetails.duration"), "PT(\\d+)S", 1).cast("int")
    ))
def main(process_date: str):
    spark = SparkSession.builder.appName("YoutubeTrendingETL").getOrCreate()
    raw_path = f"{RAW_DIR}/{process_date}.json"
    bronze_out = f"{BRONZE_DIR}/{process_date}"
    silver_out = f"{SILVER_DIR}/{process_date}"

    df_raw = spark.read.option("multiline", "true").json(raw_path)

    df_items = df_raw.select(explode("items").alias("item"))

    df_items.write.mode("overwrite").parquet(bronze_out)

    df_flat = parse_duration(df_items).select(
        col("item.id").alias("video_id"),
        col("item.snippet.title").alias("title"),
        col("item.snippet.channelTitle").alias("channel_title"),
        col("item.snippet.categoryId").alias("category_id"),
        to_timestamp("item.snippet.publishedAt").alias("publish_time"),
        col("item.contentDetails.definition").alias("definition"),
        col("item.contentDetails.licensedContent").alias("licensed_content"),
        col("duration_seconds"),
        col("item.statistics.viewCount").cast("long").alias("views"),
        col("item.statistics.likeCount").cast("long").alias("likes"),
        lit(None).cast("long").alias("dislikes"),
        col("item.statistics.commentCount").cast("long").alias("comment_count"),
        col("item.snippet.tags").alias("tags"),
        col("item.snippet.description").alias("description"),
        lit(process_date).alias("process_date")
    )

    df_enriched = df_flat \
                .withColumn("days_since_publish", datediff(col("process_date"), col("publish_time"))) \
                .withColumn("day_of_week", dayofweek("process_date")) \
                .withColumn("is_weekend", when(dayofweek("process_date").isin(1,7),True).otherwise(False)) \
                .withColumn("engagement_rate", (col("likes") + col("comment_count"))/col("views")) \
                .withColumn("dislike_rate", col("dislikes") / col("views")) \
                .withColumn("comment_rate", col("comment_count") / col("views")) \
                .withColumn("like_dislike_ratio", col("likes") / col("dislikes"))


    df_enriched.write.mode("overwrite").parquet(silver_out)

    print(f"Bronze written -> {bronze_out}")
    print(f"silver written -> {silver_out}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python bronze_to_silver_videos.py <process_date YYYY-MM-DD")
        sys.exit(1)
    main(sys.argv[1])
