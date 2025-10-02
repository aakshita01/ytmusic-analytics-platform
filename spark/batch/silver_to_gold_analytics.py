import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, avg, rank, weekofyear, sum as _sum)
from pyspark.sql.window import Window

SILVER_DIR = "data/silver/yt/videos"
GOLD_DIR = "data/gold/yt/analytics"

def main():
    spark = SparkSession.builder.appName("process_date").getOrCreate()
    df_silver = spark.read.option("basePath", "data/silver/yt/videos").parquet("data/silver/yt/videos/*")
    w_daily = Window.partitionBy("process_date").orderBy(col("views").desc())

    #top videos daily
    df_top_videos_daily = (df_silver.withColumn("rank", rank().over(w_daily))
                                    .filter(col("rank")<=10)
                                    .select("process_date", "video_id", "title", "channel_title",
                                            "views", "likes", "comment_count", "engagement_rate", "rank"))
    df_top_videos_daily.write.mode("overwrite").parquet(f"{GOLD_DIR}/top_videos_daily")

    #top channels weekly
    df_weekly = (df_silver.withColumn("week", weekofyear("publish_time"))
                          .groupBy("week", "channel_title")
                          .agg(avg("engagement_rate").alias("avg_engagement_rate"),
                               _sum("views").alias("total_views"))
    )
    w_weekly = Window.partitionBy("week").orderBy(col("avg_engagement_rate"))
    df_top_channels_weekly = (df_weekly.withColumn("rank", rank().over(w_weekly))
                                         .filter(col("rank")<=10)
    )
    df_top_channels_weekly.write.mode("overwrite").parquet(f"{GOLD_DIR}/top_channels_weekly")
    
    #Daily summary (averages)
    df_daily_summary = (df_silver.groupBy("process_date")
                                 .agg(avg("views").alias("avg_views"),
                                      avg("likes").alias("avg_likes"),
                                      avg("dislikes").alias("avg_dislikes"),
                                      avg("comment_count").alias("avg_comments"))
    )
    df_daily_summary.write.mode("overwrite").parquet(f"{GOLD_DIR}/daily_summary")

    print("Gold datasets written ->", GOLD_DIR)
    spark.stop()

if __name__ == "__main__":
    main()