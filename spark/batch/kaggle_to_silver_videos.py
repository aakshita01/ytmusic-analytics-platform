import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, regexp_replace, dayofweek, when, datediff, to_date
)

RAW_KAGGLE = "data/raw/kaggle/USvideos.csv"
SILVER_KAGGLE = "data/silver/yt/videos"

def main():
    spark = SparkSession.builder.appName("YoutubeKaggleETL").getOrCreate()
    df_raw = spark.read.option("header","true").csv(RAW_KAGGLE)
    df_clean = df_raw.select(
        col("video_id"),
        col("title"),
        col("channel_title"),
        col("category_id"),
        to_timestamp("publish_time").alias("publish_time"),
        to_date(col("trending_date"), "yy.dd.MM").alias("process_date"),
        regexp_replace(col("views"),",","").cast("long").alias("views"),
        regexp_replace(col("likes"),",","").cast("long").alias("likes"),
        regexp_replace(col("dislikes"),",","").cast("long").alias("dislikes"),
        regexp_replace(col("comment_count"),",","").cast("long").alias("comment_count"),
        col("tags"),
        col("description"),
        lit(None).cast("string").alias("definition"),
        lit(None).cast("boolean").alias("licensed_content"),
        lit(None).cast("int").alias("duration_seconds")
    )

    df_clean = df_clean.filter(col("process_date").isNotNull())

    df_enriched = df_clean \
                .withColumn("days_since_publish", datediff(col("process_date"), col("publish_time"))) \
                .withColumn("day_of_week", dayofweek("process_date")) \
                .withColumn("is_weekend", when(dayofweek("process_date").isin(1,7),True).otherwise(False)) \
                .withColumn("engagement_rate", (col("likes") + col("comment_count"))/col("views")) \
                .withColumn("dislike_rate", col("dislikes") / col("views")) \
                .withColumn("comment_rate", col("comment_count") / col("views")) \
                .withColumn("like_dislike_ratio", col("likes") / col("dislikes"))
    
    df_enriched.write.mode("overwrite").partitionBy("process_date").parquet(SILVER_KAGGLE)
    print(f"kaggle data written -> {SILVER_KAGGLE}")
    spark.stop()

if __name__=="__main__":
    main()