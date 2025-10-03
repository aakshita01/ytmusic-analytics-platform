CREATE OR REPLACE TABLE yt_daily_summary (
    process_date DATE,
    avg_views BIGINT,
    avg_likes BIGINT,
    avg_dislikes BIGINT,
    avg_comments BIGINT
);
CREATE OR REPLACE TABLE yt_top_channels_weekly (
    week INT,
    channel_title STRING,
    avg_engagement_rate DOUBLE,
    total_views BIGINT,
    rank INT
);
CREATE OR REPLACE TABLE yt_top_videos_daily (
    process_date DATE,
    video_id STRING,
    title STRING,
    channel_title STRING,
    views BIGINT,
    likes BIGINT,
    comment_count BIGINT,
    engagement_rate DOUBLE,
    rank INT
);
CREATE OR REPLACE TABLE yt_silver_videos (
    video_id STRING,
    title STRING,
    channel_title STRING,
    category_id STRING,
    publish_time TIMESTAMP_NTZ,
    process_date DATE,
    views BIGINT,
    likes BIGINT,
    dislikes BIGINT,
    comment_count BIGINT,
    tags STRING,
    description STRING,
    definition STRING,
    licensed_content BOOLEAN,
    duration_seconds INT,
    days_since_publish INT,
    day_of_week INT,
    is_weekend BOOLEAN,
    engagement_rate DOUBLE,
    dislike_rate DOUBLE,
    comment_rate DOUBLE,
    like_dislike_ratio DOUBLE
);