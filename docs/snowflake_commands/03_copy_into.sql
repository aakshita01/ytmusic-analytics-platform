COPY INTO yt_silver_videos
FROM @yt_stage
FILES = ('part-00000-098626eb-...snappy.parquet', 'part-00001-...snappy.parquet', ...)
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO yt_daily_summary
FROM @yt_stage
FILES = ('part-00000-8f7dfc28-...snappy.parquet')
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO yt_top_channels_weekly
FROM @yt_stage
FILES = ('part-00000-5889e0ef-...snappy.parquet')
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO yt_top_videos_daily
FROM @yt_stage
FILES = ('part-00000-95b8f721-...snappy.parquet')
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;