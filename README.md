# YouTube Music Analytics & Recommendation Platform

### Day 1: YouTube API Ingestion (Raw Zone)
- Built `scripts/fetch_trending.py` to fetch trending YouTube Music videos via API.
- Stored daily snapshots in `data/raw/yt/videos/{region}/{date}.json`.
- Followed `.env` + `.env.example` convention for API key security.

### Day 2: PySpark ETL (Bronze → Silver)
- Created `spark/batch/bronze_to_silver_videos.py` for ETL.
- Stored daily snapshots in `data/raw/yt/videos/{region}/{date}.json`.
- Raw JSON → Bronze (semi-structured parquet).
- Bronze → Silver (flattened parquet with typed schema).
- Example Silver schema:
    |-- video_id: string (nullable = true)
    |-- title: string (nullable = true)
    |-- channel_title: string (nullable = true)
    |-- published_at: string (nullable = true)
    |-- view_count: long (nullable = true)
    |-- like_count: long (nullable = true)
    |-- comment_count: long (nullable = true)
    |-- process_date: string (nullable = true)
