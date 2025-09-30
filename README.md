# YouTube Music Analytics & Recommendation Platform

### Day 1: YouTube API Ingestion (Raw Zone)
- Built `scripts/fetch_trending.py` to fetch trending YouTube Music videos via API.  
- Stored daily snapshots in `data/raw/yt/videos/{region}/{date}.json`.  
- Followed `.env` + `.env.example` convention for API key security.  

- Example Raw JSON schema:

| Field          | Type   |
|----------------|--------|
| kind           | string |
| etag           | string |
| items          | array  |
| pageInfo       | struct |

---

### Day 2: PySpark ETL (Bronze → Silver)
- Created `spark/batch/bronze_to_silver_videos.py` for ETL.  
- Raw JSON → Bronze (semi-structured parquet).  
- Bronze → Silver (flattened parquet with typed schema).  

- Example Silver schema:

| Column        | Type   | Nullable |
|---------------|--------|----------|
| video_id      | string | true     |
| title         | string | true     |
| channel_title | string | true     |
| published_at  | string | true     |
| view_count    | long   | true     |
| like_count    | long   | true     |
| comment_count | long   | true     |
| process_date  | string | true     |

---

### Day 3: Kaggle Dataset Integration
- Added historical YouTube trending data (`USvideos.csv`) from Kaggle under `data/raw/kaggle/`.  
- Created `spark/batch/kaggle_to_silver_videos.py` to align Kaggle dataset with Silver schema.  
- Unified API + Kaggle datasets into the same Silver layer for consistency.  
- Added enrichment columns for analytics (engagement rate, like/dislike ratio, days since publish, etc.).  

- Example Enriched Silver schema:

| Column            | Type    | Nullable |
|-------------------|---------|----------|
| video_id          | string  | true     |
| title             | string  | true     |
| channel_title     | string  | true     |
| category_id       | string  | true     |
| publish_time      | ts      | true     |
| process_date      | string  | true     |
| views             | long    | true     |
| likes             | long    | true     |
| dislikes          | long    | true     |
| comment_count     | long    | true     |
| tags              | string  | true     |
| description       | string  | true     |
| definition        | string  | true     |
| licensed_content  | boolean | true     |
| duration_seconds  | int     | true     |
| days_since_publish| int     | true     |
| day_of_week       | int     | true     |
| is_weekend        | boolean | true     |
| engagement_rate   | double  | true     |
| dislike_rate      | double  | true     |
| comment_rate      | double  | true     |
| like_dislike_ratio| double  | true     |

