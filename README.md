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

---

### Day 4: Gold Layer Analytics (BI-Ready Datasets)
- Created spark/batch/silver_to_gold_analytics.py to aggregate Silver data into Gold (BI-ready) datasets.
- Designed analytical datasets for BI dashboards & reporting use cases.
- Stored outputs in data/gold/{dataset_name}/.
- Generated datasets:
    1) Daily Summary → Key metrics aggregated by process_date.
    2) Weekly Summary → Engagement trends grouped by week(process_date).
    3) Top Channels Weekly → Most popular channels ranked by total views, likes, and comments.

- Example Gold schemas:

1) Daily Summary

| Column            | Type    | Description                       |
|-------------------|---------|-----------------------------------|
| process_date      | string  | Snapshot date                     |
| total_videos      | long    | Number of videos ingested         |
| total_views	    | long    |	Sum of views across all videos    |
| total_likes	    | long	  | Sum of likes across all videos    |
| total_comments	| long	  | Sum of comments across all videos |

2) Weekly Summary

| Column            | Type | Description
|-------------------|------|------------------------------|
| week_start        | date | Start of the week            |
| total_videos      | long | Number of videos in the week |
| total_views       | long | Total views in the week      |
| total_likes       | long | Total likes in the week      |
| total_comments    | long | Total comments in the week   |

3) Top Channels Weekly

| Column            | Type   | Description                       |
|-------------------|--------|-----------------------------------|
| week_start        | date   | Start of the week                 |
| channel_title     | string | Channel name                      |
| total_views       | long   | Total views for the channel       |
| total_likes       | long   | Total likes for the channel       | 
| total_comments    | long   | Total comments for the channel    |
| rank              | int    | Channel rank based on total views |