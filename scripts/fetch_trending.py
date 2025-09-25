import os
import json
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

API_URL = "https://www.googleapis.com/youtube/v3/videos"

def fetch_trending_music(api_key: str, region: str = 'US', max_results: int =50):
    params = {
        "part": "snippet, statistics, contentDetails",
        "chart": "mostPopular",
        "regionCode": region,
        "videoCategoryId": "10",
        "maxResults": str(max_results),
        "key": api_key,
    }

    items = []
    page_token = None
    while True:
        if page_token:
            params["pageToken"] = page_token
        else:
            params.pop("pageToken", None)
        
        resp = requests.get(API_URL,params = params, timeout = 30)
        if resp.status_code!=200:
            raise RuntimeError(f"API error {resp.status_code}: {resp.text}")
        data = resp.json()

        items.extend(data.get("items", []))
        page_token = data.get("nextPageToken")

        time.sleep(0.25)

        if not page_token:
            break
    return items

def main():
    load_dotenv()
    api_key = os.getenv('YT_API_KEY')
    if not api_key:
        raise RuntimeError("Missing API_KEY in environment (.evn)")
    parser = argparse.ArgumentParser(description="Fetch trending Youtube music videos to raw zone.")
    parser.add_argument("--region", default="US", help="ISO-2 region code (default: US)")
    args = parser.parse_args()

    items = fetch_trending_music(api_key = api_key, region = args.region)

    today = datetime.now(timezone.utc).date().isoformat()
    out_dir = Path("data/raw/yt/videos") / args.region.upper()
    out_dir.mkdir(parents=True, exist_ok = True)
    out_path = out_dir / f"{today}.json"

    payload = {
        "metadata" : {
            "source" : "youtube_v3_videos",
            "region" : args.region.upper(),
            "videoCategoryId" : 10,
            "chart" : "mostPopular",
            "utc_saved_at" : datetime.now(timezone.utc).isoformat()
        },
        "items" : items,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    
    print(f"Saved {len(items)} items -> {out_path}")

if __name__ == "__main__":
    main()
