# scripts/python/scrape_reddit_enhanced.py
# Enhanced Reddit scraper that fetches both rising and hot posts
# Usage: python scripts/python/scrape_reddit_enhanced.py

import os, csv, time, datetime as dt, requests
from requests.auth import HTTPBasicAuth
from pathlib import Path

# Reddit API credentials
CLIENT_ID = "vB6jB0g4ZbNM8HeJxcAJGw"
CLIENT_SECRET = "64DQoMZOS8VU5UNkH5YuKJP51K3PSw"
UA = "trendcsv/0.1 (by u/c0ttonpker)"
OUT_DIR = Path("trendingcsv/reddit")

def get_token():
    # App-only OAuth (client_credentials)
    r = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET),
        data={"grant_type": "client_credentials"},
        headers={"User-Agent": UA},
        timeout=20
    )
    r.raise_for_status()
    return r.json()["access_token"]

def fetch_listing(kind="rising", limit=100, token=None):
    # kind: "rising" or "hot"
    headers = {"Authorization": f"bearer {token}", "User-Agent": UA}
    url = f"https://oauth.reddit.com/r/all/{kind}"
    params = {"limit": min(limit, 100), "raw_json": 1}
    r = requests.get(url, headers=headers, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def normalize_items(data, kind):
    now = dt.datetime.utcnow()
    out = []
    for child in data.get("data", {}).get("children", []):
        d = child.get("data", {})
        if not d or d.get("stickied") or d.get("removed_by_category"):
            continue
        # Basic fields
        title = d.get("title") or ""
        permalink = d.get("permalink") or ""
        url = f"https://reddit.com{permalink}" if permalink else d.get("url_overridden_by_dest") or ""
        subreddit = d.get("subreddit") or ""
        created_utc = dt.datetime.utcfromtimestamp(d.get("created_utc", time.time()))
        age_h = max((now - created_utc).total_seconds() / 3600.0, 0.25)
        score = float(d.get("score", 0))
        comments = int(d.get("num_comments", 0))
        velocity = score / age_h  # simple "rising" signal

        out.append({
            "source": "reddit",
            "title": title,
            "url": url,
            "region": "GLOBAL",
            "observed_at": now.replace(microsecond=0).isoformat() + "Z",
            "raw_metric": round(velocity, 4),
            "subreddit": subreddit,
            "score": int(score),
            "age_hours": round(age_h, 2),
            "num_comments": comments,
            "listing_type": kind  # Track if it came from rising or hot
        })
    return out

def write_combined_csv(all_rows):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d_%H-%M")
    path = OUT_DIR / f"{ts}_REDDIT_COMBINED.csv"
    
    # Remove duplicates based on title and subreddit
    seen = set()
    unique_rows = []
    for row in all_rows:
        key = (row["title"].lower(), row["subreddit"].lower())
        if key not in seen:
            seen.add(key)
            unique_rows.append(row)
    
    # Sort by velocity (highest first)
    unique_rows.sort(key=lambda x: x["raw_metric"], reverse=True)
    
    cols = ["source","title","url","region","observed_at","raw_metric","subreddit","score","age_hours","num_comments","listing_type"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(unique_rows)
    
    print(f"Wrote {len(unique_rows)} unique rows → {path}")
    print(f"Removed {len(all_rows) - len(unique_rows)} duplicates")
    return path

def main():
    print("🚀 Starting enhanced Reddit scraper...")
    
    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit("❌ Reddit API credentials not found.")
    
    token = get_token()
    print("✅ Got Reddit API token")
    
    all_rows = []
    
    # Fetch rising posts (150 limit)
    print("📈 Fetching rising posts...")
    try:
        rising_data = fetch_listing(kind="rising", limit=150, token=token)
        rising_rows = normalize_items(rising_data, "rising")
        print(f"✅ Got {len(rising_rows)} rising posts")
        all_rows.extend(rising_rows)
    except Exception as e:
        print(f"❌ Error fetching rising posts: {e}")
    
    # Fetch hot posts (150 limit)
    print("🔥 Fetching hot posts...")
    try:
        hot_data = fetch_listing(kind="hot", limit=150, token=token)
        hot_rows = normalize_items(hot_data, "hot")
        print(f"✅ Got {len(hot_rows)} hot posts")
        all_rows.extend(hot_rows)
    except Exception as e:
        print(f"❌ Error fetching hot posts: {e}")
    
    if all_rows:
        # Write combined CSV
        csv_path = write_combined_csv(all_rows)
        print(f"🎉 Successfully created combined Reddit trends file: {csv_path}")
        
        # Show some stats
        rising_count = len([r for r in all_rows if r["listing_type"] == "rising"])
        hot_count = len([r for r in all_rows if r["listing_type"] == "hot"])
        print(f"📊 Summary: {rising_count} rising + {hot_count} hot = {len(all_rows)} total posts")
        
        # Show top 5 by velocity
        top_5 = sorted(all_rows, key=lambda x: x["raw_metric"], reverse=True)[:5]
        print("\n🏆 Top 5 by velocity:")
        for i, post in enumerate(top_5, 1):
            print(f"  {i}. {post['title'][:60]}... (r/{post['subreddit']}) - {post['raw_metric']:.1f} velocity")
    else:
        print("❌ No posts fetched")

if __name__ == "__main__":
    main()
