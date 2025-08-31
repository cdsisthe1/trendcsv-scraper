#!/usr/bin/env python3
import os
import sys
import json
import csv
import requests
import datetime
from datetime import timezone
import re
import time
import urllib.parse
from typing import List, Dict, Any
import argparse

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

UA = {"User-Agent": "TrendSite/1.0 (+contact@example.com)"}

DATE_RE = re.compile(r"^(January|February|March|April|May|June|July|August|September|October|November|December)_(\d{1,2})(,_\d{4})?$", re.I)
YEAR_RE = re.compile(r"^\d{4}$")
YEAR_IN_RE = re.compile(r"^\d{4}_in_", re.I)
LIST_RE = re.compile(r"^List_of_", re.I)
DEATHS_RE = re.compile(r"^Deaths_in_\d{4}$", re.I)

STOP_TITLES = {"Main_Page"}
STOP_PREFIXES = ("Special:",)               # hard-stop
STOP_NAMESPACES = ("Wikipedia:", "Help:", "Category:", "Talk:", "Portal:", "File:", "Template:", "User:", "Draft:", "Module:", "MediaWiki:", "TimedText:", "Book:", "Gadget:", "Topic:")

# Inappropriate content filter
INAPPROPRIATE_WORDS = {
    "xxx", ".xxx", "porn", "sex", "adult", "nsfw", "explicit", "mature"
}

def looks_like_noise(title: str) -> bool:
    if title in STOP_TITLES: return True
    if title.startswith(STOP_PREFIXES): return True
    if any(title.startswith(ns) for ns in STOP_NAMESPACES): return True
    if ":" in title: return True  # catches any odd namespace that slips through
    if DATE_RE.match(title): return True
    if YEAR_RE.match(title): return True
    if YEAR_IN_RE.match(title): return True
    if LIST_RE.match(title): return True
    if DEATHS_RE.match(title): return True
    
    # Check for inappropriate content
    title_lower = title.lower()
    if any(word in title_lower for word in INAPPROPRIATE_WORDS): return True
    
    return False

def wiki_summary(title: str, lang="en"):
    # Resolve redirects; classify type (standard/disambiguation/mainpage/redirect/…)
    url = f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{urllib.parse.quote(title)}?redirect=true"
    r = requests.get(url, headers=UA, timeout=20)
    if r.status_code != 200:
        return None
    j = r.json()
    return {
        "type": j.get("type"),
        "title": j.get("title"),
        "canonical": j.get("content_urls", {}).get("desktop", {}).get("page"),
        "description": j.get("description"),
        "thumbnail": j.get("thumbnail", {}).get("source"),
        "lang": lang,
    }

def fetch_wiki_top_real(lang="en", project="wikipedia", access="all-access", max_items=100, max_summaries=60, sleep_s=0.05):
    # 1) get top pageviews
    # Use yesterday's data since today's might not be available yet
    yesterday = datetime.datetime.now(timezone.utc) - datetime.timedelta(days=1)
    y, m, d = yesterday.strftime("%Y %m %d").split()
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{lang}.{project}/{access}/{y}/{m}/{d}"
    data = requests.get(url, headers=UA, timeout=30).json()

    items = []
    for day in data.get("items", []):
        for a in day.get("articles", []):
            raw_title = a["article"]
            views = a.get("views", 0)
            if looks_like_noise(raw_title):
                continue
            items.append({"title": raw_title, "views": views})

    # 2) verify each candidate with Summary API and keep only real articles
    real = []
    for i, it in enumerate(items[:max_items]):
        if i >= max_summaries: break  # cap API calls per run
        s = wiki_summary(it["title"], lang=lang)
        time.sleep(sleep_s) if sleep_s else None  # be polite
        if not s: 
            continue
        if s["type"] != "standard":  # skip disambiguation, mainpage, redirect, etc.
            continue
        real.append({
            "source": "wikipedia",
            "title": s["title"],
            "url": s["canonical"] or f"https://{lang}.wikipedia.org/wiki/{urllib.parse.quote(it['title'])}",
            "region": "GLOBAL",
            "raw_metric": it["views"],
            "description": s["description"],
            "thumbnail": s["thumbnail"],
            "lang": lang
        })

    # sort by views desc and return
    real.sort(key=lambda x: x["raw_metric"], reverse=True)
    return real

def extract_trending_topics(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extract trending topics from Wikipedia articles"""
    topics = []
    observed_at = datetime.datetime.now(timezone.utc).isoformat()
    
    for article in articles:
        # Calculate a score based on views (Wikipedia views are already real numbers)
        score = min(article["raw_metric"] / 1000, 1000)  # Normalize to 0-1000 scale
        
        topics.append({
            "source": article["source"],
            "title": article["title"],
            "slug": to_slug(article["title"]),
            "url": article["url"],
            "region": article["region"],
            "observed_at": observed_at,
            "raw_metric": str(article["raw_metric"]),
            "score": score,
            "description": article.get("description", ""),
            "thumbnail": article.get("thumbnail", ""),
            "lang": article.get("lang", "en")
        })
    
    return topics

def to_slug(text: str) -> str:
    """Convert text to URL-friendly slug"""
    return text.lower().replace(" ", "-").replace("_", "-")

def save_to_csv(topics: List[Dict[str, Any]], output_file: str):
    """Save trending topics to CSV file"""
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    fieldnames = [
        "source", "title", "slug", "url", "region", "observed_at", 
        "raw_metric", "score", "description", "thumbnail", "lang"
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(topics)

def main():
    parser = argparse.ArgumentParser(description='Scrape Wikipedia trending articles')
    parser.add_argument('--lang', default='en', help='Language code (default: en)')
    parser.add_argument('--max-items', type=int, default=50, help='Maximum items to fetch (default: 50)')
    parser.add_argument('--max-summaries', type=int, default=60, help='Maximum summaries to fetch (default: 60)')
    parser.add_argument('--output', default='trending/wikipedia_trends.csv', help='Output CSV file')
    args = parser.parse_args()

    try:
        print(f"🔍 Fetching Wikipedia trending articles (lang: {args.lang})...")
        
        # Fetch trending articles
        articles = fetch_wiki_top_real(
            lang=args.lang,
            max_items=args.max_items,
            max_summaries=args.max_summaries
        )
        
        if not articles:
            print("❌ No articles found")
            return
        
        print(f"✅ Found {len(articles)} trending articles")
        
        # Extract trending topics
        topics = extract_trending_topics(articles)
        
        # Save to CSV
        save_to_csv(topics, args.output)
        
        print(f"✅ Successfully saved {len(topics)} topics to {args.output}")
        print(f"📊 Top 5 Wikipedia trends:")
        for i, topic in enumerate(topics[:5], 1):
            print(f"  {i}. {topic['title']}: {topic['raw_metric']} views")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
