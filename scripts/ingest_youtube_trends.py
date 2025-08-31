#!/usr/bin/env python3
"""
YouTube Trends Scraper
Fetches trending videos from YouTube API and outputs to CSV format
"""

import os
import sys
import json
import csv
import requests
from datetime import datetime, timezone
from typing import List, Dict, Any
import argparse

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_youtube_api_key() -> str:
    """Get YouTube API key from environment"""
    # Try to get from environment first
    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        # Fallback to hardcoded key for now
        api_key = "AIzaSyC0jt6LfU8CmflySRZSfKax4PuQ5vte-7w"
    return api_key

def fetch_trending_videos(api_key: str, region_code: str = 'US', max_results: int = 50) -> List[Dict[str, Any]]:
    """
    Fetch trending videos from YouTube API
    
    Args:
        api_key: YouTube API key
        region_code: Country code (e.g., 'US', 'GB', 'CA')
        max_results: Maximum number of results to fetch
    
    Returns:
        List of trending video data
    """
    url = "https://www.googleapis.com/youtube/v3/videos"
    
    params = {
        'part': 'snippet,statistics',
        'chart': 'mostPopular',
        'regionCode': region_code,
        'maxResults': min(max_results, 50),  # YouTube API max is 50
        'key': api_key
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if 'items' not in data:
            print(f"Error: No items in response. Response: {data}")
            return []
        
        return data['items']
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching YouTube trends: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return []

def extract_trending_topics(videos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract trending topics from video titles and descriptions
    
    Args:
        videos: List of video data from YouTube API
    
    Returns:
        List of trending topics with metadata
    """
    topics = []
    now = datetime.now(timezone.utc).isoformat()
    
    for video in videos:
        try:
            snippet = video.get('snippet', {})
            statistics = video.get('statistics', {})
            
            # Extract title and clean it
            title = snippet.get('title', '').strip()
            if not title:
                continue
            
            # Extract view count
            view_count = int(statistics.get('viewCount', 0))
            
            # Generate a slug from title
            slug = title.lower()
            slug = ''.join(c for c in slug if c.isalnum() or c.isspace())
            slug = '-'.join(slug.split()[:5])  # First 5 words
            
            # Calculate a score based on views and recency
            # YouTube trending videos typically have 100K+ views
            score = min(view_count / 1000, 1000)  # Cap at 1000 for CSV compatibility
            
            topic = {
                'source': 'youtube',
                'title': title,
                'slug': slug,
                'url': f"https://www.youtube.com/watch?v={video.get('id')}",
                'region': 'US',  # We'll make this configurable later
                'observed_at': now,
                'raw_metric': view_count,
                'score': score,
                'channel': snippet.get('channelTitle', ''),
                'published_at': snippet.get('publishedAt', ''),
                'description': snippet.get('description', '')[:200]  # First 200 chars
            }
            
            topics.append(topic)
            
        except Exception as e:
            print(f"Error processing video {video.get('id', 'unknown')}: {e}")
            continue
    
    return topics

def save_to_csv(topics: List[Dict[str, Any]], output_file: str):
    """
    Save trending topics to CSV file
    
    Args:
        topics: List of trending topics
        output_file: Output CSV file path
    """
    if not topics:
        print("No topics to save")
        return
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Define CSV headers
    headers = [
        'source', 'title', 'slug', 'url', 'region', 'observed_at', 
        'raw_metric', 'score', 'channel', 'published_at', 'description'
    ]
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(topics)
        
        print(f"✅ Saved {len(topics)} YouTube trends to {output_file}")
        
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def main():
    parser = argparse.ArgumentParser(description='Scrape YouTube trending videos')
    parser.add_argument('--region', default='US', help='Region code (default: US)')
    parser.add_argument('--max-results', type=int, default=50, help='Maximum results (default: 50)')
    parser.add_argument('--output', default='trending/youtube_trends.csv', help='Output CSV file')
    
    args = parser.parse_args()
    
    try:
        # Get API key
        api_key = get_youtube_api_key()
        print(f"🔑 Using YouTube API key: {api_key[:10]}...")
        
        # Fetch trending videos
        print(f"📺 Fetching YouTube trends for region: {args.region}")
        videos = fetch_trending_videos(api_key, args.region, args.max_results)
        
        if not videos:
            print("❌ No videos fetched")
            sys.exit(1)
        
        print(f"📊 Found {len(videos)} trending videos")
        
        # Extract trending topics
        topics = extract_trending_topics(videos)
        print(f"🎯 Extracted {len(topics)} trending topics")
        
        # Save to CSV
        save_to_csv(topics, args.output)
        
        # Print some examples
        print("\n📋 Sample trending topics:")
        for i, topic in enumerate(topics[:5]):
            print(f"  {i+1}. {topic['title'][:50]}... (Views: {topic['raw_metric']:,})")
        
        print(f"\n✅ YouTube trends scraping completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
