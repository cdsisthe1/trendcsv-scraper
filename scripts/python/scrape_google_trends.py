#!/usr/bin/env python3
import time
import os
import csv
import re
import argparse
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException

# Configuration
TIMEOUT = 30
MIN_SEARCH_VOLUME = 5000  # Minimum search volume threshold

def get_trends_url(region):
    """Get the appropriate Google Trends URL for the region"""
    if region.upper() == 'US':
        return "https://trends.google.com/trending?geo=US"
    elif region.upper() == 'GLOBAL':
        return "https://trends.google.com/trending?geo="
    else:
        return f"https://trends.google.com/trending?geo={region}"

def parse_search_volume(volume_str):
    """Parse search volume string to integer"""
    if not volume_str or volume_str.strip() == '':
        return 0
    
    volume_str = volume_str.strip().lower()
    
    # Handle "1M+", "500K+", etc.
    if 'm' in volume_str:
        # Extract number before M and multiply by 1,000,000
        number = re.search(r'(\d+(?:\.\d+)?)', volume_str)
        if number:
            return int(float(number.group(1)) * 1000000)
    
    # Handle "500K+", "20K+", etc.
    elif 'k' in volume_str:
        # Extract number before K and multiply by 1,000
        number = re.search(r'(\d+(?:\.\d+)?)', volume_str)
        if number:
            return int(float(number.group(1)) * 1000)
    
    # Handle plain numbers
    else:
        number = re.search(r'(\d+)', volume_str)
        if number:
            return int(number.group(1))
    
    return 0

def setup_driver():
    """Setup Chrome WebDriver with download preferences"""
    chrome_options = Options()
    
    # Get current working directory for downloads
    download_dir = os.getcwd()
    
    # Set download preferences
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Add other options
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--headless")  # Run headless
    
    # Create driver
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def download_trends_csv(driver, region):
    """Navigate to Google Trends and download CSV"""
    try:
        trends_url = get_trends_url(region)
        print(f"Navigating to: {trends_url}")
        driver.get(trends_url)
        
        # Wait for page to load
        print("Waiting for page to load...")
        time.sleep(5)
        
        # Look for the export button
        print("Looking for export button...")
        wait = WebDriverWait(driver, TIMEOUT)
        
        # Find the export button directly
        export_button = None
        
        # Method 1: Find by XPath with text content
        try:
            export_button = driver.find_element(By.XPATH, "//span[contains(text(), 'Export') and @jsname='V67aGc']")
            print("Found export button using XPath")
        except:
            pass
        
        # Method 2: Find by CSS selector and check text
        if not export_button:
            try:
                spans = driver.find_elements(By.CSS_SELECTOR, "span[jsname='V67aGc']")
                for span in spans:
                    if "Export" in span.text:
                        export_button = span
                        print(f"Found export button with text: {span.text}")
                        break
            except:
                pass
        
        # Method 3: Find by class and check text
        if not export_button:
            try:
                spans = driver.find_elements(By.CSS_SELECTOR, "span.FOBRw-vQzf8d")
                for span in spans:
                    if "Export" in span.text:
                        export_button = span
                        print(f"Found export button with text: {span.text}")
                        break
            except:
                pass
        
        if not export_button:
            print("Could not find export button. Let me show you what elements are available:")
            spans = driver.find_elements(By.CSS_SELECTOR, "span[jsname='V67aGc']")
            for i, span in enumerate(spans[:10]):  # Show first 10 spans
                print(f"Span {i+1}: '{span.text}' - class: '{span.get_attribute('class')}'")
            return None
        
        # Click the export button
        print("Clicking export button...")
        driver.execute_script("arguments[0].click();", export_button)
        time.sleep(2)
        
        # Look for CSV download option
        print("Looking for CSV download option...")
        
        # Wait a bit for the dropdown to appear
        time.sleep(2)
        
        csv_button = None
        
        # Method 1: Look for "Download CSV" text
        try:
            csv_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Download CSV')]")
            for element in csv_elements:
                if element.is_displayed() and element.is_enabled():
                    csv_button = element
                    print(f"Found CSV download element: {element.text}")
                    break
        except:
            pass
        
        # Method 2: Look for any element containing "CSV"
        if not csv_button:
            try:
                csv_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'CSV')]")
                for element in csv_elements:
                    if element.is_displayed() and element.is_enabled():
                        csv_button = element
                        print(f"Found CSV element: {element.text}")
                        break
            except:
                pass
        
        # Method 3: Look for menu items or list items
        if not csv_button:
            try:
                menu_items = driver.find_elements(By.CSS_SELECTOR, "[role='menuitem'], li, div[tabindex]")
                for item in menu_items:
                    if "csv" in item.text.lower() and item.is_displayed() and item.is_enabled():
                        csv_button = item
                        print(f"Found CSV menu item: {item.text}")
                        break
            except:
                pass
        
        if csv_button:
            print("Clicking CSV download button...")
            driver.execute_script("arguments[0].click();", csv_button)
            
            # Wait for download to complete
            print("Waiting for download to complete...")
            time.sleep(10)
            
            # Check if file was downloaded
            # Look for the most recently created CSV file (the newly downloaded one)
            csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
            if csv_files:
                # Sort files by creation time (newest first)
                csv_files.sort(key=lambda x: os.path.getctime(x), reverse=True)
                downloaded_file = csv_files[0]
                print(f"Most recent CSV file: {downloaded_file}")
                return downloaded_file
            else:
                print("No CSV file found in download directory")
                return None
        else:
            print("Could not find CSV download option")
            return None
            
    except Exception as e:
        print(f"Error: {e}")
        return None

def process_csv_to_schema(input_file, region):
    """Process the downloaded CSV and convert to our schema"""
    try:
        if not os.path.exists(input_file):
            print(f"Error: {input_file} not found!")
            return None
        
        print(f"Processing CSV for region: {region}")
        
        # Read the CSV file
        trends = []
        with open(input_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Parse search volume
                volume_str = row.get('Search volume', '0')
                search_volume = parse_search_volume(volume_str)
                
                # Only keep trends with sufficient search volume
                if search_volume >= MIN_SEARCH_VOLUME:
                    trend_name = row.get('Trends', 'Unknown')
                    trends.append({
                        'source': 'google_trends',
                        'title': trend_name,
                        'url': f"https://www.google.com/search?q={trend_name.replace(' ', '+')}",
                        'region': region,
                        'observed_at': datetime.now().isoformat(),
                        'raw_metric': 1  # presence
                    })
        
        print(f"Found {len(trends)} trends with search volume >= {MIN_SEARCH_VOLUME:,}")
        
        if not trends:
            print("No trends meet the minimum search volume threshold!")
            return None
        
        return trends
        
    except Exception as e:
        print(f"Error processing CSV: {e}")
        return None

def save_to_csv(trends, region):
    """Save trends to CSV with timestamped filename"""
    if not trends:
        return None
    
    # Create timestamp for filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    filename = f"trendingcsv/google_trends/{timestamp}_{region}.csv"
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Write CSV
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        fieldnames = ['source', 'title', 'url', 'region', 'observed_at', 'raw_metric']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        
        for trend in trends:
            writer.writerow(trend)
    
    print(f"Saved {len(trends)} trends to {filename}")
    return filename

def main():
    parser = argparse.ArgumentParser(description='Scrape Google Trends and save to CSV')
    parser.add_argument('--region', choices=['US', 'GLOBAL'], default='US', 
                       help='Region to scrape (default: US)')
    
    args = parser.parse_args()
    
    print(f"Google Trends CSV Scraper - Region: {args.region}")
    print("=" * 50)
    
    driver = None
    try:
        # Setup driver
        print("Setting up Chrome WebDriver...")
        driver = setup_driver()
        
        # Download CSV
        downloaded_file = download_trends_csv(driver, args.region)
        
        if downloaded_file:
            print(f"\n[SUCCESS] Download completed successfully!")
            
            # Process CSV to our schema
            print("\n" + "=" * 30)
            print("Processing CSV to Schema")
            print("=" * 30)
            
            trends = process_csv_to_schema(downloaded_file, args.region)
            
            if trends:
                # Save to our schema format
                output_file = save_to_csv(trends, args.region)
                
                if output_file:
                    print(f"\n[SUCCESS] Processing completed successfully!")
                    print(f"Output file: {output_file}")
                    print(f"Trends processed: {len(trends)}")
                    
                    # Clean up downloaded file
                    try:
                        os.remove(downloaded_file)
                        print(f"Cleaned up temporary file: {downloaded_file}")
                    except:
                        pass
                else:
                    print("\n❌ Failed to save processed trends!")
            else:
                print("\n❌ Failed to process CSV!")
        else:
            print("\n[ERROR] Download failed. Check the console for details.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if driver:
            print("Closing browser...")
            driver.quit()

if __name__ == "__main__":
    main()
