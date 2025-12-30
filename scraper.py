"""
Yeniemlak.az Real Estate Scraper
Scrapes all listings with full details from yeniemlak.az
"""

import requests
from bs4 import BeautifulSoup
import csv
import json
import time
import re
from urllib.parse import urljoin
from datetime import datetime
import urllib3
import sys

# Fix Windows console encoding for Azerbaijani characters
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Disable SSL warnings for self-signed certificate
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://yeniemlak.az"
SEARCH_URL = "https://yeniemlak.az/elan/axtar"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'az,en;q=0.9',
}

def get_page(url, retries=3):
    """Fetch a page with retry logic"""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30, verify=False)
            response.raise_for_status()
            response.encoding = 'utf-8'
            return response.text
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(2)
    return None


def get_total_pages(soup):
    """Extract total number of pages from pagination"""
    pagination = soup.find_all('a', href=re.compile(r'page=\d+'))
    if pagination:
        page_numbers = []
        for link in pagination:
            match = re.search(r'page=(\d+)', link.get('href', ''))
            if match:
                page_numbers.append(int(match.group(1)))
        return max(page_numbers) if page_numbers else 1
    return 1


def extract_listing_urls(soup):
    """Extract all listing URLs from a search results page"""
    listing_urls = []

    # Find all listing links (they contain /elan/ and end with an ID)
    for link in soup.find_all('a', href=re.compile(r'/elan/[a-z0-9-]+-\d+$')):
        href = link.get('href')
        if href and 'axtar' not in href:
            full_url = urljoin(BASE_URL, href)
            if full_url not in listing_urls:
                listing_urls.append(full_url)

    return listing_urls


def extract_listing_details(url):
    """Extract all details from a single listing page"""
    html = get_page(url)
    if not html:
        return None

    soup = BeautifulSoup(html, 'html.parser')
    listing = {'url': url}

    # Extract listing ID from URL
    id_match = re.search(r'-(\d+)$', url)
    listing['id'] = id_match.group(1) if id_match else None

    # Find the main view table
    view_table = soup.find('table', class_='view')
    if not view_table:
        print(f"Could not find view table for {url}")
        return None

    # Sale type (Satılır, Kirayə, Günlük)
    tip = view_table.find('tip')
    listing['sale_type'] = tip.get_text(strip=True) if tip else None

    # Price
    price = view_table.find('price')
    listing['price'] = price.get_text(strip=True) if price else None

    # Views count
    views_elem = view_table.find('g')
    if views_elem:
        b_tag = views_elem.find('b')
        listing['views'] = b_tag.get_text(strip=True) if b_tag else None

    # Date
    date_items = view_table.find_all('titem')
    for item in date_items:
        text = item.get_text()
        if 'Tarix' in text:
            b_tag = item.find('b')
            listing['date'] = b_tag.get_text(strip=True) if b_tag else None
        elif 'Elan' in text:
            b_tag = item.find('b')
            listing['elan_id'] = b_tag.get_text(strip=True) if b_tag else None

    # Property type (Həyət evi / Villa, Mənzil, etc.)
    emlak = view_table.find('emlak')
    listing['property_type'] = emlak.get_text(strip=True) if emlak else None

    # Extract params (rooms, area, land area, floors)
    params = view_table.find_all('div', class_='params')
    params_data = []
    for param in params:
        b_tag = param.find('b')
        if b_tag:
            params_data.append(b_tag.get_text(strip=True))

    # Parse common params
    param_texts = [p.get_text(strip=True) for p in params]
    for i, text in enumerate(param_texts):
        if 'otaq' in text.lower():
            listing['rooms'] = params_data[i] if i < len(params_data) else None
        elif 'm2' in text or 'm²' in text:
            listing['area_m2'] = params_data[i] if i < len(params_data) else None
        elif 'sot' in text.lower():
            listing['land_area_sot'] = params_data[i] if i < len(params_data) else None
        elif 'mərtəbəli' in text.lower() or 'mertebeli' in text.lower():
            listing['floors'] = params_data[i] if i < len(params_data) else None

    # If params weren't matched by keyword, try positional assignment for house listings
    if 'rooms' not in listing and len(params_data) >= 1:
        listing['rooms'] = params_data[0]
    if 'area_m2' not in listing and len(params_data) >= 2:
        listing['area_m2'] = params_data[1]
    if 'land_area_sot' not in listing and len(params_data) >= 3:
        listing['land_area_sot'] = params_data[2]
    if 'floors' not in listing and len(params_data) >= 4:
        listing['floors'] = params_data[3]

    # Description
    text_divs = view_table.find_all('div', class_='text')
    descriptions = []
    for div in text_divs:
        text = div.get_text(strip=True)
        if text and len(text) > 20:  # Filter out short address texts
            descriptions.append(text)
    listing['description'] = ' '.join(descriptions) if descriptions else None

    # Features (checkboxes like Təmirli, Qaz, Su, etc.)
    features = []
    check_divs = view_table.find_all('div', class_='check')
    for check in check_divs:
        features.append(check.get_text(strip=True))
    listing['features'] = features

    # Address - find region and specific location
    # Look for params after "Ünvan" header
    address_parts = []
    h1_tags = view_table.find_all('h1')
    for h1 in h1_tags:
        if 'Ünvan' in h1.get_text():
            # Get following siblings for address info
            for sibling in h1.find_next_siblings():
                if sibling.name == 'h1':
                    break
                if sibling.name == 'div':
                    if 'params' in sibling.get('class', []):
                        b_tag = sibling.find('b')
                        if b_tag:
                            address_parts.append(b_tag.get_text(strip=True))
                    elif 'text' in sibling.get('class', []):
                        text = sibling.get_text(strip=True)
                        if text:
                            address_parts.append(text)

    listing['region'] = address_parts[0] if len(address_parts) > 0 else None
    listing['address'] = ', '.join(address_parts[1:]) if len(address_parts) > 1 else None

    # Contact info
    ad_div = view_table.find('div', class_='ad')
    listing['contact_name'] = ad_div.get_text(strip=True) if ad_div else None

    elvrn_div = view_table.find('div', class_='elvrn')
    listing['contact_type'] = elvrn_div.get_text(strip=True) if elvrn_div else None

    # Phone (usually in image, extract image URL)
    tel_div = view_table.find('div', class_='tel')
    if tel_div:
        tel_img = tel_div.find('img')
        if tel_img and tel_img.get('src'):
            # The phone is in an image at /tel-show/XXXXXXXXXX
            phone_src = tel_img.get('src')
            phone_match = re.search(r'/tel-show/(\d+)', phone_src)
            listing['phone'] = phone_match.group(1) if phone_match else phone_src

    # Images
    images = []
    img_divs = view_table.find_all('div', class_='img_div')
    for img_div in img_divs:
        link = img_div.find('a')
        if link and link.get('href'):
            images.append(link.get('href'))

    # Also get main image
    main_img = view_table.find('img', class_='imgb')
    if main_img and main_img.get('src'):
        main_img_url = urljoin(BASE_URL, main_img.get('src'))
        if main_img_url not in images:
            images.insert(0, main_img_url)

    listing['images'] = images
    listing['image_count'] = len(images)

    return listing


def scrape_all_listings(start_page=1, end_page=None, delay=1):
    """
    Scrape all listings from yeniemlak.az

    Args:
        start_page: Page to start from (default 1)
        end_page: Page to end at (default None = all pages)
        delay: Delay between requests in seconds
    """
    all_listings = []

    # Get first page to determine total pages
    first_page_url = f"{SEARCH_URL}?page={start_page}"
    print(f"Fetching first page: {first_page_url}")

    html = get_page(first_page_url)
    if not html:
        print("Failed to fetch first page")
        return []

    soup = BeautifulSoup(html, 'html.parser')
    total_pages = get_total_pages(soup)

    if end_page:
        total_pages = min(total_pages, end_page)

    print(f"Total pages to scrape: {total_pages}")

    # Process each page
    for page_num in range(start_page, total_pages + 1):
        print(f"\n--- Processing page {page_num}/{total_pages} ---")

        if page_num > start_page:
            page_url = f"{SEARCH_URL}?page={page_num}"
            html = get_page(page_url)
            if not html:
                print(f"Failed to fetch page {page_num}")
                continue
            soup = BeautifulSoup(html, 'html.parser')

        # Get listing URLs from this page
        listing_urls = extract_listing_urls(soup)
        print(f"Found {len(listing_urls)} listings on page {page_num}")

        # Process each listing
        for i, listing_url in enumerate(listing_urls, 1):
            print(f"  Processing listing {i}/{len(listing_urls)}: {listing_url}")

            listing = extract_listing_details(listing_url)
            if listing:
                all_listings.append(listing)
                print(f"    -> {listing.get('property_type')} | {listing.get('price')} AZN | {listing.get('region')}")

            time.sleep(delay)

        time.sleep(delay)

    return all_listings


def save_to_csv(listings, filename='listings.csv'):
    """Save listings to CSV file"""
    if not listings:
        print("No listings to save")
        return

    # Define column order
    columns = [
        'id', 'elan_id', 'url', 'sale_type', 'property_type', 'price',
        'rooms', 'area_m2', 'land_area_sot', 'floors',
        'region', 'address', 'description', 'features',
        'contact_name', 'contact_type', 'phone',
        'views', 'date', 'image_count', 'images'
    ]

    with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()

        for listing in listings:
            # Convert lists to strings for CSV
            row = listing.copy()
            if 'features' in row and isinstance(row['features'], list):
                row['features'] = ', '.join(row['features'])
            if 'images' in row and isinstance(row['images'], list):
                row['images'] = ', '.join(row['images'])
            writer.writerow(row)

    print(f"Saved {len(listings)} listings to {filename}")


def save_to_json(listings, filename='listings.json'):
    """Save listings to JSON file"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(listings, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(listings)} listings to {filename}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Scrape yeniemlak.az listings')
    parser.add_argument('--start-page', type=int, default=1, help='Start page (default: 1)')
    parser.add_argument('--end-page', type=int, default=None, help='End page (default: all)')
    parser.add_argument('--delay', type=float, default=1, help='Delay between requests in seconds (default: 1)')
    parser.add_argument('--output', type=str, default='listings', help='Output filename without extension')

    args = parser.parse_args()

    print(f"Starting scraper at {datetime.now()}")
    print(f"Pages: {args.start_page} to {args.end_page or 'end'}")
    print(f"Delay: {args.delay}s")

    listings = scrape_all_listings(
        start_page=args.start_page,
        end_page=args.end_page,
        delay=args.delay
    )

    print(f"\n{'='*50}")
    print(f"Scraping completed! Total listings: {len(listings)}")

    if listings:
        save_to_csv(listings, f"{args.output}.csv")
        save_to_json(listings, f"{args.output}.json")

    print(f"Finished at {datetime.now()}")
