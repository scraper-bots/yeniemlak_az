"""
Yeniemlak.az Real Estate Scraper
Async scraper with crash-proof features: checkpointing, auto-resume, progress saving
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
import csv
import json
import re
from urllib.parse import urljoin
from datetime import datetime
import ssl
import sys
import os
import signal
import logging

# Setup logging to both file and console
LOG_FILE = "scraper.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8', mode='a'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# Fix Windows console encoding for Azerbaijani characters
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except AttributeError:
        pass

BASE_URL = "https://yeniemlak.az"
# Full search URL with all required parameters for proper pagination
SEARCH_URL = "https://yeniemlak.az/elan/axtar?elan_nov=&emlak=&metro%5B%5D=0&menzil_nov=&mertebe_sayi=&mertebe_sayi2=&mertebe=&mertebe2=&otaq=0&otaq2=0&sahe_m=&sahe_m2=&sahe_s=&sahe_s2=&qiymet=&qiymet2=&sened="

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'az,en;q=0.9',
}

# Max concurrent requests (keep low to avoid rate limiting)
MAX_CONCURRENT_REQUESTS = 2
# Delay between requests in seconds
REQUEST_DELAY = 1.0
# Save checkpoint every N listings
CHECKPOINT_INTERVAL = 25
# Checkpoint files
CHECKPOINT_FILE = "checkpoint.json"
PROGRESS_FILE = "listings_progress.json"

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully"""
    global shutdown_requested
    log.info("\n\nShutdown requested. Saving progress...")
    shutdown_requested = True


# Register signal handler
signal.signal(signal.SIGINT, signal_handler)
if sys.platform != 'win32':
    signal.signal(signal.SIGTERM, signal_handler)


def load_checkpoint():
    """Load checkpoint from file if exists"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                log.info(f"Loaded checkpoint: {checkpoint.get('completed_listings', 0)} listings completed")
                return checkpoint
        except Exception as e:
            log.info(f"Error loading checkpoint: {e}")
    return {
        'completed_urls': [],
        'pending_urls': [],
        'last_page': 0,
        'total_pages': 0,
        'completed_listings': 0,
        'phase': 'collecting'  # 'collecting' or 'scraping'
    }


def save_checkpoint(checkpoint):
    """Save checkpoint to file"""
    try:
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False)
    except Exception as e:
        log.info(f"Error saving checkpoint: {e}")


def load_progress():
    """Load scraped listings from progress file"""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                listings = json.load(f)
                log.info(f"Loaded {len(listings)} listings from progress file")
                return listings
        except Exception as e:
            log.info(f"Error loading progress: {e}")
    return []


def save_progress(listings):
    """Save scraped listings to progress file"""
    try:
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(listings, f, ensure_ascii=False)
    except Exception as e:
        log.info(f"Error saving progress: {e}")


async def get_page(session, url, retries=5):
    """Fetch a page with retry logic"""
    for attempt in range(retries):
        if shutdown_requested:
            return None
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 429:
                    # Rate limited - wait longer
                    wait_time = 10 * (attempt + 1)
                    log.info(f"Rate limited, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    log.info(f"Status {response.status} for {url}")
        except asyncio.TimeoutError:
            log.info(f"Timeout for {url}, retrying...")
            await asyncio.sleep(5)
        except Exception as e:
            log.info(f"Attempt {attempt + 1} failed for {url}: {type(e).__name__}")
            if attempt < retries - 1:
                await asyncio.sleep(3)
    return None


def get_total_pages(soup, default_max=1485):
    """Extract total number of pages from pagination"""
    pagination = soup.find_all('a', href=re.compile(r'page=\d+'))
    page_numbers = []

    if pagination:
        for link in pagination:
            href = link.get('href', '')
            match = re.search(r'page=(\d+)', href)
            if match:
                page_numbers.append(int(match.group(1)))

    if page_numbers:
        return max(page_numbers)

    return default_max


def extract_listing_urls(soup):
    """Extract all listing URLs from a search results page"""
    listing_urls = []

    for link in soup.find_all('a', href=re.compile(r'/elan/[a-z0-9-]+-\d+$')):
        href = link.get('href')
        if href and 'axtar' not in href:
            full_url = urljoin(BASE_URL, href)
            if full_url not in listing_urls:
                listing_urls.append(full_url)

    return listing_urls


def extract_listing_details(html, url):
    """Extract all details from a single listing page"""
    if not html:
        return None

    try:
        soup = BeautifulSoup(html, 'html.parser')
        listing = {'url': url}

        # Extract listing ID from URL
        id_match = re.search(r'-(\d+)$', url)
        listing['id'] = id_match.group(1) if id_match else None

        # Find the main view table
        view_table = soup.find('table', class_='view')
        if not view_table:
            return None

        # Sale type
        tip = view_table.find('tip')
        listing['sale_type'] = tip.get_text(strip=True) if tip else None

        # Price
        price = view_table.find('price')
        listing['price'] = price.get_text(strip=True) if price else None

        # Views count and date
        date_items = view_table.find_all('titem')
        for item in date_items:
            text = item.get_text()
            g_tag = item.find('g')
            if g_tag:
                b_tag = g_tag.find('b')
                if 'Baxis' in text:
                    listing['views'] = b_tag.get_text(strip=True) if b_tag else None
            if 'Tarix' in text:
                b_tag = item.find('b')
                listing['date'] = b_tag.get_text(strip=True) if b_tag else None
            elif 'Elan' in text:
                b_tag = item.find('b')
                listing['elan_id'] = b_tag.get_text(strip=True) if b_tag else None

        # Property type
        emlak = view_table.find('emlak')
        listing['property_type'] = emlak.get_text(strip=True) if emlak else None

        # Extract params
        box_div = view_table.find('div', class_='box')
        if box_div:
            params = box_div.find_all('div', class_='params')
            params_data = []
            param_texts = []
            for param in params:
                b_tag = param.find('b')
                if b_tag:
                    params_data.append(b_tag.get_text(strip=True))
                param_texts.append(param.get_text(strip=True))

            for i, text in enumerate(param_texts):
                if 'otaq' in text.lower():
                    listing['rooms'] = params_data[i] if i < len(params_data) else None
                elif 'm2' in text:
                    listing['area_m2'] = params_data[i] if i < len(params_data) else None
                elif 'sot' in text.lower():
                    listing['land_area_sot'] = params_data[i] if i < len(params_data) else None
                elif 'mertebeli' in text.lower():
                    listing['floors'] = params_data[i] if i < len(params_data) else None

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
            if text and len(text) > 20:
                descriptions.append(text)
        listing['description'] = ' '.join(descriptions) if descriptions else None

        # Features
        features = []
        check_divs = view_table.find_all('div', class_='check')
        for check in check_divs:
            features.append(check.get_text(strip=True))
        listing['features'] = features

        # Address
        address_parts = []
        h1_tags = view_table.find_all('h1')
        for h1 in h1_tags:
            if 'Unvan' in h1.get_text() or 'nvan' in h1.get_text():
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

        # Phone
        tel_div = view_table.find('div', class_='tel')
        if tel_div:
            tel_img = tel_div.find('img')
            if tel_img and tel_img.get('src'):
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

        main_img = view_table.find('img', class_='imgb')
        if main_img and main_img.get('src'):
            main_img_url = urljoin(BASE_URL, main_img.get('src'))
            if main_img_url not in images:
                images.insert(0, main_img_url)

        listing['images'] = images
        listing['image_count'] = len(images)

        return listing

    except Exception as e:
        log.info(f"Error parsing {url}: {e}")
        return None


async def fetch_listing(session, url, semaphore):
    """Fetch and parse a single listing with semaphore control"""
    async with semaphore:
        await asyncio.sleep(REQUEST_DELAY)
        html = await get_page(session, url)
        if html:
            return extract_listing_details(html, url)
        return None


async def fetch_page_listings(session, page_url):
    """Fetch all listing URLs from a single search page"""
    html = await get_page(session, page_url)
    if not html:
        return []
    soup = BeautifulSoup(html, 'html.parser')
    return extract_listing_urls(soup)


async def scrape_all_listings(start_page=1, end_page=None, max_concurrent=MAX_CONCURRENT_REQUESTS):
    """
    Scrape all listings with crash-proof features
    """
    global shutdown_requested

    # Load checkpoint and progress
    checkpoint = load_checkpoint()
    all_listings = load_progress()
    completed_urls = set(checkpoint.get('completed_urls', []))

    # Create SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=max_concurrent)

    async with aiohttp.ClientSession(connector=connector) as session:
        # Phase 1: Collect listing URLs
        if checkpoint.get('phase') == 'collecting' or not checkpoint.get('pending_urls'):
            log.info("\n" + "="*50)
            log.info("PHASE 1: Collecting listing URLs")
            log.info("="*50)

            # Get first page to determine total pages
            first_page_url = f"{SEARCH_URL}&page={start_page}"
            log.info(f"Fetching first page...")

            html = await get_page(session, first_page_url)
            if not html:
                log.info("Failed to fetch first page")
                return all_listings

            soup = BeautifulSoup(html, 'html.parser')
            total_pages = get_total_pages(soup)

            if end_page:
                total_pages = min(total_pages, end_page)

            checkpoint['total_pages'] = total_pages
            log.info(f"Total pages to scrape: {total_pages}")

            all_listing_urls = []

            # Resume from last page if available
            resume_page = max(checkpoint.get('last_page', 0), start_page)
            if resume_page > start_page:
                log.info(f"Resuming from page {resume_page}")

            # Fetch first page URLs if starting fresh
            if resume_page == start_page:
                urls_from_first = extract_listing_urls(soup)
                all_listing_urls.extend(urls_from_first)
                log.info(f"  Page 1: {len(urls_from_first)} listings")
                checkpoint['last_page'] = 1
                save_checkpoint(checkpoint)

            # Fetch remaining pages
            for page_num in range(max(resume_page, start_page + 1), total_pages + 1):
                if shutdown_requested:
                    log.info("Shutdown requested, saving progress...")
                    break

                await asyncio.sleep(REQUEST_DELAY)
                page_url = f"{SEARCH_URL}&page={page_num}"
                urls = await fetch_page_listings(session, page_url)

                if urls:
                    all_listing_urls.extend(urls)
                    log.info(f"  Page {page_num}/{total_pages}: {len(urls)} listings (total: {len(all_listing_urls)})")
                else:
                    log.info(f"  Page {page_num}/{total_pages}: 0 listings")

                checkpoint['last_page'] = page_num
                if page_num % 10 == 0:
                    save_checkpoint(checkpoint)

            # Remove duplicates and already completed URLs
            unique_urls = []
            seen = set()
            for url in all_listing_urls:
                if url not in seen and url not in completed_urls:
                    seen.add(url)
                    unique_urls.append(url)

            checkpoint['pending_urls'] = unique_urls
            checkpoint['phase'] = 'scraping'
            save_checkpoint(checkpoint)

            log.info(f"\nTotal unique listings to scrape: {len(unique_urls)}")
            if completed_urls:
                log.info(f"Already completed: {len(completed_urls)}")

        else:
            unique_urls = [url for url in checkpoint.get('pending_urls', []) if url not in completed_urls]
            log.info(f"\nResuming scraping: {len(unique_urls)} listings remaining")

        if shutdown_requested:
            save_progress(all_listings)
            save_checkpoint(checkpoint)
            return all_listings

        # Phase 2: Scrape listing details
        log.info("\n" + "="*50)
        log.info("PHASE 2: Scraping listing details")
        log.info("="*50)

        semaphore = asyncio.Semaphore(max_concurrent)
        total = len(unique_urls)
        scraped_count = 0
        failed_urls = []

        for i, url in enumerate(unique_urls):
            if shutdown_requested:
                log.info("\nShutdown requested, saving progress...")
                break

            try:
                listing = await fetch_listing(session, url, semaphore)

                if listing:
                    all_listings.append(listing)
                    completed_urls.add(url)
                    scraped_count += 1
                else:
                    failed_urls.append(url)

                # Progress update
                if (i + 1) % 10 == 0 or i == total - 1:
                    log.info(f"  Progress: {i + 1}/{total} ({scraped_count} successful, {len(failed_urls)} failed)")

                # Save checkpoint periodically
                if (i + 1) % CHECKPOINT_INTERVAL == 0:
                    checkpoint['completed_urls'] = list(completed_urls)
                    checkpoint['pending_urls'] = unique_urls[i + 1:]
                    checkpoint['completed_listings'] = len(all_listings)
                    save_checkpoint(checkpoint)
                    save_progress(all_listings)
                    log.info(f"    [Checkpoint saved: {len(all_listings)} listings]")

            except Exception as e:
                log.info(f"  Error scraping {url}: {e}")
                failed_urls.append(url)

        # Retry failed URLs once
        if failed_urls and not shutdown_requested:
            log.info(f"\nRetrying {len(failed_urls)} failed URLs...")
            for url in failed_urls[:]:
                if shutdown_requested:
                    break
                try:
                    await asyncio.sleep(2)
                    listing = await fetch_listing(session, url, semaphore)
                    if listing:
                        all_listings.append(listing)
                        completed_urls.add(url)
                        failed_urls.remove(url)
                except Exception:
                    pass

        # Final save
        checkpoint['completed_urls'] = list(completed_urls)
        checkpoint['pending_urls'] = [u for u in unique_urls if u not in completed_urls]
        checkpoint['completed_listings'] = len(all_listings)
        save_checkpoint(checkpoint)
        save_progress(all_listings)

        return all_listings


def save_to_csv(listings, filename='listings.csv'):
    """Save listings to CSV file"""
    if not listings:
        log.info("No listings to save")
        return

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
            row = listing.copy()
            if 'features' in row and isinstance(row['features'], list):
                row['features'] = ', '.join(row['features'])
            if 'images' in row and isinstance(row['images'], list):
                row['images'] = ', '.join(row['images'])
            writer.writerow(row)

    log.info(f"Saved {len(listings)} listings to {filename}")


def save_to_json(listings, filename='listings.json'):
    """Save listings to JSON file"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(listings, f, ensure_ascii=False, indent=2)
    log.info(f"Saved {len(listings)} listings to {filename}")


def cleanup_checkpoint():
    """Remove checkpoint files after successful completion"""
    for f in [CHECKPOINT_FILE, PROGRESS_FILE]:
        if os.path.exists(f):
            os.remove(f)
            log.info(f"Removed {f}")


async def main():
    import argparse

    parser = argparse.ArgumentParser(description='Scrape yeniemlak.az listings (crash-proof)')
    parser.add_argument('--start-page', type=int, default=1, help='Start page (default: 1)')
    parser.add_argument('--end-page', type=int, default=None, help='End page (default: all)')
    parser.add_argument('--concurrent', type=int, default=MAX_CONCURRENT_REQUESTS,
                        help=f'Max concurrent requests (default: {MAX_CONCURRENT_REQUESTS})')
    parser.add_argument('--output', type=str, default='listings', help='Output filename without extension')
    parser.add_argument('--clean', action='store_true', help='Start fresh (remove checkpoint)')

    args = parser.parse_args()

    if args.clean:
        cleanup_checkpoint()
        log.info("Starting fresh scrape...")

    log.info(f"\n{'='*50}")
    log.info(f"YENIEMLAK.AZ SCRAPER")
    log.info(f"{'='*50}")
    log.info(f"Started at: {datetime.now()}")
    log.info(f"Pages: {args.start_page} to {args.end_page or 'all'}")
    log.info(f"Max concurrent requests: {args.concurrent}")
    log.info(f"Checkpoint interval: every {CHECKPOINT_INTERVAL} listings")
    log.info(f"Press Ctrl+C to stop and save progress")
    log.info(f"{'='*50}")

    listings = await scrape_all_listings(
        start_page=args.start_page,
        end_page=args.end_page,
        max_concurrent=args.concurrent
    )

    log.info(f"\n{'='*50}")
    log.info(f"SCRAPING COMPLETED")
    log.info(f"{'='*50}")
    log.info(f"Total listings scraped: {len(listings)}")

    if listings:
        save_to_csv(listings, f"{args.output}.csv")
        save_to_json(listings, f"{args.output}.json")

        if not shutdown_requested:
            # Only cleanup if completed successfully without interruption
            cleanup_checkpoint()

    log.info(f"Finished at: {datetime.now()}")


if __name__ == '__main__':
    # Windows event loop policy fix
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
