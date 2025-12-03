import json
import logging
import os
import random
import re
import signal
import sys
import time
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

BRANDS = [
    "Audi",
    "BMW",
    "Mercedes-Benz",
    "Volkswagen",
    "Skoda",
    "Seat",
    "Opel",
    "Peugeot",
    "Citroen",
    "Renault",
    "Dacia",
    "Fiat",
    "Alfa Romeo",
    "Volvo",
    "Porsche",
    "Mini",
    "Land Rover",
    "Jaguar",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_5_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.88 Safari/537.36",
]

LOG_FILE = "car_scraper_log.txt"
OUTPUT_FILE = "car_catalog.json"
RETRIES = 5
TIMEOUT = 20
SLEEP_RANGE = (1.0, 2.0)

logger = logging.getLogger("car_scraper")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

stop_requested = False

def handle_stop(signum, frame):
    global stop_requested
    stop_requested = True
    logger.info("Received stop signal, preparing to exit safely.")


def slugify_brand(brand: str) -> str:
    return brand.lower().replace(" ", "-")


def load_catalog() -> Dict[str, List[Dict[str, object]]]:
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Existing catalog file is corrupted; starting fresh.")
                return {}
    return {}


def save_catalog(catalog: Dict[str, List[Dict[str, object]]]):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)


def get_known_ids(catalog: Dict[str, List[Dict[str, object]]]) -> Dict[str, set]:
    known: Dict[str, set] = {}
    for brand, listings in catalog.items():
        known[brand] = {entry.get("id") for entry in listings if entry.get("id")}
    return known


def fetch_url(url: str) -> Optional[str]:
    for attempt in range(1, RETRIES + 1):
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        logger.info("URL being fetched: %s (attempt %s)", url, attempt)
        try:
            response = requests.get(url, headers=headers, timeout=TIMEOUT)
            logger.info("Status code: %s", response.status_code)
            if response.status_code == 200:
                time.sleep(random.uniform(*SLEEP_RANGE))
                return response.text
        except requests.RequestException as exc:
            logger.warning("Request failed: %s", exc)
        if attempt < RETRIES:
            logger.info("Retrying")
            time.sleep(random.uniform(*SLEEP_RANGE))
    return None


def parse_number(text: str) -> Optional[int]:
    digits = re.findall(r"\d+", text.replace("\xa0", " "))
    if not digits:
        return None
    return int("".join(digits))


def extract_year_and_mileage(listing) -> (Optional[int], Optional[int]):
    year = None
    mileage = None
    for li in listing.select("li"):
        value = li.get_text(strip=True)
        if not value:
            continue
        year_match = re.search(r"(19\d{2}|20\d{2})", value)
        if year_match and not year:
            year_val = int(year_match.group(1))
            if 1900 <= year_val <= 2100:
                year = year_val
        if "km" in value.lower() and mileage is None:
            mileage_val = parse_number(value)
            if mileage_val is not None:
                mileage = mileage_val
    return year, mileage


def parse_listings(html: str, brand: str, known_ids: set) -> List[Dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    entries: List[Dict[str, object]] = []
    listing_elements = soup.select("article[data-ad-id]") or soup.select("article[data-id]")

    for article in listing_elements:
        listing_id = article.get("data-ad-id") or article.get("data-id")
        if listing_id in known_ids:
            logger.info("Skipped known listing %s", listing_id)
            continue

        title_el = article.select_one('[data-testid="ad-title"]') or article.select_one("h1, h2, h3")
        model = title_el.get_text(strip=True) if title_el else None

        link_el = article.select_one("a")
        url = link_el.get("href") if link_el else None
        if url and url.startswith("/"):
            url = f"https://www.otomoto.pl{url}"

        price_el = article.select_one('[data-testid="ad-price"]') or article.select_one("span.price")
        price = parse_number(price_el.get_text()) if price_el else None

        year, mileage = extract_year_and_mileage(article)

        img_el = article.select_one("img")
        photo = None
        if img_el:
            photo = img_el.get("data-src") or img_el.get("src")

        entries.append(
            {
                "brand": brand,
                "model": model,
                "year": year,
                "price": price,
                "mileage": mileage,
                "photo": photo,
                "url": url,
                "id": listing_id,
            }
        )

    logger.info("Parsed listing count: %s", len(entries))
    return entries


def process_brand(brand: str, catalog: Dict[str, List[Dict[str, object]]], known_ids: Dict[str, set]):
    logger.info("Start brand: %s", brand)
    page = 1
    brand_slug = slugify_brand(brand)
    brand_entries = catalog.setdefault(brand, [])
    while not stop_requested:
        url = f"https://www.otomoto.pl/osobowe/{brand_slug}/?page={page}"
        logger.info("Page %s request", page)
        html = fetch_url(url)
        if html is None:
            logger.warning("Failed to fetch page %s for brand %s", page, brand)
            break
        listings = parse_listings(html, brand, known_ids.get(brand, set()))
        if not listings:
            logger.info("No listings found on page %s for brand %s; stopping pagination.", page, brand)
            break
        for entry in listings:
            if entry["id"] and entry["id"] in known_ids.get(brand, set()):
                continue
            brand_entries.append(entry)
            if entry["id"]:
                known_ids.setdefault(brand, set()).add(entry["id"])
        save_catalog(catalog)
        page += 1
    logger.info("Finished brand: %s", brand)


def main():
    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    catalog = load_catalog()
    known_ids = get_known_ids(catalog)

    try:
        for brand in BRANDS:
            if stop_requested:
                break
            process_brand(brand, catalog, known_ids)
    except Exception as exc:
        logger.exception("Unexpected error occurred: %s", exc)
    finally:
        save_catalog(catalog)
        logger.info("Catalog saved. Exiting safely.")


if __name__ == "__main__":
    main()
