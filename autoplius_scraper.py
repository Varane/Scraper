import json
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin

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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
]

DATA_PATH = Path("autoplius.json")
LOG_PATH = Path("autoplius_log.txt")


# Logging ---------------------------------------------------------------------

def timestamp() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def log(message: str) -> None:
    try:
        with LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(f"[{timestamp()}] {message}\n")
    except Exception:
        pass


# HTTP helpers ----------------------------------------------------------------

def random_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    }


def request_with_retry(url: str, session: requests.Session) -> Optional[str]:
    for attempt in range(1, 6):
        sleep_seconds = random.uniform(1.0, 2.5)
        log(f"Sleeping {sleep_seconds:.2f}s before request attempt {attempt} URL={url}")
        time.sleep(sleep_seconds)
        try:
            response = session.get(url, headers=random_headers(), timeout=25)
            if response.status_code == 200:
                log(f"Request success attempt={attempt} status=200 URL={url}")
                return response.text
            log(f"Non-200 status={response.status_code} attempt={attempt} URL={url}")
        except requests.RequestException as exc:
            log(f"Request exception attempt={attempt} URL={url} exc={exc!r}")
    log(f"Failed all retries for URL={url}")
    return None


# Parsing helpers -------------------------------------------------------------

def extract_int(text: str) -> Optional[int]:
    digits = re.findall(r"[0-9]+", text)
    if not digits:
        return None
    try:
        return int(digits[0])
    except ValueError:
        return None


def extract_float(text: str) -> Optional[float]:
    match = re.search(r"([0-9]+(?:[.,][0-9]+)?)", text)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_specs_value(soup: BeautifulSoup, labels: List[str]) -> Optional[str]:
    for dt in soup.select("dt"):
        label_text = dt.get_text(strip=True).lower()
        if any(label.lower() in label_text for label in labels):
            dd = dt.find_next_sibling("dd")
            if dd:
                return dd.get_text(strip=True)
    return None


def parse_photos(soup: BeautifulSoup) -> (str, List[str]):
    gallery_imgs = soup.select("img")
    photo_urls: List[str] = []
    for img in gallery_imgs:
        candidate = img.get("data-src") or img.get("src") or ""
        if candidate and candidate.startswith("http"):
            photo_urls.append(candidate)
    photo_urls = list(dict.fromkeys(photo_urls))
    main_photo = photo_urls[0] if photo_urls else ""
    additional = photo_urls[1:] if len(photo_urls) > 1 else []
    return main_photo, additional


def parse_price(soup: BeautifulSoup) -> Optional[int]:
    price_meta = soup.select_one('[itemprop="price"]')
    if price_meta and price_meta.get("content"):
        return extract_int(price_meta["content"])
    text_candidates = [
        tag.get_text(" ", strip=True)
        for tag in soup.select(".price, .pricefield, .announcement-price, .value")
    ]
    for text in text_candidates:
        price_val = extract_int(text)
        if price_val:
            return price_val
    return None


def parse_listing_detail(url: str, session: requests.Session, brand: str) -> Optional[Dict[str, object]]:
    html = request_with_retry(url, session)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")

    title = soup.find("h1")
    title_text = title.get_text(" ", strip=True) if title else ""
    model = ""
    if title_text:
        lowered = title_text.replace(brand, "", 1).strip()
        model = lowered.split(" ")[0] if lowered else ""

    generation = parse_specs_value(soup, ["karta", "generation", "modelio versija", "modifikacija"])
    year_text = parse_specs_value(soup, ["metai", "pagaminimo", "year"])
    year = extract_int(year_text or "")
    mileage_text = parse_specs_value(soup, ["rida", "mileage", "km"])
    mileage = extract_int(mileage_text or "")
    vin = parse_specs_value(soup, ["vin"])
    price = parse_price(soup)
    main_photo, additional_photos = parse_photos(soup)

    listing_id = None
    id_match = re.search(r"(\d+)(?:\.html)?$", url)
    if id_match:
        listing_id = id_match.group(1)
    if not listing_id:
        id_tag = soup.select_one("[data-id]")
        if id_tag and id_tag.has_attr("data-id"):
            listing_id = id_tag["data-id"]

    return {
        "brand": brand,
        "model": model,
        "generation": generation or "",
        "year": year,
        "mileage": mileage,
        "price": price,
        "vin": vin or "",
        "photo": main_photo,
        "photos": additional_photos,
        "url": url,
        "id": listing_id or "",
    }


def parse_listings_page(html: str, base_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    listing_cards = soup.select("[data-id], .announcement-item, article")
    listings: List[Dict[str, str]] = []
    for card in listing_cards:
        link = card.select_one("a[href]")
        if not link:
            continue
        url = urljoin(base_url, link.get("href", ""))
        listing_id = card.get("data-id") or card.get("data-ad-id")
        image_tag = card.select_one("img")
        photo = ""
        if image_tag:
            photo = image_tag.get("data-src") or image_tag.get("src") or ""
        if url:
            listings.append({"url": url, "id": listing_id or "", "photo": photo})
    unique = []
    seen_urls: Set[str] = set()
    for item in listings:
        if item["url"] not in seen_urls:
            unique.append(item)
            seen_urls.add(item["url"])
    return unique


def extract_total_pages(html: str) -> int:
    soup = BeautifulSoup(html, "lxml")
    page_numbers = set()
    for link in soup.select("a[href]"):
        text = link.get_text(strip=True)
        if text.isdigit():
            page_numbers.add(int(text))
    return max(page_numbers) if page_numbers else 1


# Persistence -----------------------------------------------------------------

def load_existing() -> Dict[str, List[Dict[str, object]]]:
    if DATA_PATH.exists():
        try:
            return json.loads(DATA_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            log(f"Failed to load existing data: {exc!r}")
            return {}
    return {}


def save_data(data: Dict[str, List[Dict[str, object]]]) -> None:
    try:
        DATA_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        log("autoplius.json saved")
    except Exception as exc:
        log(f"Failed to save data: {exc!r}")


# Scraper ---------------------------------------------------------------------

def scrape_brand(brand: str, session: requests.Session, existing_ids: Set[str]) -> List[Dict[str, object]]:
    log(f"Starting brand {brand}")
    brand_slug = brand.lower().replace(" ", "-")
    base_url = f"https://autoplius.lt/skelbimai/naudoti-automobiliai/{brand_slug}"

    first_page = request_with_retry(base_url, session)
    if not first_page:
        log(f"Failed to fetch first page for brand={brand}")
        return []

    total_pages = extract_total_pages(first_page)
    log(f"Detected {total_pages} pages for brand={brand}")

    all_entries: List[Dict[str, object]] = []
    for page in range(1, total_pages + 1):
        page_url = base_url if page == 1 else f"{base_url}?page_nr={page}"
        page_html = first_page if page == 1 else request_with_retry(page_url, session)
        if not page_html:
            continue
        for listing in parse_listings_page(page_html, base_url):
            listing_id = listing.get("id") or ""
            if listing_id and listing_id in existing_ids:
                log(f"Skipping already scraped listing id={listing_id}")
                continue
            detail = parse_listing_detail(listing["url"], session, brand)
            if not detail:
                continue
            if not detail.get("photo") and listing.get("photo"):
                detail["photo"] = listing["photo"]
            listing_id = detail.get("id")
            if listing_id:
                existing_ids.add(str(listing_id))
            all_entries.append(detail)
    log(f"Finished brand {brand} with {len(all_entries)} listings")
    return all_entries


def main() -> None:
    LOG_PATH.touch(exist_ok=True)
    session = requests.Session()
    data = load_existing()
    scraped_ids: Set[str] = set()
    for brand_entries in data.values():
        for entry in brand_entries:
            if isinstance(entry, dict) and entry.get("id"):
                scraped_ids.add(str(entry["id"]))

    for brand in BRANDS:
        try:
            brand_results = scrape_brand(brand, session, scraped_ids)
            if brand_results:
                data.setdefault(brand, [])
                data[brand].extend(brand_results)
                save_data(data)
        except KeyboardInterrupt:
            log("KeyboardInterrupt received, saving and exiting")
            break
        except Exception as exc:
            log(f"Unhandled exception for brand={brand}: {exc!r}")
            save_data(data)

    save_data(data)
    log("Scraping completed")


if __name__ == "__main__":
    main()
