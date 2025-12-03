import json
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

PARTS = [
    "engine",
    "turbo",
    "gearbox",
    "injector",
    "alternator",
    "starter",
    "ecu",
    "abs pump",
    "steering rack",
    "turbo actuator",
    "egr",
    "airflow sensor",
    "fuel pump",
    "radiator",
    "throttle body",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
]

DATA_PATH = Path("parts_catalog.json")
LOG_PATH = Path("autoplius_log.txt")
BASE_SEARCH_URL = "https://rrr.lt/paieska/?q={query}"


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

def normalize_oem(text: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", text.upper())


def parse_price(text: str) -> Optional[float]:
    match = re.search(r"([0-9]+(?:[.,][0-9]+)?)", text)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def extract_listing_cards(html: str) -> List[BeautifulSoup]:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select(".products .product, .item, .products-item")
    return cards or soup.select("article")


def parse_listing_card(card: BeautifulSoup, base_url: str) -> Dict[str, str]:
    link = card.select_one("a[href]")
    url = urljoin(base_url, link.get("href", "")) if link else ""
    image_tag = card.select_one("img")
    image_url = ""
    if image_tag:
        image_url = image_tag.get("data-src") or image_tag.get("src") or ""
    price_text = card.get_text(" ", strip=True)
    price = parse_price(price_text)
    title = link.get_text(strip=True) if link else card.get_text(" ", strip=True)
    return {
        "url": url,
        "image_url": image_url,
        "price": price,
        "title": title,
    }


def parse_detail_page(url: str, session: requests.Session, fallback: Dict[str, object]) -> Optional[Dict[str, object]]:
    html = request_with_retry(url, session)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")

    title = soup.find("h1")
    title_text = title.get_text(" ", strip=True) if title else fallback.get("title", "")

    price = None
    price_tag = soup.select_one(".price, [itemprop='price']")
    if price_tag:
        price = parse_price(price_tag.get_text(" ", strip=True))
    if price is None:
        price = fallback.get("price")

    currency = "EUR"

    oem_main = ""
    oem_cross: List[str] = []
    specs_text = soup.get_text(" ", strip=True)
    oem_candidates = [normalize_oem(token) for token in re.findall(r"[A-Z0-9]{4,}", specs_text)]
    if oem_candidates:
        oem_main = oem_candidates[0]
        oem_cross = sorted(set(oem_candidates[1:]))

    model = ""
    year = None
    for dt in soup.select("dt"):
        label = dt.get_text(strip=True).lower()
        dd = dt.find_next_sibling("dd")
        value = dd.get_text(strip=True) if dd else ""
        if "model" in label or "automobil" in label:
            model = value
        if "year" in label or "metai" in label:
            try:
                year = int(re.findall(r"\d{4}", value)[0])
            except Exception:
                year = None

    image_url = fallback.get("image_url", "")
    gallery = soup.select("img")
    for img in gallery:
        candidate = img.get("data-src") or img.get("src") or ""
        if candidate.startswith("http"):
            image_url = candidate
            break

    return {
        "oem_main": oem_main,
        "oem_cross_refs": oem_cross,
        "price": price,
        "currency": currency,
        "model": model,
        "year": year,
        "image_url": image_url,
        "url": url,
        "title": title_text,
    }


# Persistence -----------------------------------------------------------------

def load_existing() -> Dict:
    if DATA_PATH.exists():
        try:
            return json.loads(DATA_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            log(f"Failed to load existing parts catalog: {exc!r}")
            return {}
    return {}


def save_data(data: Dict) -> None:
    try:
        DATA_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        log("parts_catalog.json saved")
    except Exception as exc:
        log(f"Failed to save parts catalog: {exc!r}")


# Scraper ---------------------------------------------------------------------

def scrape_part(part: str, session: requests.Session) -> List[Dict[str, object]]:
    log(f"Starting part search {part}")
    encoded = quote_plus(part)
    url = BASE_SEARCH_URL.format(query=encoded)
    html = request_with_retry(url, session)
    if not html:
        return []

    cards = extract_listing_cards(html)
    results: List[Dict[str, object]] = []
    for card in cards:
        summary = parse_listing_card(card, url)
        if not summary.get("url"):
            continue
        detail = parse_detail_page(summary["url"], session, summary)
        if not detail:
            continue
        results.append(detail)
    log(f"Finished part {part} with {len(results)} listings")
    return results


def main() -> None:
    LOG_PATH.touch(exist_ok=True)
    session = requests.Session()
    data = load_existing()

    for part in PARTS:
        try:
            part_results = scrape_part(part, session)
            if part_results:
                data.setdefault(part, [])
                data[part] = part_results
                save_data(data)
        except KeyboardInterrupt:
            log("KeyboardInterrupt received, saving and exiting")
            break
        except Exception as exc:
            log(f"Unhandled exception while scraping part={part}: {exc!r}")
            save_data(data)

    save_data(data)
    log("Parts scraping completed")


if __name__ == "__main__":
    main()
