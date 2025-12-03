import json
import os
import random
import re
import time
from typing import Dict, List, Optional, Set

import requests
from bs4 import BeautifulSoup

BRANDS: List[str] = [
    "Audi",
    "BMW",
    "Mercedes",
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
    "Lancia",
    "Porsche",
    "Mini",
    "Volvo",
    "Land Rover",
    "Jaguar",
    "Saab",
    "Smart",
    "Cupra",
    "Ford Europe",
]

PARTS: List[str] = [
    "engine",
    "turbo",
    "gearbox",
    "alternator",
    "injector",
    "ecu",
    "abs pump",
    "radiator",
    "starter",
    "steering rack",
    "turbo actuator",
    "throttle body",
    "egr",
    "airflow sensor",
    "fuel pump",
]

USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.90 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.140 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_6_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.198 Safari/537.36",
    "Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:110.0) Gecko/20100101 Firefox/110.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.3; rv:111.0) Gecko/20100101 Firefox/111.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
]

PROXY = os.environ.get("SCRAPER_PROXY")
CATALOG_FILE = "catalog.json"
LOG_FILE = "log.txt"


def random_sleep() -> None:
    time.sleep(random.uniform(1.5, 4.0))


def build_proxies() -> Optional[Dict[str, str]]:
    if not PROXY:
        return None
    return {"http": PROXY, "https": PROXY}


def request_with_retry(url: str, params: Optional[Dict[str, str]] = None) -> Optional[str]:
    for attempt in range(5):
        try:
            response = requests.get(
                url,
                params=params,
                headers={"User-Agent": random.choice(USER_AGENTS)},
                timeout=20,
                proxies=build_proxies(),
            )
            if response.status_code == 200:
                return response.text
        except requests.RequestException:
            pass
        random_sleep()
    return None


def normalize_model(name: str) -> str:
    cleaned = re.sub(r"\(.*?\)", "", name)
    cleaned = re.sub(r"\b\d{4}\b", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = re.sub(r"\s+/.*", "", cleaned)
    cleaned = cleaned.replace("–", "-")
    cleaned = cleaned.strip("- ")
    return cleaned.title()


def extract_models_from_content(content: BeautifulSoup, brand: str) -> Set[str]:
    models: Set[str] = set()
    text_sources: List[str] = []
    for li in content.find_all("li"):
        text_sources.append(li.get_text(" ", strip=True))
    for cell in content.find_all(["td", "th"]):
        text_sources.append(cell.get_text(" ", strip=True))

    brand_lower = brand.lower()
    for source in text_sources:
        for candidate in re.findall(r"[A-Z][A-Za-z0-9][A-Za-z0-9\-\s]{0,24}", source):
            normalized = normalize_model(candidate)
            if not normalized or len(normalized) < 2 or len(normalized) > 30:
                continue
            lowered = normalized.lower()
            if lowered.startswith(brand_lower):
                lowered = lowered[len(brand_lower) :].strip()
                normalized = normalize_model(lowered) if lowered else normalized
            if normalized and not normalized.isdigit():
                models.add(normalized)
    return models


def fetch_models_for_brand(brand: str) -> List[str]:
    slug = brand.replace(" ", "_")
    candidates = [
        f"https://en.wikipedia.org/wiki/List_of_{slug}_vehicles",
        f"https://en.wikipedia.org/wiki/{slug}",
    ]
    collected: Set[str] = set()
    for url in candidates:
        html = request_with_retry(url)
        random_sleep()
        if not html:
            continue
        soup = BeautifulSoup(html, "lxml")
        content = soup.select_one("#mw-content-text") or soup
        collected |= extract_models_from_content(content, brand)
        if collected:
            break
    if not collected:
        collected.add(normalize_model(brand))
    return sorted(collected)


def parse_price(text: str) -> (Optional[float], Optional[str]):
    cleaned = text.replace("\xa0", " ")
    match = re.search(r"([£€$])\s?([0-9,.]+)", cleaned)
    if not match:
        return None, None
    currency = match.group(1)
    number = match.group(2).replace(",", "")
    try:
        value = float(number)
        return value, currency
    except ValueError:
        return None, currency


def find_oems(text: str) -> List[str]:
    normalized = text.upper().replace("-", " ")
    matches = re.findall(r"[A-Z0-9]{4,}", normalized)
    unique: List[str] = []
    seen: Set[str] = set()
    for m in matches:
        cleaned = m.strip().upper()
        if cleaned not in seen:
            unique.append(cleaned)
            seen.add(cleaned)
    return unique


def log_oems(oems: List[str]) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as log_file:
        for oem in oems:
            log_file.write(f"{oem}\n")


def extract_listings(html: str) -> List[Dict[str, object]]:
    soup = BeautifulSoup(html, "lxml")
    listings: List[Dict[str, object]] = []
    seen_oems: Set[str] = set()
    for item in soup.select("li.s-item"):
        title_el = item.select_one(".s-item__title")
        subtitle_el = item.select_one(".s-item__subtitle")
        if not title_el:
            continue
        title = title_el.get_text(" ", strip=True)
        if title.lower() in {"new listing", "listing preview"}:
            continue
        subtitle = subtitle_el.get_text(" ", strip=True) if subtitle_el else ""
        combined_text = f"{title} {subtitle}".strip()
        oems = find_oems(combined_text)
        if not oems:
            continue

        price_el = item.select_one(".s-item__price")
        if not price_el:
            continue
        price_value, currency = parse_price(price_el.get_text(" ", strip=True))
        if price_value is None or not currency:
            continue

        link_el = item.select_one("a.s-item__link")
        if not link_el or not link_el.get("href"):
            continue
        listing_url = link_el.get("href").split("?")[0]

        image_el = item.select_one("img.s-item__image-img")
        image_url = None
        if image_el:
            image_url = image_el.get("src") or image_el.get("data-src")

        frequency: Dict[str, int] = {}
        for oem in oems:
            frequency[oem] = frequency.get(oem, 0) + 1
        sorted_oems = sorted(frequency, key=lambda k: (-frequency[k], k))
        primary = sorted_oems[0]
        if primary in seen_oems:
            continue
        seen_oems.add(primary)
        cross_refs = [o for o in sorted_oems if o != primary]
        log_oems(sorted_oems)

        listings.append(
            {
                "oem_main": primary,
                "oem_cross_refs": cross_refs,
                "title": title,
                "price": price_value,
                "currency": currency,
                "image_url": image_url,
                "ebay_url": listing_url,
            }
        )
    return listings


def search_ebay(brand: str, model: str, part: str) -> List[Dict[str, object]]:
    queries = [
        f"{brand} {model} {part}",
        f"{brand} {model} {part} OEM",
        f"{brand} {model} {part} replacement",
    ]
    aggregated: List[Dict[str, object]] = []
    seen_primary: Set[str] = set()
    for query in queries:
        html = request_with_retry(
            "https://www.ebay.com/sch/i.html",
            params={"_nkw": query, "_sop": "12"},
        )
        random_sleep()
        if not html:
            continue
        results = extract_listings(html)
        for listing in results[:10]:
            primary = listing.get("oem_main")
            if primary and primary in seen_primary:
                continue
            if primary:
                seen_primary.add(primary)
            aggregated.append(listing)
    return aggregated


def load_catalog() -> Dict[str, Dict[str, Dict[str, List[Dict[str, object]]]]]:
    if os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE, "r", encoding="utf-8") as file:
            return json.load(file)
    return {}


def save_catalog(catalog: Dict[str, Dict[str, Dict[str, List[Dict[str, object]]]]]) -> None:
    with open(CATALOG_FILE, "w", encoding="utf-8") as file:
        json.dump(catalog, file, ensure_ascii=False, indent=2)


def ensure_nested(catalog: Dict, brand: str, model: str) -> None:
    if brand not in catalog:
        catalog[brand] = {}
    if model not in catalog[brand]:
        catalog[brand][model] = {}


def should_skip_part(catalog: Dict, brand: str, model: str, part: str) -> bool:
    return brand in catalog and model in catalog[brand] and part in catalog[brand][model]


def main() -> None:
    catalog = load_catalog()
    for brand in BRANDS:
        models = fetch_models_for_brand(brand)
        for model in models:
            for part in PARTS:
                if should_skip_part(catalog, brand, model, part):
                    continue
                ensure_nested(catalog, brand, model)
                listings = search_ebay(brand, model, part)
                if listings:
                    catalog[brand][model][part] = listings
                    save_catalog(catalog)


if __name__ == "__main__":
    main()
