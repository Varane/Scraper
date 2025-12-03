import argparse
import json
import random
import re
import time
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

BRANDS = [
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

PARTS = [
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

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15",
]

BRAND_SLUGS = {
    "Mercedes": "Mercedes-Benz",
    "Skoda": "Škoda",
    "Seat": "SEAT",
    "Citroen": "Citroën",
    "Mini": "Mini_(marque)",
    "Land Rover": "Land_Rover",
    "Jaguar": "Jaguar_Cars",
    "Saab": "Saab_Automobile",
    "Smart": "Smart_(marque)",
    "Ford Europe": "Ford_of_Europe",
}

CATALOG_PATH = Path("catalog.json")
LOG_PATH = Path("log.txt")


def random_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
    }


def sleep_with_jitter() -> None:
    time.sleep(random.uniform(1.5, 4.0))


def request_with_retry(url: str, session: requests.Session, proxy: Optional[str] = None) -> Optional[str]:
    for attempt in range(5):
        sleep_with_jitter()
        try:
            response = session.get(url, headers=random_headers(), proxies={"http": proxy, "https": proxy} if proxy else None, timeout=20)
            if response.status_code == 200:
                return response.text
        except requests.RequestException:
            pass
    return None


def normalize_model_name(name: str) -> str:
    cleaned = re.sub(r"\(.*?\)", "", name)
    cleaned = re.sub(r"\b\d{4}(?:–\d{4})?", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.title()


def extract_models_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    content = soup.find("div", {"class": "mw-parser-output"}) or soup
    candidates: List[str] = []
    for li in content.find_all("li"):
        text = li.get_text(strip=True)
        if 2 < len(text) <= 35 and not text.lower().startswith("see also"):
            candidates.append(text)
    for cell in content.select("table td, table th"):
        text = cell.get_text(strip=True)
        if 2 < len(text) <= 35:
            candidates.append(text)
    normalized = {normalize_model_name(c) for c in candidates if not any(prefix in c.lower() for prefix in ["see also", "external", "references"])}
    return sorted({c for c in normalized if len(c) > 1})


def brand_slug(brand: str) -> str:
    return BRAND_SLUGS.get(brand, brand).replace(" ", "_")


def fetch_models_for_brand(brand: str, session: requests.Session, proxy: Optional[str]) -> List[str]:
    slug = brand_slug(brand)
    candidates = [
        f"https://en.wikipedia.org/wiki/List_of_{slug}_vehicles",
        f"https://en.wikipedia.org/wiki/List_of_{slug}_automobiles",
        f"https://en.wikipedia.org/wiki/List_of_{slug}_models",
        f"https://en.wikipedia.org/wiki/List_of_{slug}_cars",
        f"https://en.wikipedia.org/wiki/{slug}",
        f"https://en.wikipedia.org/w/index.php?search={quote_plus(brand + ' car models')}",
    ]
    for url in candidates:
        html = request_with_retry(url, session, proxy)
        if not html:
            continue
        models = extract_models_from_html(html)
        if models:
            return models
    return []


def extract_oems(text: str) -> List[str]:
    raw = re.findall(r"[A-Z0-9]{4,}", text.upper())
    normalized = [re.sub(r"[-\s]", "", token) for token in raw]
    return normalized


def parse_currency(price_text: str) -> (Optional[str], Optional[str]):
    match = re.search(r"([€£$])\s*([0-9,.]+)", price_text)
    if not match:
        return None, None
    symbol, amount = match.groups()
    currency = {"€": "EUR", "£": "GBP", "$": "USD"}.get(symbol, symbol)
    return currency, amount.replace(",", "")


def extract_listings(query: str, session: requests.Session, proxy: Optional[str], existing_oems: Iterable[str]) -> List[Dict[str, str]]:
    encoded_query = quote_plus(query)
    url = f"https://www.ebay.com/sch/i.html?_nkw={encoded_query}"
    html = request_with_retry(url, session, proxy)
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    listings: List[Dict[str, str]] = []
    seen_oems = {oem.upper() for oem in existing_oems}
    for item in soup.select("li.s-item"):
        if len(listings) >= 10:
            break
        title_tag = item.select_one(".s-item__title")
        if not title_tag or "Shop on eBay" in title_tag.get_text():
            continue
        subtitle_tag = item.select_one(".s-item__subtitle")
        price_tag = item.select_one(".s-item__price")
        link_tag = item.select_one("a.s-item__link")
        image_tag = item.select_one(".s-item__image-img")
        if not price_tag or not link_tag:
            continue
        currency, amount = parse_currency(price_tag.get_text())
        if not currency or not amount:
            continue
        full_text = title_tag.get_text(" ") + " " + (subtitle_tag.get_text(" ") if subtitle_tag else "")
        oems = extract_oems(full_text)
        if not oems:
            continue
        frequencies = Counter(oems)
        primary = frequencies.most_common(1)[0][0]
        if primary in seen_oems:
            continue
        cross_refs = sorted({oem for oem in oems if oem != primary})
        listing = {
            "oem_main": primary,
            "oem_cross_refs": cross_refs,
            "title": title_tag.get_text(strip=True),
            "price": amount,
            "currency": currency,
            "image_url": image_tag["src"] if image_tag and image_tag.get("src") else image_tag.get("data-src") if image_tag else "",
            "ebay_url": link_tag["href"],
        }
        listings.append(listing)
        seen_oems.add(primary)
        log_oems([primary] + cross_refs)
    return listings


def log_oems(oems: List[str]) -> None:
    with LOG_PATH.open("a", encoding="utf-8") as log_file:
        for oem in oems:
            log_file.write(f"{oem}\n")


def load_catalog() -> Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]]:
    if CATALOG_PATH.exists():
        with CATALOG_PATH.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def save_catalog(catalog: Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]]) -> None:
    with CATALOG_PATH.open("w", encoding="utf-8") as fh:
        json.dump(catalog, fh, indent=2, ensure_ascii=False)


def existing_oems_for_part(catalog: Dict, brand: str, model: str, part: str) -> List[str]:
    existing = catalog.get(brand, {}).get(model, {}).get(part, [])
    collected: List[str] = []
    for entry in existing:
        collected.append(entry.get("oem_main", ""))
        collected.extend(entry.get("oem_cross_refs", []))
    return collected


def ensure_brand_model(catalog: Dict, brand: str, model: str) -> None:
    catalog.setdefault(brand, {})
    catalog[brand].setdefault(model, {})


def build_catalog(proxy: Optional[str] = None) -> None:
    session = requests.Session()
    catalog = load_catalog()

    for brand in BRANDS:
        print(f"Processing brand: {brand}")
        models = fetch_models_for_brand(brand, session, proxy)
        if not models:
            print(f"No models found for {brand}, skipping.")
            continue
        for model in models:
            ensure_brand_model(catalog, brand, model)
            for part in PARTS:
                existing_entries = catalog[brand][model].get(part, [])
                if existing_entries:
                    continue
                oems = existing_oems_for_part(catalog, brand, model, part)
                part_results: List[Dict[str, str]] = []
                for suffix in ("", " OEM", " replacement"):
                    query = f"{brand} {model} {part}{suffix}"
                    part_results.extend(extract_listings(query, session, proxy, oems + [r["oem_main"] for r in part_results]))
                    sleep_with_jitter()
                if part_results:
                    catalog[brand][model][part] = part_results
                    save_catalog(catalog)
    save_catalog(catalog)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build an eBay spare parts catalog for EU brands.")
    parser.add_argument("--proxy", help="Optional proxy URL (e.g. http://user:pass@host:port)")
    args = parser.parse_args()

    build_catalog(args.proxy)


if __name__ == "__main__":
    main()
