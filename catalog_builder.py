import argparse
import json
import random
import re
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

BRAND_MODELS = {
    "Audi": ["A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "Q3", "Q5", "Q7"],
    "BMW": ["1 Series", "2 Series", "3 Series", "4 Series", "5 Series", "7 Series", "X1", "X3", "X5"],
    "Mercedes": ["A-Class", "B-Class", "C-Class", "E-Class", "S-Class", "GLA", "GLC", "GLE", "Sprinter"],
    "Volkswagen": ["Golf", "Polo", "Passat", "Tiguan", "Touran", "Caddy", "Transporter"],
    "Skoda": ["Fabia", "Octavia", "Superb", "Kodiaq", "Karoq"],
    "Seat": ["Ibiza", "Leon", "Toledo", "Altea", "Ateca"],
    "Opel": ["Corsa", "Astra", "Insignia", "Zafira", "Mokka"],
    "Peugeot": ["208", "308", "508", "2008", "3008", "5008", "Partner"],
    "Citroen": ["C1", "C3", "C4", "C5", "Berlingo", "Jumpy"],
    "Renault": ["Clio", "Megane", "Laguna", "Scenic", "Twingo", "Kangoo", "Master"],
    "Dacia": ["Logan", "Sandero", "Duster", "Dokker"],
    "Fiat": ["Panda", "500", "Punto", "Tipo", "Doblo", "Ducato"],
    "Alfa Romeo": ["Giulietta", "Giulia", "MiTo", "Stelvio"],
    "Volvo": ["S40", "S60", "S80", "V40", "V60", "XC60", "XC90"],
    "Porsche": ["911", "Cayenne", "Macan", "Panamera", "Boxster"],
    "Mini": ["One", "Cooper", "Clubman", "Countryman"],
    "Land Rover": ["Defender", "Discovery", "Range Rover", "Range Rover Sport", "Evoque"],
    "Jaguar": ["XE", "XF", "XJ", "F-Pace", "E-Pace", "F-Type"],
    "Saab": ["9-3", "9-5", "900", "9000"],
    "Smart": ["Fortwo", "Forfour"],
    "Ford Europe": ["Fiesta", "Focus", "Mondeo", "Kuga", "Transit"],
}

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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:117.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
]

CATALOG_PATH = Path("catalog.json")
LOG_PATH = Path("log.txt")


# Logging helpers

def timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_files_exist() -> None:
    if not CATALOG_PATH.exists():
        try:
            CATALOG_PATH.write_text("{}", encoding="utf-8")
        except Exception:
            pass
    if not LOG_PATH.exists():
        try:
            LOG_PATH.touch()
        except Exception:
            pass
    log_info("--- RUN STARTED ---")


def log_info(message: str) -> None:
    try:
        with LOG_PATH.open("a", encoding="utf-8") as log_file:
            log_file.write(f"[{timestamp()}] INFO {message}\n")
    except Exception:
        pass


def log_error(message: str) -> None:
    try:
        with LOG_PATH.open("a", encoding="utf-8") as log_file:
            log_file.write(f"[{timestamp()}] ERROR {message}\n")
    except Exception:
        pass


def log_oems(brand: str, model: str, part: str, oems: List[str]) -> None:
    joined = ",".join(oems)
    log_info(f"OEM brand={brand} model={model} part={part} oems={joined}.")


# Catalog helpers

def load_catalog() -> Dict:
    try:
        with CATALOG_PATH.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as exc:
        log_error(f"Failed to load catalog: {exc!r}")
        return {}


def save_catalog(catalog: Dict) -> None:
    try:
        temp_path = CATALOG_PATH.with_suffix(".tmp")
        with temp_path.open("w", encoding="utf-8") as fh:
            json.dump(catalog, fh, indent=2, ensure_ascii=False)
        temp_path.replace(CATALOG_PATH)
    except Exception as exc:
        log_error(f"Failed to save catalog: {exc!r}")


def existing_oems_for_part(catalog: Dict, brand: str, model: str, part: str) -> Set[str]:
    existing = catalog.get(brand, {}).get(model, {}).get(part, [])
    collected: Set[str] = set()
    for entry in existing:
        main = entry.get("oem_main")
        if main:
            collected.add(str(main).upper())
        for cross in entry.get("oem_cross_refs", []):
            collected.add(str(cross).upper())
    return collected


# Scraping helpers

def random_headers() -> Dict[str, str]:
    return {"User-Agent": random.choice(USER_AGENTS), "Accept-Language": "en-US,en;q=0.9"}


def polite_sleep() -> None:
    time.sleep(random.uniform(1.0, 3.0))


def request_with_retry(url: str, session: requests.Session, proxy: Optional[str]) -> Optional[str]:
    proxies = {"http": proxy, "https": proxy} if proxy else None
    for attempt in range(5):
        polite_sleep()
        try:
            response = session.get(url, headers=random_headers(), proxies=proxies, timeout=20)
            if response.status_code == 200:
                return response.text
            log_error(f"Non-200 status {response.status_code} for {url}")
        except requests.RequestException as exc:
            log_error(f"Request failed (attempt {attempt + 1}) for {url}: {exc!r}")
    return None


def extract_oems(text: str) -> List[str]:
    raw = re.findall(r"[A-Z0-9]{4,}", text.upper())
    normalized = [re.sub(r"[-\s]", "", token) for token in raw]
    return normalized


def parse_price(price_text: str) -> Optional[Dict[str, float]]:
    match = re.search(r"([€£$])\s*([0-9]+(?:[.,][0-9]+)?)", price_text)
    if not match:
        return None
    symbol, amount_text = match.groups()
    currency = {"€": "EUR", "£": "GBP", "$": "USD"}.get(symbol, symbol)
    try:
        amount = float(amount_text.replace(",", ""))
    except ValueError:
        return None
    return {"currency": currency, "price": amount}


def extract_listings(query: str, session: requests.Session, proxy: Optional[str], existing_oems: Set[str], brand: str, model: str, part: str) -> List[Dict[str, object]]:
    encoded_query = quote_plus(query)
    url = f"https://www.ebay.com/sch/i.html?_nkw={encoded_query}"
    html = request_with_retry(url, session, proxy)
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    listings: List[Dict[str, object]] = []
    seen_oems = set(existing_oems)

    for item in soup.select("li.s-item"):
        if len(listings) >= 10:
            break
        try:
            title_tag = item.select_one(".s-item__title")
            if not title_tag or "Shop on eBay" in title_tag.get_text():
                continue
            price_tag = item.select_one(".s-item__price")
            link_tag = item.select_one("a.s-item__link")
            if not price_tag or not link_tag:
                continue
            price_info = parse_price(price_tag.get_text(" "))
            if not price_info:
                continue
            subtitle_tag = item.select_one(".s-item__subtitle")
            full_text = title_tag.get_text(" ") + " " + (subtitle_tag.get_text(" ") if subtitle_tag else "")
            oems = extract_oems(full_text)
            if not oems:
                continue
            frequencies = Counter(oems)
            primary = frequencies.most_common(1)[0][0]
            primary_upper = primary.upper()
            if primary_upper in seen_oems:
                continue
            cross_refs = sorted({oem for oem in oems if oem.upper() != primary_upper})
            image_tag = item.select_one(".s-item__image-img")
            image_url = ""
            if image_tag:
                image_url = image_tag.get("src") or image_tag.get("data-src") or ""

            listing = {
                "oem_main": primary_upper,
                "oem_cross_refs": [c.upper() for c in cross_refs],
                "title": title_tag.get_text(strip=True),
                "price": price_info["price"],
                "currency": price_info["currency"],
                "image_url": image_url,
                "ebay_url": link_tag.get("href", ""),
            }
            listings.append(listing)
            seen_oems.add(primary_upper)
            log_oems(brand, model, part, [primary_upper] + [c.upper() for c in cross_refs])
        except Exception as exc:  # pragma: no cover - defensive logging
            log_error(f"Failed to parse listing for query '{query}': {exc!r}")
            continue
    return listings


def ensure_brand_model_part(catalog: Dict, brand: str, model: str, part: str) -> None:
    catalog.setdefault(brand, {}).setdefault(model, {}).setdefault(part, [])


def build_catalog(proxy: Optional[str] = None) -> Dict:
    session = requests.Session()
    catalog = load_catalog()

    for brand, models in BRAND_MODELS.items():
        log_info(f"Processing brand {brand}")
        for model in models:
            for part in PARTS:
                try:
                    existing_entries = catalog.get(brand, {}).get(model, {}).get(part, [])
                    if existing_entries:
                        continue
                    existing_oems = existing_oems_for_part(catalog, brand, model, part)
                    part_results: List[Dict[str, object]] = []
                    queries = [
                        f"{brand} {model} {part}",
                        f"{brand} {model} {part} OEM",
                        f"{brand} {model} {part} replacement",
                    ]
                    for query in queries:
                        dedupe_set = existing_oems | {entry["oem_main"].upper() for entry in part_results if isinstance(entry.get("oem_main"), str)}
                        dedupe_set.update({ref.upper() for entry in part_results for ref in entry.get("oem_cross_refs", []) if isinstance(ref, str)})
                        listings = extract_listings(query, session, proxy, dedupe_set, brand, model, part)
                        part_results.extend(listings)
                    if part_results:
                        ensure_brand_model_part(catalog, brand, model, part)
                        catalog[brand][model][part] = part_results
                        save_catalog(catalog)
                except Exception as exc:
                    log_error(f"Error while processing brand={brand} model={model} part={part}: {exc!r}")
                    save_catalog(catalog)
                    continue
    save_catalog(catalog)
    return catalog


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a spare parts catalog from eBay.")
    parser.add_argument("--proxy", help="Optional proxy URL, e.g. http://user:pass@host:port")
    args = parser.parse_args()

    ensure_files_exist()

    catalog: Optional[Dict] = None
    try:
        catalog = build_catalog(proxy=args.proxy)
    except KeyboardInterrupt:
        log_error("KeyboardInterrupt received, saving catalog and exiting.")
        print("KeyboardInterrupt received, exiting. Catalog saved.")
    except Exception as exc:
        log_error(f"Unhandled exception in main: {exc!r}")
        print("An unexpected error occurred. See log.txt for details. Catalog saved.")
    finally:
        if catalog is None:
            catalog = load_catalog()
        save_catalog(catalog)


if __name__ == "__main__":
    main()
