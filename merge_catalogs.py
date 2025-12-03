import json
from pathlib import Path
from typing import Dict, List, Set

AUTOPLIUS_PATH = Path("autoplius.json")
PARTS_PATH = Path("parts_catalog.json")
OUTPUT_PATH = Path("sonver_catalog.json")


def load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def derive_brand_model(entry: Dict[str, object], known_brands: List[str]) -> (str, str):
    model_field = str(entry.get("model", "")).strip()
    for brand in known_brands:
        if model_field.lower().startswith(brand.lower()):
            cleaned_model = model_field[len(brand) :].strip()
            return brand, cleaned_model or model_field
        if brand.lower() in model_field.lower():
            return brand, model_field
    return "Unknown", model_field or "Unknown"


def add_oems(target: Dict[str, object], brand: str, model: str, part: str, entry: Dict[str, object]) -> None:
    target.setdefault(brand, {}).setdefault(model, {}).setdefault(part, set())
    oem_main = entry.get("oem_main")
    if oem_main:
        target[brand][model][part].add(str(oem_main))
    for cross in entry.get("oem_cross_refs", []):
        target[brand][model][part].add(str(cross))


def convert_sets_to_lists(tree: Dict[str, Dict[str, Dict[str, Set[str]]]]) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
    output: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
    for brand, models in tree.items():
        output[brand] = {}
        for model, parts in models.items():
            output[brand][model] = {}
            for part, oems in parts.items():
                output[brand][model][part] = sorted(oems)
    return output


def main() -> None:
    autoplius_data = load_json(AUTOPLIUS_PATH)
    parts_data = load_json(PARTS_PATH)

    known_brands = list(autoplius_data.keys())
    mapping: Dict[str, Dict[str, Dict[str, Set[str]]]] = {}

    for part, entries in parts_data.items():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            brand, model = derive_brand_model(entry, known_brands)
            add_oems(mapping, brand, model, part, entry)

    merged = convert_sets_to_lists(mapping)
    OUTPUT_PATH.write_text(json.dumps(merged, indent=2, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
