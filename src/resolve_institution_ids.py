import csv
import json
import pathlib
import time
import urllib.parse

import requests

DATA_DIR = pathlib.Path("data")
DATA_DIR.mkdir(exist_ok=True)

BASE_URL = "https://api.openalex.org/institutions"

def load_institutions():
    institutions = []
    with open(DATA_DIR / "institutions.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            institutions.append(row)
    return institutions

def search_openalex_institution(name):
    """
    Call OpenAlex institutions search by name.
    Returns the best match (id, display_name) or None.
    """
    params = {
        "search": name,
        "per-page": 5,
    }
    url = BASE_URL + "?" + urllib.parse.urlencode(params)
    print(f"[INFO] Searching OpenAlex for: {name}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    results = data.get("results", [])
    if not results:
        return None

    # Take the first result as best guess
    top = results[0]
    return {
        "id": top.get("id"),
        "display_name": top.get("display_name"),
        "country_code": top.get("country_code"),
        "type": top.get("type"),
    }

def main():
    institutions = load_institutions()
    mapped = []

    for inst in institutions:
        name = inst["name"]
        state = inst.get("state", "")
        short_label = inst.get("short_label", "")

        try:
            match = search_openalex_institution(name)
        except Exception as e:
            print(f"[WARN] Error searching for {name}: {e}")
            match = None

        if match:
            print(f"  -> Matched to: {match['display_name']} ({match['id']})")
            openalex_id = match["id"]
            display_name = match["display_name"]
        else:
            print(f"  -> No match found for {name}")
            openalex_id = ""
            display_name = ""

        mapped.append({
            "name": name,
            "state": state,
            "short_label": short_label,
            "openalex_id": openalex_id,
            "openalex_display_name": display_name,
        })

        time.sleep(1)  # be polite to API

    # Save JSON
    json_path = DATA_DIR / "institution_ids.json"
    json_path.write_text(json.dumps(mapped, indent=2), encoding="utf-8")
    print(f"[INFO] Saved {len(mapped)} institution mappings to {json_path}")

    # Save CSV with IDs
    csv_path = DATA_DIR / "institutions_with_ids.csv"
    fieldnames = ["name", "state", "short_label", "openalex_id", "openalex_display_name"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(mapped)
    print(f"[INFO] Saved CSV with IDs to {csv_path}")

if __name__ == "__main__":
    main()
