import csv
import json
import pathlib
import time
from collections import Counter
import requests

BASE_URL = "https://api.openalex.org/works"
DATA_DIR = pathlib.Path("data")
DATA_DIR.mkdir(exist_ok=True)


def load_institutions():
    institutions = []
    with open(DATA_DIR / "institutions.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            institutions.append(row)
    return institutions


def fetch_works_for_institution(openalex_id, from_year=2020):
    """Fetch works for a single institution from OpenAlex (simplified, paginated)."""
    works = []
    page = 1
    per_page = 50  # keep small to avoid long runs at first
    from_date = f"{from_year}-01-01"

    while True:
        params = {
            "filter": f"institutions.id:{openalex_id},from_publication_date:{from_date}",
            "per-page": per_page,
            "page": page
        }
        print(f"[INFO] Fetching page {page} for {openalex_id}...")
        r = requests.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        results = data.get("results", [])
        if not results:
            break

        works.extend(results)

        # Stop early during development to avoid huge calls
        if page >= 3:
            break

        page += 1
        time.sleep(1)  # be polite

    return works


def extract_collaborations(works, home_inst_id):
    """
    From a list of works, extract pairs (home_inst_id, other_inst_id)
    whenever they co-appear on a paper.
    """
    edges = []

    for w in works:
        # Each work has "authorships" -> each authorship can have "institutions"
        inst_ids = set()
        for auth in w.get("authorships", []):
            for inst in auth.get("institutions", []):
                inst_id = inst.get("id")
                if inst_id:
                    inst_ids.add(inst_id)

        # If our home institution isn’t on the paper, skip
        if home_inst_id not in inst_ids:
            continue

        # Add edges from home_inst to every other inst on that paper
        for other in inst_ids:
            if other == home_inst_id:
                continue
            pair = tuple(sorted([home_inst_id, other]))
            edges.append(pair)

    return edges


def main():
    institutions = load_institutions()
    all_edges = []

    for inst in institutions:
        openalex_id = inst["openalex_id"]
        print(f"[INFO] Processing institution: {inst['name']} ({openalex_id})")
        works = fetch_works_for_institution(openalex_id)
        edges = extract_collaborations(works, openalex_id)
        all_edges.extend(edges)

    # Count edges
    counter = Counter(all_edges)

    # Convert to structured list
    rows = []
    for (inst_a, inst_b), count in counter.items():
        rows.append({
            "institution_a": inst_a,
            "institution_b": inst_b,
            "collab_count": count
        })

    out_path = DATA_DIR / "collaborations.json"
    out_path.write_text(json.dumps(rows, indent=2))
    print(f"[INFO] Saved {len(rows)} collaboration edges to {out_path}")


if __name__ == "__main__":
    main()
