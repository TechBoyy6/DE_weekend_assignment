import requests, os, time, json
import pandas as pd
from pathlib import Path

base_url = os.environ.get("API_BASE_URL", "http://localhost:8000") + "/api/v1/"
username = os.environ.get("API_USERNAME")
password = os.environ.get("API_PASSWORD")


BRONZE_DIR = Path("submission/output/bronze")
MANIFEST_PATH = BRONZE_DIR / "_manifest.json"
PAGE_SIZE = 1000
DATE_FILTERED = {"/orders", "/order_items"}

def get_auth_token():
    url = base_url + "auth/token"
    response = requests.post(url, json={"username": username, "password": password})
    return response.json().get("access_token")

# ------------------------------------------------------------------
# Part 1 — Data Ingestion & Resilience
# ------------------------------------------------------------------


def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {}


def save_manifest(manifest: dict):
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))


def manifest_key(endpoint: str, date_from: str = None, date_to: str = None) -> str:
    """Unique key that distinguishes different date-range runs of the same endpoint."""
    if date_from and date_to:
        return f"{endpoint}:{date_from}:{date_to}"
    return endpoint


def completed_pages(manifest: dict, key: str) -> set:
    return set(manifest.get(key, {}).get("pages", []))


def mark_page_done(manifest: dict, key: str, page: int):
    entry = manifest.setdefault(key, {"pages": []})
    if page not in entry["pages"]:
        entry["pages"].append(page)
    save_manifest(manifest)


def fetch_page(endpoint: str, page: int, params: dict) -> list | None:
    """Fetch a single page; handles rate-limits, token expiry, and retries."""
    global auth_token
    url = base_url + "data" + endpoint
    headers = {"Authorization": f"Bearer {auth_token}"}
    query = {**params, "page": page, "page_size": PAGE_SIZE}

    for attempt in range(6):
        response = requests.get(url, headers=headers, params=query)

        if response.status_code == 200:
            return response.json()["data"]

        if response.status_code in (429, 500):
            wait = 5 * (2 ** attempt)
            print(f"  Rate-limited on page {page}. Waiting {wait}s...")
            time.sleep(wait)
            continue

        if response.status_code == 401:
            print("  Token expired. Re-authenticating...")
            auth_token = get_auth_token()
            headers["Authorization"] = f"Bearer {auth_token}"
            continue

        print(f"  Failed {endpoint} page {page}: HTTP {response.status_code}")
        return None

    return None


def ingest_endpoint(endpoint: str, manifest: dict, date_from: str = None, date_to: str = None):
    key = manifest_key(endpoint, date_from, date_to)
    done = completed_pages(manifest, key)

    filepath = BRONZE_DIR / f"{endpoint.strip('/')}.csv"
    params = {}
    if date_from and date_to:
        params = {"date_from": date_from, "date_to": date_to}

    page = 1
    total_appended = 0

    while True:
        if page in done:
            print(f"  Page {page} already ingested — skipping")
            page += 1
            continue

        data = fetch_page(endpoint, page, params)

        if not data:
            # Empty page means we've gone past the last page
            break

        df = pd.DataFrame(data)
        df["_ingested_at"] = pd.Timestamp.now()
        df["_source_endpoint"] = endpoint

        write_header = not filepath.exists()
        df.to_csv(filepath, mode="a", header=write_header, index=False)

        mark_page_done(manifest, key, page)
        total_appended += len(df)
        print(f"  Page {page}: appended {len(df)} rows")
        page += 1

    print(f"Done {endpoint} — {total_appended} new rows appended")


def ingest_data():
    endpoints = [
        "/orders",
        "/order_items",
        "/customers",
        "/products",
        "/sellers",
        "/payments",
    ]

    auth_token = get_auth_token()
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest()

    # Date range to ingest for date-filtered endpoints.
    # Adjust or accept as CLI args / env vars as needed.
    DATE_FROM = os.environ.get("DATE_FROM", "2018-07-01")
    DATE_TO   = os.environ.get("DATE_TO",   pd.Timestamp.now().strftime("%Y-%m-%d"))

    for endpoint in endpoints:
        print(f"\nIngesting {endpoint}...")
        if endpoint in DATE_FILTERED:
            ingest_endpoint(endpoint, manifest, date_from=DATE_FROM, date_to=DATE_TO)
        else:
            ingest_endpoint(endpoint, manifest)
