import os
import time
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# =========================
# CONFIGURATION
# =========================

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
AIRTABLE_TABLE_NAME = "Fixtures"

RUGBY_HOST = "rugby-live-data.p.rapidapi.com"

LOCAL_TZ = ZoneInfo("Europe/Dublin")


# Rugby competitions you want to fetch
RUGBY_ENDPOINTS = [
    ("/fixtures/30/2025", "Rugby"),     # International
    ("/fixtures/1464/2026", "Rugby"),   # Champions Cup
    ("/fixtures/1236/2026", "Rugby"),   # URC
]


# =========================
# HELPERS
# =========================

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def api_get_with_retry(url, headers=None, max_retries=3):
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            return resp

        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
            wait = 2 ** attempt
            print(f"[WARN] {url} -> {resp.status_code}, retrying in {wait}s...")
            time.sleep(wait)
            continue

        print(f"[ERROR] {url} -> {resp.status_code}: {resp.text}")
        resp.raise_for_status()
    return None


# =========================
# FETCH RUGBY FIXTURES
# =========================

def fetch_rugby_fixtures(endpoint, sport_label):
    url = f"https://{RUGBY_HOST}{endpoint}"
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": RUGBY_HOST,
    }

    resp = api_get_with_retry(url, headers=headers)
    if not resp:
        return []

    json_data = resp.json()
    fixtures = json_data.get("results", [])
    print(f"[INFO] {endpoint}: fetched {len(fixtures)} fixtures")

    # attach sport label
    for fx in fixtures:
        fx["_sport_label"] = sport_label

    return fixtures


def fetch_all_rugby():
    all_fixtures = []
    for endpoint, sport_label in RUGBY_ENDPOINTS:
        all_fixtures.extend(fetch_rugby_fixtures(endpoint, sport_label))

    print(f"[INFO] Total rugby fixtures fetched: {len(all_fixtures)}")
    return all_fixtures


# =========================
# NORMALISE FOR AIRTABLE
# =========================

def normalise_rugby_fixture(fx):
    fixture_id = fx.get("id")
    date_str = fx.get("date")

    if not fixture_id or not date_str:
        return None

    # Convert to local time (Ireland)
    dt_utc = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")
    dt_local = dt_utc.astimezone(LOCAL_TZ)

    # Only fixtures for the next 30 days
    today = datetime.now(LOCAL_TZ).date()
    if not (today <= dt_local.date() <= today + timedelta(days=30)):
        return None

    # ISO format required by Airtable
    date_out = dt_local.strftime("%Y-%m-%d")
    time_out = dt_local.strftime("%H:%M")

    sport = fx.get("_sport_label", "Rugby")
    home = fx.get("home", "")
    away = fx.get("away", "")
    venue = fx.get("venue", "") or "Unknown Venue"
    tv = "TBC"

    return {
        "FixtureID": str(fixture_id),
        "Date": date_out,
        "Time": time_out,
        "Sport": sport,
        "TeamA": home,
        "TeamB": away,
        "TV": tv,
        "Venue": venue,
    }


def normalise_all(fixtures_raw):
    normalised = []
    seen = set()

    for fx in fixtures_raw:
        fields = normalise_rugby_fixture(fx)
        if not fields:
            continue

        fid = fields["FixtureID"]
        if fid in seen:
            continue

        seen.add(fid)
        normalised.append(fields)

    print(f"[INFO] Normalised rugby fixtures: {len(normalised)}")
    return normalised


# =========================
# AIRTABLE UPSERT LOGIC
# =========================

def airtable_get_existing_ids(fixture_ids):
    existing = {}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    for batch in chunked(list(fixture_ids), 50):
        formula = "OR(" + ",".join([f"{{FixtureID}}='{fid}'" for fid in batch]) + ")"
        params = {"filterByFormula": formula}

        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            print("[WARN] Airtable lookup error:", resp.text)
            continue

        data = resp.json()
        for rec in data.get("records", []):
            fid = str(rec["fields"].get("FixtureID"))
            if fid:
                existing[fid] = rec["id"]

    print(f"[INFO] Existing Airtable rugby records: {len(existing)}")
    return existing


def airtable_create(records):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }

    for batch in chunked(records, 10):
        payload = {"records": [{"fields": r} for r in batch], "typecast": True}
        requests.post(url, headers=headers, json=payload)


def airtable_update(records):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }

    for batch in chunked(records, 10):
        payload = {"records": batch, "typecast": True}
        requests.patch(url, headers=headers, json=payload)


def upsert_to_airtable(records):
    if not records:
        print("[INFO] No rugby records to upsert.")
        return

    fixture_ids = {r["FixtureID"] for r in records}
    existing = airtable_get_existing_ids(fixture_ids)

    to_create = []
    to_update = []

    for r in records:
        fid = r["FixtureID"]

        if fid in existing:
            # 🚫 Prevent TV from being overwritten on update
            update_fields = r.copy()
            update_fields.pop("TV", None)

            to_update.append({
                "id": existing[fid],
                "fields": update_fields
            })
        else:
            # 🟢 Allow TV on create
            to_create.append(r)

    print(f"[INFO] Rugby to create: {len(to_create)}, Rugby to update: {len(to_update)}")

    if to_create:
        airtable_batch_create(to_create)

    if to_update:
        airtable_batch_update(to_update)

# =========================
# MAIN
# =========================

def main():
    if not AIRTABLE_API_KEY:
        print("[ERROR] AIRTABLE_API_KEY is not set.")
        return

    if not RAPIDAPI_KEY:
        print("[ERROR] RAPIDAPI_KEY is not set.")
        return

    raw = fetch_all_rugby()
    normalised = normalise_all(raw)
    upsert_to_airtable(normalised)
    print("[DONE] Rugby sync complete.")


if __name__ == "__main__":
    main()