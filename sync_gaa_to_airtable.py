import os
import json
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

# =========================
# CONFIG
# =========================

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = "Fixtures"

LOCAL_TZ = ZoneInfo("Europe/Dublin")
BASE_DIR = Path(__file__).parent
GAA_JSON_FILE = BASE_DIR / "gaa_data" / "gaa_data" / "gaa_scrape" / "matches.json"

# Sport label for all GAA fixtures
GAA_SPORT_LABEL = "GAA"


# =========================
# HELPERS
# =========================

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


# =========================
# LOAD & NORMALISE JSON
# =========================

def load_gaa_fixtures_from_json(path: Path):
    path = Path(path)

    if not path.exists():
        print(f"[ERROR] JSON file not found: {path}")
        return []

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        data = [data]

    print(f"[INFO] Loaded {len(data)} GAA fixtures from {path}")
    return data


def normalise_gaa_fixture(raw: dict):
    """
    raw already matches the JSON from the GAA spider:
    FixtureID, Date, Time, Sport, TeamA, TeamB, Venue, TV
    """

    fixture_id = raw.get("FixtureID")
    date_str = raw.get("Date")

    if not fixture_id or not date_str:
        return None

    # Trim ISO datetime -> date (YYYY-MM-DD)
    if "T" in date_str:
        date_out = date_str.split("T", 1)[0]
    else:
        date_out = date_str

    time_out = raw.get("Time") or ""

    # Map long competition name -> Gaelic / Hurling / GAA
    sport_src = raw.get("Sport") or ""
    group = sport_src.lower()

    if "football" in group:
        sport = "Gaelic"
    elif "hurling" in group:
        sport = "Hurling"
    else:
        sport = "GAA"

    team_a = raw.get("TeamA") or ""
    team_b = raw.get("TeamB") or ""
    venue = raw.get("Venue") or ""
    tv = raw.get("TV") or ""

    return {
        "FixtureID": f"GAA-{fixture_id}",  # Prefix to keep IDs unique
        "Date": date_out,
        "Time": time_out,
        "Sport": sport,
        "TeamA": team_a,
        "TeamB": team_b,
        "TV": tv,
        "Venue": venue,
    }


def normalise_all(fixtures_raw):
    return [
        f for f in (normalise_gaa_fixture(r) for r in fixtures_raw)
        if f
    ]




def normalise_all(fixtures_raw):
    return [
        f for f in (normalise_gaa_fixture(r) for r in fixtures_raw)
        if f
    ]



# =========================
# AIRTABLE UPSERT
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
            fields = rec.get("fields", {})
            fid = fields.get("FixtureID")
            if fid:
                existing[str(fid)] = rec["id"]

    print(f"[INFO] Existing Airtable GAA records: {len(existing)}")
    return existing


def airtable_batch_create(records):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }

    for batch in chunked(records, 10):
        payload = {
            "records": [{"fields": r} for r in batch],
            "typecast": True,
        }
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            print("[ERROR] Create failed:", resp.status_code, resp.text)
        else:
            created = len(resp.json().get("records", []))
            print(f"[INFO] Created {created} GAA records")


def airtable_batch_update(records_with_ids):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }

    for batch in chunked(records_with_ids, 10):
        payload = {
            "records": batch,
            "typecast": True,
        }
        resp = requests.patch(url, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            print("[ERROR] Update failed:", resp.status_code, resp.text)
        else:
            updated = len(resp.json().get("records", []))
            print(f"[INFO] Updated {updated} GAA records")


def upsert_to_airtable(records):
    if not records:
        print("[INFO] No GAA records to upsert.")
        return

    fixture_ids = {r["FixtureID"] for r in records}
    existing = airtable_get_existing_ids(fixture_ids)

    to_create = []
    to_update = []

    for r in records:
        fid = r["FixtureID"]

        if fid in existing:
            # 🚫 Do not update the TV field (manually maintained)
            update_fields = r.copy()
            update_fields.pop("TV", None)

            to_update.append({
                "id": existing[fid],
                "fields": update_fields
            })
        else:
            # 🟢 Keep TV on create (scraped from GAA.ie)
            to_create.append(r)

    print(f"[INFO] GAA to create: {len(to_create)}, GAA to update: {len(to_update)}")

    if to_create:
        airtable_batch_create(to_create)

    if to_update:
        airtable_batch_update(to_update)

# =========================
# MAIN
# =========================

def main():
    print(f"GAA JSON Path: {GAA_JSON_FILE}")

    raw_fixtures = load_gaa_fixtures_from_json(GAA_JSON_FILE)
    if not raw_fixtures:
        print("[INFO] No GAA fixtures to normalise.")
        return

    normalised = normalise_all(raw_fixtures)
    print(f"[INFO] Normalised GAA fixtures: {len(normalised)}")

    if not normalised:
        print("[INFO] No GAA records to upsert.")
        return

    # 🔐 Sanity check that Airtable env vars are set
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        print("[ERROR] AIRTABLE_API_KEY or AIRTABLE_BASE_ID not set.")
        return

    # 🚀 Actually write to Airtable
    upsert_to_airtable(normalised)
    print("[DONE] GAA sync complete.")


if __name__ == "__main__":
    main()
