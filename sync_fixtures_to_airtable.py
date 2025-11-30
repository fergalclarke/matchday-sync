import os
import time
import requests
from datetime import datetime, timedelta

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo
except ImportError:
    # For older Python versions, install backports.zoneinfo
    from backports.zoneinfo import ZoneInfo


# =========================
# CONFIGURATION
# =========================

# 🔐 Keys – preferably set as environment variables
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")

AIRTABLE_TABLE_NAME = "Fixtures"  # Change if your table name differs

# API-Football via RapidAPI
FOOTBALL_URL = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
FOOTBALL_HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": "api-football-v1.p.rapidapi.com",
}

# Leagues you want to fetch
LEAGUES = [
    {"id": 357, "season": 2025, "sport": "LoI"},  # LOI
    {"id": 39,  "season": 2025, "sport": "EPL"},  # Premier League
    {"id": 2,   "season": 2025, "sport": "UCL"},  # UCL
    {"id": 3,   "season": 2025, "sport": "EL"},  # Europa League
]

# Time config
LOCAL_TZ = ZoneInfo("Europe/Dublin")
DAYS_AHEAD = 30   # fetch next 30 days
DAYS_BEHIND = 3   # and last 3 days (for reschedules)


# =========================
# HELPERS
# =========================

def chunked(iterable, size):
    """Yield successive chunks of given size from iterable."""
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def api_get_with_retry(url, headers=None, params=None, max_retries=3):
    """GET with simple retry on 429 / 5xx."""
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, headers=headers, params=params)
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
# FETCH FROM API-FOOTBALL
# =========================

def fetch_fixtures_for_league(league_id, season, date_from, date_to):
    """Fetch fixtures for a league & date window from API-Football."""
    params = {
        "league": str(league_id),
        "season": str(season),
        "from": date_from,
        "to": date_to,
        "timezone": "331",  # matches your original script
    }

    resp = api_get_with_retry(FOOTBALL_URL, headers=FOOTBALL_HEADERS, params=params)
    if not resp:
        return []

    data = resp.json()
    fixtures = data.get("response", [])
    print(f"[INFO] League {league_id} season {season}: fetched {len(fixtures)} fixtures")
    return fixtures


def fetch_all_fixtures():
    """Fetch fixtures for all configured leagues within our date window."""
    today = datetime.now(tz=LOCAL_TZ).date()
    date_from = (today - timedelta(days=DAYS_BEHIND)).strftime("%Y-%m-%d")
    date_to = (today + timedelta(days=DAYS_AHEAD)).strftime("%Y-%m-%d")

    print(f"[INFO] Fetching fixtures from {date_from} to {date_to}")

    all_fixtures_raw = []
    for league in LEAGUES:
        fixtures = fetch_fixtures_for_league(
            league_id=league["id"],
            season=league["season"],
            date_from=date_from,
            date_to=date_to,
        )
        for fx in fixtures:
            fx["_sport_label"] = league["sport"]
        all_fixtures_raw.extend(fixtures)

    print(f"[INFO] Total fixtures fetched: {len(all_fixtures_raw)}")
    return all_fixtures_raw


# =========================
# NORMALISE FOR AIRTABLE
# =========================

def normalise_fixture(fx):
    """
    Convert raw fixture JSON into Airtable fields:
    FixtureID,Date,Time,Sport,TeamA,TeamB,TV,Venue
    """
    fixture_info = fx.get("fixture", {})
    teams_info = fx.get("teams", {})

    fixture_id = fixture_info.get("id")
    if fixture_id is None:
        return None

    date_str = fixture_info.get("date")
    if not date_str:
        return None

    # Example: 2025-07-27T13:00:00+00:00 or 2025-07-27T13:00:00Z
    dt_utc = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    dt_local = dt_utc.astimezone(LOCAL_TZ)

    # Match your sample format: 27-7-2025 / 14:00
    date_out = dt_local.strftime("%Y-%m-%d")
    time_out = dt_local.strftime("%H:%M")

    sport = fx.get("_sport_label", "Football")
    home_team = teams_info.get("home", {}).get("name", "")
    away_team = teams_info.get("away", {}).get("name", "")
    venue = fixture_info.get("venue", {}).get("name", "")

    fields = {
        "FixtureID": str(fixture_id),
        "Date": date_out,
        "Time": time_out,
        "Sport": sport,
        "TeamA": home_team,
        "TeamB": away_team,
        "TV": "TBC",  # adjust if you later add TV data
        "Venue": venue,
    }
    return fields


def normalise_all(fixtures_raw):
    """Normalise all fixtures and drop any that fail or duplicate."""
    records = []
    seen_ids = set()

    for fx in fixtures_raw:
        fields = normalise_fixture(fx)
        if not fields:
            continue
        fid = fields["FixtureID"]
        if fid in seen_ids:
            continue
        seen_ids.add(fid)
        records.append(fields)

    print(f"[INFO] Normalised fixtures: {len(records)}")
    return records


# =========================
# AIRTABLE UPSERT
# =========================

def airtable_get_existing_ids(fixture_ids):
    """
    Return a dict mapping FixtureID -> Airtable record ID
    for the given fixture_ids.
    """
    existing = {}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    }

    for batch in chunked(list(fixture_ids), 50):
        formula = "OR(" + ",".join(
            [f"{{FixtureID}}='{fid}'" for fid in batch]
        ) + ")"
        params = {"filterByFormula": formula}

        offset = None
        while True:
            if offset:
                params["offset"] = offset

            resp = api_get_with_retry(url, headers=headers, params=params)
            if not resp:
                break

            data = resp.json()
            for rec in data.get("records", []):
                fields = rec.get("fields", {})
                fid = str(fields.get("FixtureID"))
                if fid:
                    existing[fid] = rec["id"]

            offset = data.get("offset")
            if not offset:
                break

    print(f"[INFO] Existing Airtable records found: {len(existing)}")
    return existing


def airtable_batch_create(records):
    """Create records in Airtable in batches of 10."""
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
            print(f"[ERROR] Create failed: {resp.status_code} {resp.text}")
        else:
            created_count = len(resp.json().get("records", []))
            print(f"[INFO] Created {created_count} records")


def airtable_batch_update(records_with_ids):
    """Update records in Airtable in batches of 10."""
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
            print(f"[ERROR] Update failed: {resp.status_code} {resp.text}")
        else:
            updated_count = len(resp.json().get("records", []))
            print(f"[INFO] Updated {updated_count} records")


def upsert_to_airtable(records):
    if not records:
        print("[INFO] No football records to upsert.")
        return

    fixture_ids = {r["FixtureID"] for r in records}
    existing = airtable_get_existing_ids(fixture_ids)

    to_create = []
    to_update = []

    for r in records:
        fid = r["FixtureID"]

        if fid in existing:
            # 🚫 Do not overwrite TV on updates
            update_fields = r.copy()
            update_fields.pop("TV", None)

            to_update.append({
                "id": existing[fid],
                "fields": update_fields
            })
        else:
            # 🟢 New records keep the TV field
            to_create.append(r)

    print(f"[INFO] Football to create: {len(to_create)}, Football to update: {len(to_update)}")

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

    fixtures_raw = fetch_all_fixtures()
    normalised = normalise_all(fixtures_raw)
    upsert_to_airtable(normalised)
    print("[DONE] Sync complete.")


if __name__ == "__main__":
    main()