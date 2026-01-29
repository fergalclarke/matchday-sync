import os
import re
import json
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# =========================
# CONFIG
# =========================

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = "Fixtures"

LGFA_URL = "https://ladiesgaelic.ie/fixtures-results/upcoming-fixtures/"
SPORT_LABEL = "LGFA"


# =========================
# HELPERS
# =========================

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def build_fixture_id(date_str: str, team_a: str, team_b: str) -> str:
    """
    Short, deterministic ID for LGFA fixtures.
    Example: LGFA-20260124-DUBLIN-KERRY (truncated & cleaned)
    """
    base_date = date_str.replace("-", "")  # YYYYMMDD

    def clean(name):
        return re.sub(r"[^A-Za-z0-9]", "", name.upper())[:8] or "TEAM"

    a = clean(team_a)
    b = clean(team_b)
    return f"LGFA-{base_date}-{a}-{b}"


# =========================
# SCRAPE LGFA FIXTURES
# =========================

def fetch_lgfa_html():
    resp = requests.get(LGFA_URL, timeout=15)
    resp.raise_for_status()
    return resp.text


def parse_lgfa_fixtures(html: str):
    """
    Return a list of raw fixture dicts scraped from ladiesgaelic.ie
    """
    soup = BeautifulSoup(html, "html.parser")
    fixtures = []

    for div in soup.select("div.fixture"):
        date_attr = div.get("data-date")  # e.g. "20260124"
        if not date_attr or len(date_attr) != 8:
            continue

        # Convert YYYYMMDD -> YYYY-MM-DD
        dt = datetime.strptime(date_attr, "%Y%m%d")
        date_iso = dt.date().isoformat()  # "2026-01-24"

        # Teams
        team_a_el = div.select_one(".col-lg-5.alignright")
        team_b_el = None
        # any other .col-lg-5 that is not alignright
        for candidate in div.select(".col-lg-5"):
            if "alignright" not in candidate.get("class", []):
                team_b_el = candidate
                break

        if not team_a_el or not team_b_el:
            continue

        team_a = team_a_el.get_text(strip=True)
        team_b = team_b_el.get_text(strip=True)

        # Time
        time_el = div.select_one(".col-lg-2.aligncenter span")
        time_str = time_el.get_text(strip=True) if time_el else ""

        # Venue (from small text: "Referee: X • Venue: Y")
        venue = ""
        small_el = div.find("small")
        if small_el:
            small_text = small_el.get_text(" ", strip=True)
            # Split on bullet and look for "Venue:"
            parts = [p.strip() for p in small_text.split("•")]
            for part in parts:
                if "Venue:" in part:
                    venue = part.split("Venue:", 1)[1].strip()
                    break

        fixtures.append({
            "date": date_iso,      # "2026-01-24"
            "time": time_str,      # "14:45"
            "team_a": team_a,
            "team_b": team_b,
            "venue": venue,
        })

    return fixtures


# =========================
# NORMALISE TO AIRTABLE FIELDS
# =========================

def normalise_lgfa_fixture(raw: dict):
    date_str = raw["date"]
    team_a = raw["team_a"]
    team_b = raw["team_b"]

    fixture_id = build_fixture_id(date_str, team_a, team_b)

    return {
        "FixtureID": fixture_id,
        "Date": date_str,             # ISO 8601 date: "YYYY-MM-DD"
        "Time": raw["time"],          # "14:45" or ""
        "Sport": SPORT_LABEL,         # "LGFA"
        "TeamA": team_a,
        "TeamB": team_b,
        "Venue": raw["venue"],
        "TV": "",                     # leave empty, you’ll override manually
    }


def normalise_all(fixtures_raw):
    return [normalise_lgfa_fixture(fx) for fx in fixtures_raw]


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

    print(f"[INFO] Existing Airtable LGFA records: {len(existing)}")
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
            print(f"[INFO] Created {created} LGFA records")


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
            print(f"[INFO] Updated {updated} LGFA records")


def upsert_to_airtable(records):
    if not records:
        print("[INFO] No LGFA records to upsert.")
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
            # 🟢 On create, set TV to empty; you can later override manually
            to_create.append(r)

    print(f"[INFO] LGFA to create: {len(to_create)}, LGFA to update: {len(to_update)}")

    if to_create:
        airtable_batch_create(to_create)

    if to_update:
        airtable_batch_update(to_update)


# =========================
# MAIN
# =========================

def main():
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        print("[ERROR] AIRTABLE_API_KEY or AIRTABLE_BASE_ID not set.")
        return

    print(f"[INFO] Fetching LGFA fixtures from {LGFA_URL}...")
    html = fetch_lgfa_html()
    raw_fixtures = parse_lgfa_fixtures(html)
    print(f"[INFO] Scraped {len(raw_fixtures)} raw LGFA fixtures")

    if not raw_fixtures:
        print("[INFO] No LGFA fixtures found on page.")
        return

    normalised = normalise_all(raw_fixtures)
    print(f"[INFO] Normalised LGFA fixtures: {len(normalised)}")

    upsert_to_airtable(normalised)
    print("[DONE] LGFA sync complete.")


if __name__ == "__main__":
    main()
