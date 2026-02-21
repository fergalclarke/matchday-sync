import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = "Fixtures"

LOCAL_TZ = ZoneInfo("Europe/Dublin")

# Safety rail: prevent accidental table wipe
MAX_DELETE = int(os.getenv("MAX_AIRTABLE_DELETE", "1000"))  # override in Actions if you want


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def airtable_list_records(filter_by_formula: str):
    """Fetch all records matching a filter, handling pagination."""
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    records = []
    offset = None

    while True:
        params = {"filterByFormula": filter_by_formula}
        if offset:
            params["offset"] = offset

        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"Airtable list failed: {resp.status_code} {resp.text}")

        data = resp.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break

    return records


def airtable_batch_delete(record_ids):
    """Delete records by ID in batches of 10 (Airtable limit)."""
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

    deleted = 0
    for batch in chunked(record_ids, 10):
        # Airtable expects repeated `records[]=` params
        params = [("records[]", rid) for rid in batch]
        resp = requests.delete(url, headers=headers, params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"Airtable delete failed: {resp.status_code} {resp.text}")

        data = resp.json()
        deleted += len(data.get("records", []))

    return deleted


def main():
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        print("[ERROR] AIRTABLE_API_KEY or AIRTABLE_BASE_ID not set.")
        raise SystemExit(1)

    run_date = datetime.now(LOCAL_TZ).date().isoformat()
    print(f"[INFO] Airtable cleanup run date (Europe/Dublin): {run_date}")

    # Option A: delete anything strictly before run_date
    # NOTE: assumes {Date} is a date or ISO string like YYYY-MM-DD
    formula = f"{{Date}} < '{run_date}'"
    print(f"[INFO] Fetching records to delete with filter: {formula}")

    records = airtable_list_records(formula)
    record_ids = [r["id"] for r in records]
    count = len(record_ids)

    print(f"[INFO] Records matched for deletion: {count}")

    if count == 0:
        print("[DONE] Nothing to delete.")
        return

    if count > MAX_DELETE:
        print(f"[ERROR] Refusing to delete {count} records (MAX_DELETE={MAX_DELETE}).")
        print("[ERROR] If this is expected, set MAX_AIRTABLE_DELETE higher.")
        raise SystemExit(1)

    deleted = airtable_batch_delete(record_ids)
    print(f"[DONE] Deleted {deleted} Airtable records older than {run_date}.")


if __name__ == "__main__":
    main()
