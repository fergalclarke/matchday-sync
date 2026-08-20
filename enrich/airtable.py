import datetime as dt
import time

import requests

from .match import parse_date

API_ROOT = "https://api.airtable.com/v0"


class AirtableError(RuntimeError):
    pass


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _request_with_retry(method, url, *, headers, params=None, json=None, max_retries=3):
    for attempt in range(1, max_retries + 1):
        resp = requests.request(method, url, headers=headers, params=params, json=json)
        if resp.status_code in (200, 201):
            return resp

        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
            wait = 2**attempt
            print(f"[WARN] {method} {url} -> {resp.status_code}, retrying in {wait}s...")
            time.sleep(wait)
            continue

        raise AirtableError(f"{method} {url} -> {resp.status_code}: {resp.text}")

    raise AirtableError(f"{method} {url} -> exhausted retries")


class AirtableClient:
    def __init__(self, api_key: str, base_id: str, table: str):
        if not api_key:
            raise AirtableError("AIRTABLE_API_KEY is not set")
        if not base_id:
            raise AirtableError("Airtable base ID is not set")

        self.url = f"{API_ROOT}/{base_id}/{table}"
        self.headers = {"Authorization": f"Bearer {api_key}"}

    def list_fixtures(self, date_from: dt.date, date_to: dt.date) -> list[dict]:
        """
        All fixtures in [date_from, date_to], inclusive at both ends.

        Airtable compares its date field against these strings after applying a
        timezone offset, so a row dated exactly `date_to` lands just past
        midnight UTC and falls outside a naive `{Date} <= date_to`. That
        silently dropped the last day of the window on every run. Pad the
        query by a day at each end and do the real bounds check in Python,
        where the comparison is unambiguous.

        Filtering to 'needs work' still happens in Python too -- Sport values
        are inconsistently cased and formula-side string comparison on them is
        more trouble than it's worth.
        """
        lower = (date_from - dt.timedelta(days=1)).isoformat()
        upper = (date_to + dt.timedelta(days=1)).isoformat()
        formula = f"AND({{Date}} >= '{lower}', {{Date}} <= '{upper}')"
        params = {"filterByFormula": formula, "pageSize": 100}

        records: list[dict] = []
        offset = None
        while True:
            if offset:
                params["offset"] = offset

            resp = _request_with_retry("GET", self.url, headers=self.headers, params=params)
            data = resp.json()
            records.extend(data.get("records", []))

            offset = data.get("offset")
            if not offset:
                break

        kept = []
        for record in records:
            row_date = parse_date(record.get("fields", {}).get("Date"))
            if row_date is not None and date_from <= row_date <= date_to:
                kept.append(record)

        padding = len(records) - len(kept)
        print(
            f"[INFO] Airtable: {len(kept)} fixtures in {date_from}..{date_to} "
            f"({padding} outside the window discarded)"
        )
        return kept

    def patch_records(self, updates: list[dict]) -> int:
        """updates: [{'id': recXXX, 'fields': {...}}, ...]"""
        headers = {**self.headers, "Content-Type": "application/json"}
        patched = 0

        for batch in chunked(updates, 10):
            payload = {"records": batch, "typecast": True}
            resp = _request_with_retry(
                "PATCH", self.url, headers=headers, json=payload
            )
            patched += len(resp.json().get("records", []))

        return patched
