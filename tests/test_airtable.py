"""Window bounds on the Airtable query, with HTTP stubbed."""

import datetime as dt

import pytest

from enrich import airtable as at

FROM = dt.date(2026, 8, 20)
TO = dt.date(2026, 8, 30)


def record(rec_id, date):
    return {"id": rec_id, "fields": {"FixtureID": rec_id, "Date": date}}


class FakeResponse:
    def __init__(self, payload):
        self.status_code = 200
        self._payload = payload

    def json(self):
        return self._payload


@pytest.fixture
def captured(monkeypatch):
    """Capture the filterByFormula the client sends, and control what comes back."""
    box = {"params": None, "returns": []}

    def fake_request(method, url, *, headers, params=None, json=None, max_retries=3):
        box["params"] = params
        return FakeResponse({"records": box["returns"]})

    monkeypatch.setattr(at, "_request_with_retry", fake_request)
    return box


def client():
    return at.AirtableClient("pat-test", "appTest", "Fixtures")


def test_query_is_padded_by_a_day_at_each_end(captured):
    """
    Airtable applies a timezone offset when comparing its date field against
    these strings, so a row dated exactly date_to fell outside a naive
    `{Date} <= date_to` and the last day of the window was silently skipped.
    """
    client().list_fixtures(FROM, TO)
    formula = captured["params"]["filterByFormula"]
    assert "2026-08-19" in formula   # date_from - 1
    assert "2026-08-31" in formula   # date_to + 1


def test_both_window_ends_are_returned(captured):
    captured["returns"] = [
        record("first", "2026-08-20"),   # exactly date_from
        record("middle", "2026-08-25"),
        record("last", "2026-08-30"),    # exactly date_to -- the one that was lost
    ]
    got = {r["id"] for r in client().list_fixtures(FROM, TO)}
    assert got == {"first", "middle", "last"}


def test_padding_rows_are_discarded(captured):
    """The padding must not leak fixtures from outside the window."""
    captured["returns"] = [
        record("before", "2026-08-19"),
        record("inside", "2026-08-22"),
        record("after", "2026-08-31"),
    ]
    got = {r["id"] for r in client().list_fixtures(FROM, TO)}
    assert got == {"inside"}


def test_datetime_values_and_junk_dates(captured):
    captured["returns"] = [
        record("iso_datetime", "2026-08-30T00:00:00.000Z"),
        record("unparseable", "30-8-2026"),
        record("missing", None),
    ]
    got = {r["id"] for r in client().list_fixtures(FROM, TO)}
    assert got == {"iso_datetime"}


def test_missing_credentials_raise():
    with pytest.raises(at.AirtableError, match="AIRTABLE_API_KEY"):
        at.AirtableClient("", "appTest", "Fixtures")
    with pytest.raises(at.AirtableError, match="base ID"):
        at.AirtableClient("pat-test", "", "Fixtures")
