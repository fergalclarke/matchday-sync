"""End-to-end glue, with the network stubbed out."""

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

import enrich_tv
from enrich.extract import TeamListing
from enrich.fetch import FetchError

TODAY = dt.datetime.now(ZoneInfo("Europe/Dublin")).date()


class FakeAirtable:
    """Captures PATCHes instead of sending them."""

    def __init__(self, records):
        self.records = records
        self.patched = []

    def __call__(self, api_key, base_id, table):
        return self

    def list_fixtures(self, date_from, date_to):
        return self.records

    def patch_records(self, updates):
        self.patched.extend(updates)
        return len(updates)


def loi_row(rec_id, team_a, team_b, days_out=2, tv="TBC"):
    return {
        "id": rec_id,
        "fields": {
            "FixtureID": rec_id,
            "Date": (TODAY + dt.timedelta(days=days_out)).isoformat(),
            "Sport": "LoI",
            "TeamA": team_a,
            "TeamB": team_b,
            "TV": tv,
        },
    }


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("AIRTABLE_API_KEY", "pat-test")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.delenv("GITHUB_EVENT_NAME", raising=False)
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    monkeypatch.setattr(enrich_tv.anthropic, "Anthropic", lambda *a, **k: object())
    monkeypatch.setattr(enrich_tv, "fetch_text", lambda *a, **k: "page text")


def install(monkeypatch, records, listings):
    fake = FakeAirtable(records)
    monkeypatch.setattr(enrich_tv, "AirtableClient", fake)
    monkeypatch.setattr(
        enrich_tv,
        "extract_listings",
        lambda client, sport, text, today, end: listings if sport.key == "loi" else [],
    )
    return fake


def test_writes_matches_and_defaults(env, monkeypatch, capsys):
    records = [
        loi_row("A", "Bohemians", "Shamrock Rovers"),
        loi_row("B", "Derry City", "Sligo Rovers"),
    ]
    listings = [
        TeamListing(
            home="Bohemians",
            away="Shamrock Rovers",
            date=(TODAY + dt.timedelta(days=2)).isoformat(),
            time="19:45",
            channel="Virgin Media Two",
        ),
        TeamListing(
            home="St Patrick's Athletic",
            away="Galway United",
            date=(TODAY + dt.timedelta(days=3)).isoformat(),
            time="19:45",
            channel="Virgin Media One",
        ),
        TeamListing(
            home="Shelbourne",
            away="Waterford",
            date=(TODAY + dt.timedelta(days=4)).isoformat(),
            time="19:45",
            channel="Virgin Media Two",
        ),
    ]
    fake = install(monkeypatch, records, listings)

    assert enrich_tv.main(["--sport", "loi"]) == 0

    written = {u["id"]: u["fields"] for u in fake.patched}
    assert written["A"] == {"TV": "vmtwo"}   # matched in source
    assert written["B"] == {"TV": "loitv"}   # absent from source

    summary = capsys.readouterr().out
    assert "TV enrichment" in summary
    assert "vmtwo" in summary


def test_dry_run_writes_nothing(env, monkeypatch):
    records = [loi_row("A", "Bohemians", "Shamrock Rovers")]
    listings = [
        TeamListing(
            home="Bohemians",
            away="Shamrock Rovers",
            date=(TODAY + dt.timedelta(days=2)).isoformat(),
            time="19:45",
            channel="Virgin Media Two",
        ),
        TeamListing(
            home="Shelbourne",
            away="Waterford",
            date=(TODAY + dt.timedelta(days=3)).isoformat(),
            time="19:45",
            channel="Virgin Media One",
        ),
        TeamListing(
            home="Derry City",
            away="Galway United",
            date=(TODAY + dt.timedelta(days=4)).isoformat(),
            time="19:45",
            channel="Virgin Media Two",
        ),
    ]
    fake = install(monkeypatch, records, listings)

    assert enrich_tv.main(["--sport", "loi", "--dry-run"]) == 0
    assert fake.patched == []


def test_failed_source_touches_nothing(env, monkeypatch, capsys):
    """
    The failure that matters: if the fetch dies, every LoI row looks 'absent'
    and would otherwise be stamped loitv.
    """
    records = [loi_row(str(i), f"Team {i}", "Opponent") for i in range(6)]
    fake = install(monkeypatch, records, [])

    def boom(*args, **kwargs):
        raise FetchError("extratime.com -> HTTP 503")

    monkeypatch.setattr(enrich_tv, "fetch_text", boom)

    assert enrich_tv.main(["--sport", "loi"]) == 1
    assert fake.patched == []
    assert "503" in capsys.readouterr().out


def test_thin_extraction_is_treated_as_a_failed_source(env, monkeypatch):
    """min_extractions=3 for LoI, so two listings means the scrape is suspect."""
    records = [loi_row(str(i), f"Team {i}", "Opponent") for i in range(6)]
    listings = [
        TeamListing(
            home="Shelbourne",
            away="Waterford",
            date=(TODAY + dt.timedelta(days=4)).isoformat(),
            time="19:45",
            channel="Virgin Media Two",
        )
    ] * 2
    fake = install(monkeypatch, records, listings)

    assert enrich_tv.main(["--sport", "loi"]) == 1
    assert fake.patched == []


def test_max_default_writes_rail_blocks_a_mass_default(env, monkeypatch, capsys):
    """A source that parses fine but matches nothing must not stamp the world."""
    records = [loi_row(str(i), f"Team {i}", "Opponent") for i in range(20)]
    listings = [
        TeamListing(
            home="Completely Different",
            away="Other Club",
            date=(TODAY + dt.timedelta(days=n)).isoformat(),
            time="19:45",
            channel="Virgin Media Two",
        )
        for n in range(1, 5)
    ]
    fake = install(monkeypatch, records, listings)

    assert enrich_tv.main(["--sport", "loi"]) == 1
    assert fake.patched == []
    assert "max_default_writes" in capsys.readouterr().out


def test_rows_already_resolved_are_not_candidates(env, monkeypatch):
    records = [loi_row("A", "Bohemians", "Shamrock Rovers", tv="vmtwo")]
    fake = install(monkeypatch, records, [])

    assert enrich_tv.main(["--sport", "loi"]) == 0
    assert fake.patched == []
