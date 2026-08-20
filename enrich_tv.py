"""
Fill TV (and Time, for golf) on upcoming fixtures in Airtable.

Fetches each sport's broadcast source, extracts listings with Claude, fuzzy-matches
them to Airtable rows in code, and writes back only high-confidence results.
Everything else is reported for review.

    python enrich_tv.py --dry-run
    python enrich_tv.py --sport loi
"""

import argparse
import datetime as dt
import os
import sys
from zoneinfo import ZoneInfo

import anthropic

from enrich.airtable import AirtableClient, AirtableError
from enrich.config import ConfigError, SportConfig, load_config
from enrich.extract import ExtractionError, extract_listings
from enrich.fetch import FetchError, fetch_text
from enrich.match import Decision, Outcome, decide, resolve_channel, selectable
from enrich.report import SportReport, build_summary, emit

LOCAL_TZ = ZoneInfo("Europe/Dublin")
TARGET_LOCAL_HOUR = 9


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do everything except write to Airtable.",
    )
    parser.add_argument("--config", default="enrichment.yaml")
    parser.add_argument(
        "--sport",
        action="append",
        dest="sports",
        help="Only run this sport (repeatable). Defaults to all configured sports.",
    )
    return parser.parse_args(argv)


def should_run_now() -> bool:
    """
    GitHub cron is UTC-only and Ireland shifts twice a year, so the workflow
    fires at both 08:00 and 09:00 UTC and this guard drops whichever run isn't
    09:00 local. Only applies to scheduled runs -- manual and chained runs
    should fire whenever they're triggered.
    """
    if os.getenv("GITHUB_EVENT_NAME") != "schedule":
        return True

    local_hour = dt.datetime.now(LOCAL_TZ).hour
    if local_hour == TARGET_LOCAL_HOUR:
        return True

    print(
        f"[INFO] Local time is {local_hour:02d}:xx Europe/Dublin; "
        f"this run is the off-season duplicate of the {TARGET_LOCAL_HOUR:02d}:00 slot. Exiting."
    )
    return False


def run_sport(
    client: anthropic.Anthropic,
    sport: SportConfig,
    records: list[dict],
    today: dt.date,
    window_end: dt.date,
) -> SportReport:
    report = SportReport(key=sport.key)

    candidates = [r for r in records if selectable(sport, r)]
    report.candidates = len(candidates)
    if not candidates:
        print(f"[INFO] {sport.key}: no candidate fixtures in window, skipping source fetch")
        return report

    try:
        page_text = fetch_text(
            sport.source.url, sport.source.max_chars, sport.request_timeout
        )
        listings = extract_listings(client, sport, page_text, today, window_end)
    except (FetchError, ExtractionError) as exc:
        # A failed source must never reach the matcher. For LoI in particular,
        # "absent from source" is a positive signal, so an empty listing set from
        # a broken fetch would stamp every candidate with the default.
        report.source_ok = False
        report.error = str(exc)
        print(f"[ERROR] {sport.key}: {exc}")
        return report

    if sport.ignore_unmatched_channels:
        # Drop listings on broadcasters this sport doesn't track (HBO Max,
        # Amazon Prime) before matching, so a fixture carried only by one of
        # them reads as "absent" rather than filling the review list. Counted
        # so a pattern that stops matching shows up in the summary.
        kept = [l for l in listings if resolve_channel(sport, l.channel) is not None]
        report.ignored_channels = len(listings) - len(kept)
        if report.ignored_channels:
            print(f"[INFO] {sport.key}: ignored {report.ignored_channels} listings on untracked channels")
        listings = kept

    if len(listings) < sport.source.min_extractions:
        report.source_ok = False
        report.error = (
            f"only {len(listings)} listings extracted, below min_extractions="
            f"{sport.source.min_extractions}"
        )
        print(f"[ERROR] {sport.key}: {report.error}")
        return report

    report.listings = len(listings)
    report.decisions = [decide(sport, record, listings, today) for record in candidates]

    # Safety rail, same shape as MAX_AIRTABLE_DELETE in the cleanup script. This
    # is what catches a source redesign that still yields plausible-looking
    # listings which happen to match nothing.
    defaulted = [d for d in report.decisions if d.outcome == Outcome.DEFAULT]
    if len(defaulted) > sport.max_default_writes:
        report.aborted = (
            f"{len(defaulted)} rows would be defaulted to {sport.default_tv!r}, "
            f"over max_default_writes={sport.max_default_writes}"
        )
        print(f"[ERROR] {sport.key}: {report.aborted} — refusing to write anything for this sport")
        for d in report.decisions:
            if d.outcome in (Outcome.WRITE, Outcome.DEFAULT):
                d.outcome = Outcome.FLAG
                d.reason = f"withheld: {report.aborted}"
                d.fields = {}

    return report


def main(argv=None) -> int:
    args = parse_args(argv)

    if not should_run_now():
        return 0

    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"[ERROR] {exc}")
        return 2

    api_key = os.getenv("AIRTABLE_API_KEY")
    if not api_key:
        print("[ERROR] AIRTABLE_API_KEY is not set.")
        return 2
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("[ERROR] ANTHROPIC_API_KEY is not set.")
        return 2

    sports = config.sports
    if args.sports:
        wanted = {s.lower() for s in args.sports}
        sports = [s for s in sports if s.key.lower() in wanted]
        if not sports:
            print(f"[ERROR] No configured sport matches {sorted(wanted)}")
            return 2

    today = dt.datetime.now(LOCAL_TZ).date()
    window_end = today + dt.timedelta(days=config.window_days)

    try:
        airtable = AirtableClient(api_key, config.base_id, config.table)
        records = airtable.list_fixtures(today.isoformat(), window_end.isoformat())
    except AirtableError as exc:
        print(f"[ERROR] {exc}")
        return 2

    client = anthropic.Anthropic()
    reports = [run_sport(client, sport, records, today, window_end) for sport in sports]

    updates = []
    for report in reports:
        for decision in report.decisions:
            if decision.writable:
                updates.append({"id": decision.record_id, "fields": decision.fields})

    written = 0
    if updates and not args.dry_run:
        try:
            written = airtable.patch_records(updates)
            print(f"[INFO] Patched {written} records")
        except AirtableError as exc:
            print(f"[ERROR] Write failed: {exc}")
            emit(
                build_summary(
                    reports,
                    dry_run=args.dry_run,
                    date_from=today.isoformat(),
                    date_to=window_end.isoformat(),
                    written=0,
                )
            )
            return 1
    elif updates:
        written = sum(len(u["fields"]) for u in updates)

    emit(
        build_summary(
            reports,
            dry_run=args.dry_run,
            date_from=today.isoformat(),
            date_to=window_end.isoformat(),
            written=written,
        )
    )

    attempted = [r for r in reports if r.candidates]
    if attempted and all(not r.source_ok or r.aborted for r in attempted):
        print("[ERROR] Every source with candidate fixtures failed.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
