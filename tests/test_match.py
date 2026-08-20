import datetime as dt

import pytest

from enrich.config import SourceConfig, SportConfig
from enrich.extract import EventListing, TeamListing
from enrich.match import Outcome, decide, normalise_name, selectable, similarity

TODAY = dt.date(2026, 8, 7)


def make_sport(**overrides) -> SportConfig:
    base = dict(
        key="loi",
        aliases=["loi", "league of ireland"],
        source=SourceConfig(url="https://example.test", max_chars=1000, min_extractions=3),
        match_strategy="teams",
        writes=["TV"],
        channel_map={"Virgin Media Two": "vmtwo", "Virgin Media One": "vmone"},
        channel_fallback=None,
        select_all=False,
        select_tv_is=["tbc"],
        default_tv="loitv",
        default_tv_max_days=10,
        tie_break=None,
        model="claude-haiku-4-5",
        max_tokens=8000,
        name_match_threshold=0.82,
        date_tolerance_days=3,
        ambiguity_margin=0.05,
        max_default_writes=12,
        request_timeout=30,
    )
    base.update(overrides)
    return SportConfig(**base)


def golf_sport(**overrides) -> SportConfig:
    return make_sport(
        key="golf",
        aliases=["golf"],
        match_strategy="event_round",
        writes=["TV", "Time"],
        channel_map={},
        channel_fallback="Sky Sports",
        select_all=True,
        select_tv_is=[],
        default_tv=None,
        default_tv_max_days=None,
        tie_break="earliest_time",
        **overrides,
    )


def row(**fields) -> dict:
    base = {
        "FixtureID": "1",
        "Date": "2026-08-09",
        "Sport": "LoI",
        "TeamA": "Bohemians",
        "TeamB": "Shamrock Rovers",
        "TV": "TBC",
    }
    base.update(fields)
    return {"id": "rec123", "fields": base}


def team_listing(**overrides) -> TeamListing:
    base = dict(
        home="Bohemians",
        away="Shamrock Rovers",
        date="2026-08-09",
        time="19:45",
        channel="Virgin Media Two",
    )
    base.update(overrides)
    return TeamListing(**base)


# --- the LoI decision table -------------------------------------------------


def test_exact_match_writes_mapped_slug():
    d = decide(make_sport(), row(), [team_listing()], TODAY)
    assert d.outcome is Outcome.WRITE
    assert d.fields == {"TV": "vmtwo"}
    assert d.overwrites == []


def test_already_correct_is_a_no_op():
    d = decide(make_sport(), row(TV="vmtwo"), [team_listing()], TODAY)
    assert d.outcome is Outcome.NO_CHANGE
    assert d.fields == {}


def test_absent_from_source_gets_the_default():
    other = team_listing(home="Derry City", away="Sligo Rovers")
    d = decide(make_sport(), row(), [other], TODAY)
    assert d.outcome is Outcome.DEFAULT
    assert d.fields == {"TV": "loitv"}


def test_moved_fixture_is_flagged_not_defaulted():
    """The regression that matters: a date shift must not read as 'not on TV'."""
    moved = team_listing(date="2026-08-10")
    d = decide(make_sport(), row(Date="2026-08-09"), [moved], TODAY)
    assert d.outcome is Outcome.FLAG
    assert "date mismatch" in d.reason
    assert d.fields == {}


def test_date_shift_beyond_tolerance_falls_through_to_default():
    far = team_listing(date="2026-08-16")
    d = decide(make_sport(), row(Date="2026-08-09"), [far], TODAY)
    assert d.outcome is Outcome.DEFAULT


def test_unmapped_channel_is_flagged_not_defaulted():
    rte = team_listing(channel="RTÉ2")
    d = decide(make_sport(), row(), [rte], TODAY)
    assert d.outcome is Outcome.FLAG
    assert "channel_map" in d.reason
    assert d.fields == {}


def test_default_not_applied_beyond_horizon():
    sport = make_sport(default_tv_max_days=5)
    far_out = row(Date=(TODAY + dt.timedelta(days=8)).isoformat())
    d = decide(sport, far_out, [team_listing(home="Derry City", away="Sligo Rovers")], TODAY)
    assert d.outcome is Outcome.NO_CHANGE


def test_default_applies_on_the_horizon_boundary():
    boundary = row(Date=(TODAY + dt.timedelta(days=10)).isoformat())
    d = decide(make_sport(), boundary, [team_listing(home="Derry", away="Sligo")], TODAY)
    assert d.outcome is Outcome.DEFAULT


def test_home_away_swap_still_matches():
    swapped = team_listing(home="Shamrock Rovers", away="Bohemians")
    d = decide(make_sport(), row(), [swapped], TODAY)
    assert d.outcome is Outcome.WRITE


def test_ambiguous_conflicting_listings_are_flagged():
    a = team_listing(channel="Virgin Media Two")
    b = team_listing(channel="Virgin Media One")
    d = decide(make_sport(), row(), [a, b], TODAY)
    assert d.outcome is Outcome.FLAG
    assert "disagree" in d.reason


def test_duplicate_listings_that_agree_are_not_ambiguous():
    d = decide(make_sport(), row(), [team_listing(), team_listing()], TODAY)
    assert d.outcome is Outcome.WRITE


# --- golf -------------------------------------------------------------------


def event_listing(**overrides) -> EventListing:
    base = dict(
        event="The Open Championship",
        round="Round 2",
        date="2026-08-09",
        time="14:00",
        channel="Sky Sports Golf",
    )
    base.update(overrides)
    return EventListing(**base)


def golf_row(**fields) -> dict:
    base = {
        "FixtureID": "G1",
        "Date": "2026-08-09",
        "Sport": "Golf",
        "TeamA": "The Open Championship",
        "TeamB": "Round 2",
        "TV": "TBC",
        "Time": "08:00",
    }
    base.update(fields)
    return {"id": "recG1", "fields": base}


def test_golf_writes_tv_and_time():
    d = decide(golf_sport(), golf_row(), [event_listing()], TODAY)
    assert d.outcome is Outcome.WRITE
    assert d.fields == {"TV": "Sky Sports", "Time": "14:00"}


def test_golf_takes_the_earliest_of_several_airings():
    early = event_listing(time="09:00", channel="Sky Sports Mix")
    late = event_listing(time="14:00", channel="Sky Sports Golf")
    d = decide(golf_sport(), golf_row(), [late, early], TODAY)
    assert d.fields["Time"] == "09:00"      # earliest across all channels
    assert d.fields["TV"] == "Sky Sports"


def test_golf_overwriting_a_real_time_is_recorded():
    existing = golf_row(Time="13:30", TV="Sky Sports")
    d = decide(golf_sport(), existing, [event_listing()], TODAY)
    assert d.outcome is Outcome.WRITE
    assert d.fields == {"Time": "14:00"}
    assert d.overwrites == ["Time"]


def test_golf_absent_from_source_changes_nothing():
    """No default_tv for golf, so absence must not clear or invent a value."""
    d = decide(golf_sport(), golf_row(), [event_listing(event="US PGA")], TODAY)
    assert d.outcome is Outcome.NO_CHANGE


def test_golf_idempotent_on_repeat_runs():
    settled = golf_row(TV="Sky Sports", Time="14:00")
    d = decide(golf_sport(), settled, [event_listing()], TODAY)
    assert d.outcome is Outcome.NO_CHANGE
    assert d.fields == {}


# --- selection --------------------------------------------------------------


@pytest.mark.parametrize("sport_value", ["LoI", "loi", "LOI", " League of Ireland "])
def test_sport_matching_is_case_insensitive(sport_value):
    assert selectable(make_sport(), row(Sport=sport_value)) is True


def test_loi_skips_rows_that_already_have_a_channel():
    assert selectable(make_sport(), row(TV="vmtwo")) is False


def test_golf_selects_every_row_regardless_of_tv_or_time():
    sport = golf_sport()
    assert selectable(sport, golf_row(TV="skygolf", Time="13:30")) is True
    assert selectable(sport, golf_row(TV="TBC")) is True


def test_other_sports_are_ignored():
    assert selectable(make_sport(), row(Sport="Rugby")) is False


def test_normalise_name_strips_noise_and_accents():
    assert normalise_name("Bohemian FC") == normalise_name("Bohemian")
    assert normalise_name("Drogheda United") == normalise_name("Drogheda Utd")
    assert "e" in normalise_name("RTÉ")


# --- name matching against padded broadcaster titles ------------------------


@pytest.mark.parametrize(
    "row_name, source_title",
    [
        ("BMW Championship", "Fedex Playoffs BMW Championship Day 1 PGA Tour Golf"),
        ("Tour Championship", "Fedex Playoffs Tour Championship Day 2 PGA Tour Golf"),
        ("Betfred British Masters", "Betfred British Masters Day 1 DP World Tour Golf"),
    ],
)
def test_event_name_matches_through_broadcaster_padding(row_name, source_title):
    """
    Sky pads event names with series prefixes and round suffixes. A plain
    character ratio scores 'BMW Championship' vs the padded title at 0.68 and
    misses it -- which is exactly why golf silently updated nothing.
    """
    assert similarity(row_name, source_title) >= 0.82


@pytest.mark.parametrize(
    "row_name, source_title",
    [
        ("BMW Championship", "Nexo Championship Day 1 DP World Tour Golf"),
        ("BMW Championship", "LPGA Tour: CPKC Women's Open"),
        ("Tour Championship", "Nexo Championship Day 3"),
        ("Betfred British Masters", "Fedex Playoffs BMW Championship Day 1"),
    ],
)
def test_different_events_still_do_not_match(row_name, source_title):
    assert similarity(row_name, source_title) < 0.82


def test_single_shared_word_does_not_carry_a_match():
    """The two-token floor: 'Rovers' alone must not match 'Shamrock Rovers'."""
    assert similarity("Rovers", "Shamrock Rovers") < 0.82


def test_bmw_championship_row_now_resolves():
    """The exact row from the reported miss, end to end through decide()."""
    sport = golf_sport()
    record = {
        "id": "recsUbFUFwhAeCl01",
        "fields": {
            "FixtureID": "141",
            "Date": "2026-08-20",
            "Sport": "Golf",
            "TeamA": "BMW Championship",
            "TeamB": "PGA Tour",
            "TV": "Sky Sports",
            "Time": "08:00",
        },
    }
    listing = EventListing(
        event="Fedex Playoffs BMW Championship",
        round="Day 1",
        date="2026-08-20",
        time="15:15",
        channel="Sky Sports Golf",
    )
    d = decide(sport, record, [listing], dt.date(2026, 8, 20))
    assert d.outcome is Outcome.WRITE
    # TV already reads "Sky Sports", so only the placeholder Time changes.
    assert d.fields == {"Time": "15:15"}
    assert d.overwrites == ["Time"]


def test_golf_accepts_any_sky_channel_including_new_ones():
    """
    Golf stores "Sky Sports" whatever channel carries it. Sky Sports+ was not
    in the original enumerated map, which is why BMW Championship Day 1 would
    have been flagged instead of written.
    """
    sport = golf_sport()
    for channel in ["Sky Sports+", "Sky Sports Golf", "Sky Sports Something New"]:
        d = decide(sport, golf_row(TV="TBC"), [event_listing(channel=channel)], TODAY)
        assert d.outcome is Outcome.WRITE, channel
        assert d.fields["TV"] == "Sky Sports"


def test_loi_still_flags_an_unmapped_channel():
    """The fallback must not leak to LoI, where vmone vs vmtwo is meaningful."""
    d = decide(make_sport(), row(), [team_listing(channel="RTÉ2")], TODAY)
    assert d.outcome is Outcome.FLAG


def test_golf_missing_day_is_absent_not_a_date_mismatch():
    """
    A tournament runs the same event name on consecutive days. With a
    non-zero date tolerance, a day the source hasn't listed yet near-matches
    a sibling round and reports a bogus date mismatch, so golf uses 0.
    """
    sport = golf_sport(date_tolerance_days=0)
    day4 = golf_row(Date="2026-08-23")
    day1 = event_listing(event="Fedex Playoffs BMW Championship", date="2026-08-20")
    d = decide(sport, day4, [day1], dt.date(2026, 8, 20))
    assert d.outcome is Outcome.NO_CHANGE
    assert "absent" in d.reason


def test_loi_keeps_the_moved_fixture_detection():
    """Tolerance stays non-zero for LoI, where a date shift is a real signal."""
    d = decide(make_sport(), row(Date="2026-08-09"), [team_listing(date="2026-08-10")], TODAY)
    assert d.outcome is Outcome.FLAG
    assert "date mismatch" in d.reason
