import datetime as dt

import pytest

from enrich.config import SourceConfig, SportConfig
from enrich.extract import EventListing, TeamListing
from enrich.match import (
    Outcome, decide, normalise_name, resolve_channel, selectable, similarity,
)

TODAY = dt.date(2026, 8, 7)


def make_source(**overrides) -> SourceConfig:
    base = dict(
        name="testsource",
        url="https://example.test",
        max_chars=1000,
        min_extractions=1,
        channel_map={"Virgin Media Two": "vmtwo", "Virgin Media One": "vmone"},
        channel_patterns=[],
        channel_fallback=None,
        ignore_unmatched_channels=False,
    )
    base.update(overrides)
    return SourceConfig(**base)


def make_sport(**overrides) -> SportConfig:
    sources = overrides.pop("sources", None) or [make_source()]
    base = dict(
        key="loi",
        aliases=["loi", "league of ireland"],
        sources=sources,
        match_strategy="teams",
        writes=["TV"],
        name_aliases={},
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
        sources=[make_source(name="skysports", channel_map={}, channel_fallback="Sky Sports")],
        match_strategy="event_round",
        writes=["TV", "Time"],
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
    d = decide(make_sport(), [(make_sport().sources[0], [team_listing()])], row(), TODAY)
    assert d.outcome is Outcome.WRITE
    assert d.fields == {"TV": "vmtwo"}
    assert d.overwrites == []


def test_already_correct_is_a_no_op():
    d = decide(make_sport(), [(make_sport().sources[0], [team_listing()])], row(TV="vmtwo"), TODAY)
    assert d.outcome is Outcome.NO_CHANGE
    assert d.fields == {}


def test_absent_from_source_gets_the_default():
    other = team_listing(home="Derry City", away="Sligo Rovers")
    d = decide(make_sport(), [(make_sport().sources[0], [other])], row(), TODAY)
    assert d.outcome is Outcome.DEFAULT
    assert d.fields == {"TV": "loitv"}


def test_moved_fixture_is_flagged_not_defaulted():
    """The regression that matters: a date shift must not read as 'not on TV'."""
    moved = team_listing(date="2026-08-10")
    d = decide(make_sport(), [(make_sport().sources[0], [moved])], row(Date="2026-08-09"), TODAY)
    assert d.outcome is Outcome.FLAG
    assert "date mismatch" in d.reason
    assert d.fields == {}


def test_date_shift_beyond_tolerance_falls_through_to_default():
    far = team_listing(date="2026-08-16")
    d = decide(make_sport(), [(make_sport().sources[0], [far])], row(Date="2026-08-09"), TODAY)
    assert d.outcome is Outcome.DEFAULT


def test_unmapped_channel_is_flagged_not_defaulted():
    rte = team_listing(channel="RTÉ2")
    d = decide(make_sport(), [(make_sport().sources[0], [rte])], row(), TODAY)
    assert d.outcome is Outcome.FLAG
    assert "channel_map" in d.reason
    assert d.fields == {}


def test_default_not_applied_beyond_horizon():
    sport = make_sport(default_tv_max_days=5)
    far_out = row(Date=(TODAY + dt.timedelta(days=8)).isoformat())
    d = decide(sport, [(sport.sources[0], [team_listing(home="Derry City", away="Sligo Rovers")])], far_out, TODAY)
    assert d.outcome is Outcome.NO_CHANGE


def test_default_applies_on_the_horizon_boundary():
    boundary = row(Date=(TODAY + dt.timedelta(days=10)).isoformat())
    d = decide(make_sport(), [(make_sport().sources[0], [team_listing(home="Derry", away="Sligo")])], boundary, TODAY)
    assert d.outcome is Outcome.DEFAULT


def test_home_away_swap_still_matches():
    swapped = team_listing(home="Shamrock Rovers", away="Bohemians")
    d = decide(make_sport(), [(make_sport().sources[0], [swapped])], row(), TODAY)
    assert d.outcome is Outcome.WRITE


def test_ambiguous_conflicting_listings_are_flagged():
    a = team_listing(channel="Virgin Media Two")
    b = team_listing(channel="Virgin Media One")
    d = decide(make_sport(), [(make_sport().sources[0], [a, b])], row(), TODAY)
    assert d.outcome is Outcome.FLAG
    assert "disagree" in d.reason


def test_duplicate_listings_that_agree_are_not_ambiguous():
    d = decide(make_sport(), [(make_sport().sources[0], [team_listing(), team_listing()])], row(), TODAY)
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
    d = decide(golf_sport(), [(golf_sport().sources[0], [event_listing()])], golf_row(), TODAY)
    assert d.outcome is Outcome.WRITE
    assert d.fields == {"TV": "Sky Sports", "Time": "14:00"}


def test_golf_takes_the_earliest_of_several_airings():
    early = event_listing(time="09:00", channel="Sky Sports Mix")
    late = event_listing(time="14:00", channel="Sky Sports Golf")
    d = decide(golf_sport(), [(golf_sport().sources[0], [late, early])], golf_row(), TODAY)
    assert d.fields["Time"] == "09:00"      # earliest across all channels
    assert d.fields["TV"] == "Sky Sports"


def test_golf_overwriting_a_real_time_is_recorded():
    existing = golf_row(Time="13:30", TV="Sky Sports")
    d = decide(golf_sport(), [(golf_sport().sources[0], [event_listing()])], existing, TODAY)
    assert d.outcome is Outcome.WRITE
    assert d.fields == {"Time": "14:00"}
    assert d.overwrites == ["Time"]


def test_golf_absent_from_source_changes_nothing():
    """No default_tv for golf, so absence must not clear or invent a value."""
    d = decide(golf_sport(), [(golf_sport().sources[0], [event_listing(event="US PGA")])], golf_row(), TODAY)
    assert d.outcome is Outcome.NO_CHANGE


def test_golf_idempotent_on_repeat_runs():
    settled = golf_row(TV="Sky Sports", Time="14:00")
    d = decide(golf_sport(), [(golf_sport().sources[0], [event_listing()])], settled, TODAY)
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
    d = decide(sport, [(sport.sources[0], [listing])], record, dt.date(2026, 8, 20))
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
        d = decide(sport, [(sport.sources[0], [event_listing(channel=channel)])], golf_row(TV="TBC"), TODAY)
        assert d.outcome is Outcome.WRITE, channel
        assert d.fields["TV"] == "Sky Sports"


def test_loi_still_flags_an_unmapped_channel():
    """The fallback must not leak to LoI, where vmone vs vmtwo is meaningful."""
    d = decide(make_sport(), [(make_sport().sources[0], [team_listing(channel="RTÉ2")])], row(), TODAY)
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
    d = decide(sport, [(sport.sources[0], [day1])], day4, dt.date(2026, 8, 20))
    assert d.outcome is Outcome.NO_CHANGE
    assert "absent" in d.reason


def test_loi_keeps_the_moved_fixture_detection():
    """Tolerance stays non-zero for LoI, where a date shift is a real signal."""
    d = decide(make_sport(), [(make_sport().sources[0], [team_listing(date="2026-08-10")])], row(Date="2026-08-09"), TODAY)
    assert d.outcome is Outcome.FLAG
    assert "date mismatch" in d.reason


# --- EPL: channel families, ignored broadcasters, team aliases --------------


def epl_sport(**overrides) -> SportConfig:
    return make_sport(
        key="epl",
        aliases=["epl", "premier league"],
        sources=[make_source(
            name="footballontv",
            channel_map={},
            channel_patterns=[("sky sports", "Sky Sports"), ("tnt", "tnt")],
            ignore_unmatched_channels=True,
        )],
        default_tv=None,
        default_tv_max_days=None,
        # Built the way config.py builds it, so the test can't drift from
        # production -- note normalise_name strips "Utd"/"United" as noise,
        # so the real key here is "man" -> "manchester".
        name_aliases={
            normalise_name(variant): normalise_name(canonical)
            for variant, canonical in {
                "Man City": "Manchester City",
                "Man Utd": "Manchester United",
                "Tottenham Hotspur": "Tottenham",
                "Coventry City": "Coventry",
                "Ipswich Town": "Ipswich",
            }.items()
        },
        **overrides,
    )


@pytest.mark.parametrize(
    "channel, expected",
    [
        ("Sky Sports Main Event", "Sky Sports"),
        ("Sky Sports Premier League", "Sky Sports"),
        ("Sky Sports Ultra HDR", "Sky Sports"),
        ("Sky Sports TBC", "Sky Sports"),      # rights held, channel unconfirmed
        ("TNT Sports 1", "tnt"),
        ("TNT Sports Ultimate", "tnt"),
    ],
)
def test_epl_channel_families_collapse(channel, expected):
    d = decide(epl_sport(), [(epl_sport().sources[0], [team_listing(channel=channel)])], row(Sport="EPL"), TODAY)
    assert d.outcome is Outcome.WRITE
    assert d.fields == {"TV": expected}


@pytest.mark.parametrize("channel", ["HBO Max", "Amazon Prime", "Premier Sports 1"])
def test_epl_untracked_channels_resolve_to_nothing(channel):
    """These get dropped before matching; nothing is written and nothing flagged."""
    assert resolve_channel(epl_sport().sources[0], channel) is None


@pytest.mark.parametrize(
    "airtable_name, source_name",
    [
        ("Manchester City", "Man City"),
        ("Manchester United", "Man Utd"),
        ("Tottenham", "Tottenham Hotspur"),
        ("Coventry", "Coventry City"),
        ("Ipswich", "Ipswich Town"),
    ],
)
def test_epl_team_aliases_bridge_abbreviations(airtable_name, source_name):
    """Fuzzy matching alone scores Manchester United vs Man Utd at 0.46."""
    sport = epl_sport()
    rec = {"id": "r1", "fields": {"FixtureID": "1", "Date": "2026-08-22",
                                  "Sport": "EPL", "TeamA": airtable_name,
                                  "TeamB": "Arsenal", "TV": "TBC"}}
    listing = TeamListing(home=source_name, away="Arsenal", date="2026-08-22",
                          time="15:00", channel="Sky Sports Main Event")
    d = decide(sport, [(sport.sources[0], [listing])], rec, TODAY)
    assert d.outcome is Outcome.WRITE
    assert d.fields == {"TV": "Sky Sports"}


def test_epl_absent_fixture_is_left_alone():
    """No default for EPL: absence just means no channel we track."""
    other = team_listing(home="Chelsea", away="Fulham")
    d = decide(epl_sport(), [(epl_sport().sources[0], [other])], row(Sport="EPL"), TODAY)
    assert d.outcome is Outcome.NO_CHANGE
    assert d.fields == {}


def test_epl_sky_and_tnt_on_one_fixture_is_flagged():
    """A genuine conflict still needs a human, not a coin toss."""
    sky = team_listing(channel="Sky Sports Main Event")
    tnt = team_listing(channel="TNT Sports 1")
    d = decide(epl_sport(), [(epl_sport().sources[0], [sky, tnt])], row(Sport="EPL"), TODAY)
    assert d.outcome is Outcome.FLAG


def test_epl_repeated_sky_channels_are_not_a_conflict():
    """One fixture on three Sky channels collapses to a single value."""
    listings = [team_listing(channel=c) for c in
                ["Sky Sports Main Event", "Sky Sports Premier League", "Sky Sports Ultra HDR"]]
    sport = epl_sport()
    d = decide(sport, [(sport.sources[0], listings)], row(Sport="EPL"), TODAY)
    assert d.outcome is Outcome.WRITE
    assert d.fields == {"TV": "Sky Sports"}


# --- multiple sources, tried in priority order ------------------------------


def euro_sport(**overrides) -> SportConfig:
    """UCL/EL shape: Virgin Media first, live-footballontv as the fallback."""
    return make_sport(
        key="ucl",
        aliases=["ucl", "cl", "champions league"],
        sources=[
            make_source(
                name="virginmedia",
                channel_map={"Channel one": "vmone", "Channel two": "vmtwo"},
                min_extractions=0,
            ),
            make_source(
                name="footballontv",
                channel_map={},
                channel_patterns=[("tnt", "tnt"), ("sky sports", "Sky Sports"),
                                  ("amazon", "amazon")],
                ignore_unmatched_channels=True,
                min_extractions=0,
            ),
        ],
        default_tv=None,
        default_tv_max_days=None,
        **overrides,
    )


def euro_row(**fields) -> dict:
    base = {"FixtureID": "U1", "Date": "2026-08-09", "Sport": "UCL",
            "TeamA": "Viking", "TeamB": "Dinamo Zagreb", "TV": "TBC"}
    base.update(fields)
    return {"id": "recU1", "fields": base}


def vm_listing(**overrides) -> TeamListing:
    base = dict(home="Viking", away="Dinamo Zagreb", date="2026-08-09",
                time="19:50", channel="Channel two")
    base.update(overrides)
    return TeamListing(**base)


def fotv_listing(**overrides) -> TeamListing:
    base = dict(home="VIking FK", away="Dinamo Zagreb", date="2026-08-09",
                time="20:00", channel="TNT Sports 2")
    base.update(overrides)
    return TeamListing(**base)


def test_virgin_media_wins_when_both_sources_have_it():
    """
    The real case: Viking v Dinamo Zagreb is on Virgin Media Two *and* TNT
    Sports 2. Free-to-air here, so vmtwo must win.
    """
    sport = euro_sport()
    d = decide(sport, [(sport.sources[0], [vm_listing()]),
                       (sport.sources[1], [fotv_listing()])], euro_row(), TODAY)
    assert d.outcome is Outcome.WRITE
    assert d.fields == {"TV": "vmtwo"}


def test_falls_through_to_second_source_when_first_lacks_it():
    sport = euro_sport()
    d = decide(sport, [(sport.sources[0], []),
                       (sport.sources[1], [fotv_listing()])], euro_row(), TODAY)
    assert d.outcome is Outcome.WRITE
    assert d.fields == {"TV": "tnt"}


@pytest.mark.parametrize(
    "channel, expected",
    [("TNT Sports 1", "tnt"), ("TNT Sports 5", "tnt"),
     ("Sky Sports Main Event", "Sky Sports"), ("Amazon Prime Video", "amazon")],
)
def test_fallback_source_channel_rules(channel, expected):
    sport = euro_sport()
    d = decide(sport, [(sport.sources[0], []),
                       (sport.sources[1], [fotv_listing(channel=channel)])],
               euro_row(), TODAY)
    assert d.fields == {"TV": expected}


def test_untracked_channel_on_fallback_leaves_the_row_alone():
    sport = euro_sport()
    d = decide(sport, [(sport.sources[0], []),
                       (sport.sources[1], [fotv_listing(channel="HBO Max")])],
               euro_row(), TODAY)
    # HBO listings are dropped upstream; here the channel simply resolves to
    # nothing, and with no default the row is left untouched.
    assert d.outcome in (Outcome.NO_CHANGE, Outcome.FLAG)
    assert d.fields == {}


def test_absent_from_every_source_writes_nothing():
    sport = euro_sport()
    d = decide(sport, [(sport.sources[0], []), (sport.sources[1], [])],
               euro_row(), TODAY)
    assert d.outcome is Outcome.NO_CHANGE
    assert d.fields == {}


def test_a_flag_from_one_source_survives_another_sources_silence():
    """A date mismatch is a real signal and must not be lost to a later miss."""
    sport = euro_sport()
    moved = vm_listing(date="2026-08-11")
    d = decide(sport, [(sport.sources[0], [moved]), (sport.sources[1], [])],
               euro_row(Date="2026-08-09"), TODAY)
    assert d.outcome is Outcome.FLAG
    assert "date mismatch" in d.reason


def test_already_correct_beats_falling_through():
    sport = euro_sport()
    d = decide(sport, [(sport.sources[0], [vm_listing()]),
                       (sport.sources[1], [fotv_listing()])],
               euro_row(TV="vmtwo"), TODAY)
    assert d.outcome is Outcome.NO_CHANGE
    assert d.fields == {}


def test_european_club_suffixes_are_stripped():
    """'Viking' vs 'VIking FK' scored 0.800 and missed the 0.82 threshold."""
    assert similarity("Viking", "VIking FK") >= 0.82
    assert similarity("Mjallby AIF", "Mjallby") >= 0.82
    assert similarity("Ferencvarosi TC", "Ferencvaros") >= 0.82
