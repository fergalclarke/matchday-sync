import pytest
import yaml

from enrich.config import ConfigError, load_config

MINIMAL = {
    "airtable": {"base_id": "appTest", "table": "Fixtures"},
    "defaults": {"window_days": 10},
    "sports": {
        "loi": {
            "aliases": ["loi"],
            "source": {"url": "https://example.test", "min_extractions": 3},
            "match_strategy": "teams",
            "select": {"tv_is": ["TBC"]},
            "writes": ["TV"],
            "channel_map": {"Virgin Media Two": "vmtwo"},
            "default_tv": "loitv",
            "default_tv_max_days": 10,
        }
    },
}


def write(tmp_path, data):
    path = tmp_path / "enrichment.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def test_shipped_config_loads():
    config = load_config("enrichment.yaml")
    assert config.base_id == "appqvIWkUegyDEI9l"
    assert config.window_days == 10
    assert {s.key for s in config.sports} == {"loi", "golf"}


def test_shipped_config_golf_selects_everything():
    golf = load_config("enrichment.yaml").sport_for("Golf")
    assert golf.select_all is True
    assert golf.writes == ["TV", "Time"]
    assert golf.tie_break == "earliest_time"
    assert golf.default_tv is None


def test_shipped_config_loi_horizon():
    """
    The horizon is capped by what the Virgin Media guide can see (today + 6),
    not by the 10-day Airtable window. Raising it above the source's coverage
    would default fixtures that are merely off the end of the schedule.
    """
    config = load_config("enrichment.yaml")
    loi = config.sport_for("LoI")
    assert loi.default_tv == "loitv"
    assert loi.default_tv_max_days == 6
    assert loi.default_tv_max_days < config.window_days
    assert loi.channel_map == {"Channel one": "vmone", "Channel two": "vmtwo"}


def test_shipped_config_loi_has_a_layout_hint():
    loi = load_config("enrichment.yaml").sport_for("LoI")
    assert "Channel one" in loi.source.hint
    assert loi.source.max_chars >= 90000  # page flattens to ~87k chars


def test_env_overrides_base_id(monkeypatch, tmp_path):
    monkeypatch.setenv("AIRTABLE_BASE_ID", "appFromEnv")
    assert load_config(write(tmp_path, MINIMAL)).base_id == "appFromEnv"


def test_sport_lookup_is_case_insensitive(tmp_path):
    config = load_config(write(tmp_path, MINIMAL))
    assert config.sport_for("LoI").key == "loi"
    assert config.sport_for("nope") is None


def test_default_tv_without_horizon_is_rejected(tmp_path):
    """A default with no horizon would one-way-door far-out fixtures."""
    data = yaml.safe_load(yaml.safe_dump(MINIMAL))
    del data["sports"]["loi"]["default_tv_max_days"]
    with pytest.raises(ConfigError, match="default_tv_max_days"):
        load_config(write(tmp_path, data))


def test_unknown_strategy_is_rejected(tmp_path):
    data = yaml.safe_load(yaml.safe_dump(MINIMAL))
    data["sports"]["loi"]["match_strategy"] = "vibes"
    with pytest.raises(ConfigError, match="match_strategy"):
        load_config(write(tmp_path, data))


def test_bad_select_is_rejected(tmp_path):
    data = yaml.safe_load(yaml.safe_dump(MINIMAL))
    data["sports"]["loi"]["select"] = "some"
    with pytest.raises(ConfigError, match="select"):
        load_config(write(tmp_path, data))


def test_missing_file_is_an_error():
    with pytest.raises(ConfigError, match="not found"):
        load_config("does-not-exist.yaml")
