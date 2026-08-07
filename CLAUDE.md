# Matchday v2

Syncs fixtures for four sports (football, rugby, GAA, LGFA) from various
sources into a single Airtable **Fixtures** table, on a weekly GitHub Actions
schedule.

## Scripts

| Script | Source | What it does |
|---|---|---|
| `sync_fixtures_to_airtable.py` | API-Football via RapidAPI | Pulls fixtures for 4 competitions (LoI, EPL, UCL, Europa League) in a window of 3 days back / 30 days ahead, converts to `Europe/Dublin` local time, upserts into Airtable by `FixtureID`. |
| `sync_rugby_to_airtable.py` | Rugby Live Data via RapidAPI | Pulls 3 hardcoded competition/season endpoints (International, Champions Cup, URC), filters to next 30 days, same upsert pattern. |
| `gaa_data/gaa_data/gaa_scrape/gaa_scrape/spiders/gaa_spider.py` | Scrapy spider scraping gaa.ie/fixtures-results | Not an Airtable script — scrapes the live GAA fixtures page and emits raw match JSON (`FixtureID`, `Date`, `Time`, `Sport`, teams, `Venue`, `TV`) to `matches.json`. |
| `sync_gaa_to_airtable.py` | `gaa_data/gaa_data/gaa_scrape/matches.json` (spider output) | Reads that JSON, maps competition name → `Gaelic`/`Hurling`/`GAA`, normalises `TV` strings into Airtable codes (`TG4`, `rte2`, `gaaplus`, else `TBC`), prefixes `FixtureID` with `GAA-`, drops anything dated before today, upserts. |
| `sync_lgfa_to_airtable.py` | Scrapes ladiesgaelic.ie directly with `requests` + `BeautifulSoup` (no Scrapy) | Builds a deterministic `FixtureID` from date+team names, upserts. **Currently not run** — its workflow step is commented out in `sync.yml`. |
| `cleanup_old_fixtures_airtable.py` | Airtable itself | Deletes any Airtable record where `Date` is before today (Dublin time), with a safety cap (`MAX_AIRTABLE_DELETE`, default 1000) to avoid an accidental full-table wipe. |
| `enrich_tv.py` + `enrich/` | Virgin Media TV guide (LoI), skysports.com (golf) | Separate daily stage — fills `TV` (and `Time`, for golf) on fixtures in the next 10 days. See "TV enrichment stage" below. |
| `sync_all_sports.py` / `run_all_sports.sh` | — | Legacy **local-only** runners with hardcoded absolute paths (`/Users/fergalclarke/matchday v2`) — not used by CI, presumably for running the sync manually from a laptop/cron. Stale/unmaintained. |

All the Airtable-writing scripts share the same shape: fetch/load → normalise
into `{FixtureID, Date, Time, Sport, TeamA, TeamB, TV, Venue}` → look up
existing records by `FixtureID` via `filterByFormula` → batch-create new /
batch-update existing (in batches of 10, Airtable's limit) → **never
overwrite `TV` on update**, since it's treated as manually curated once set
(only populated on create).

## GitHub Actions (`.github/workflows/sync.yml`)

- **Schedule**: `cron: "0 9 * * 4"` — every Thursday 09:00 UTC.
- **Manual**: `workflow_dispatch` — can also be run on demand from the
  Actions tab.
- Single job, ubuntu-latest. Steps run in order: install deps → set env vars
  → football sync → rugby sync → run the GAA Scrapy spider
  (`scrapy crawl gaa_matches -O matches.json`) → GAA sync → cleanup old
  fixtures. LGFA is present in the file but commented out.

## Airtable auth

Simple bearer-token auth, no OAuth. Two secrets, `AIRTABLE_API_KEY` and
`AIRTABLE_BASE_ID`, live in the repo's GitHub Actions secrets and are
exported into `$GITHUB_ENV` in one step near the top of the job:

```yaml
echo "AIRTABLE_API_KEY=${{ secrets.AIRTABLE_API_KEY }}" >> $GITHUB_ENV
echo "AIRTABLE_BASE_ID=${{ secrets.AIRTABLE_BASE_ID }}" >> $GITHUB_ENV
```

Because `$GITHUB_ENV` writes persist for the rest of the job, every later
step (football/rugby/GAA sync) picks these up automatically without needing
`env:` blocks — except the cleanup step, which redundantly re-declares them
under its own `env:` (harmless, just belt-and-braces). Each Python script
reads them via `os.getenv(...)` and sends `Authorization: Bearer
<AIRTABLE_API_KEY>` on every request to
`https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/Fixtures`. `RAPIDAPI_KEY`
follows the same pattern for the football/rugby fetches, sent as
`x-rapidapi-key`.

## TV enrichment stage

Separate from the weekly sync: `.github/workflows/enrich-tv.yml` runs daily at
09:00 Europe/Dublin, and again whenever **Matchday Fixture Sync** completes
successfully. All behaviour is driven by `enrichment.yaml`.

Flow: select candidate rows from Airtable → fetch the sport's source page →
reduce to visible text → extract listings with Claude (`claude-haiku-4-5`,
structured JSON) → fuzzy-match to rows **in code** → write only high-confidence
results → report everything to the Actions job summary.

Per-sport rules live in `enrichment.yaml`:

- **LoI** — candidates are rows with `TV = "TBC"`. The source lists *terrestrial*
  coverage only, so **absence from it is a positive signal**: a clean no-match
  writes `loitv`. Matched fixtures get `vmtwo`/`vmone`.
  The source is Virgin Media's **accessible** TV guide
  (`/access-services-tv-guide`), not `/tv-guide`. The main guide renders one day
  only, hides the channel in an HTML attribute, and pages between days with JS
  (no href, no date param). The accessible one is server-rendered, covers 7
  days, and carries dates, times and channel headings as plain text.
  It only sees 7 days, which is why `default_tv_max_days` is **6**, below the
  10-day Airtable window — a fixture further out is missing because it is off
  the end of the schedule, not because it is off TV. **Never raise
  `default_tv_max_days` above the source's actual coverage.**
- **Golf** — candidates are *every* Golf row in the window, regardless of current
  `TV`/`Time`. Sky is authoritative, so it overwrites; writes are diff-checked so
  repeat runs are no-ops, and any overwrite of a non-`TBC` value is reported
  under its own heading.

Three things are load-bearing and easy to break:

1. **The near-match pass in `enrich/match.py` must run before anything is
   declared absent.** A fixture whose TV pick moved dates fails the exact-date
   check; without the ±3-day pass it would fall through to "not in source" and be
   silently stamped `loitv`.
2. **A failed source must never reach the matcher.** Since absence means
   `loitv` for LoI, an empty listing set from a broken fetch would stamp every
   candidate. Guarded by fetch health checks and `min_extractions`.
3. **`max_default_writes`** caps how many rows one run may default, mirroring
   `MAX_AIRTABLE_DELETE` in the cleanup script. This is what catches a source
   redesign that still yields plausible listings matching nothing.

The existing sync scripts never overwrite `TV` on update, so enrichment writes
survive the weekly run.

Tests: `python -m pytest tests/` — no network or API key needed.

## Adding a new enrichment job

The cleanest slot is **after the sport-specific syncs land data in Airtable
but before cleanup**, as its own script (e.g. `enrich_fixtures_airtable.py`):

1. Follow the existing shape: query Airtable for records needing enrichment
   (e.g. `filterByFormula` on a marker field, or just fixtures in the
   upcoming window), call your enrichment source, `PATCH` back in batches of
   10.
2. Add it as a new step in `sync.yml` between the `GAA sync` and `Cleanup old
   fixtures` steps — enrichment should run after all fixtures for the week
   exist, but before old ones are pruned, so it isn't wasted on rows about to
   be deleted.
3. Reuse the `AIRTABLE_API_KEY`/`AIRTABLE_BASE_ID` env vars already exported
   to `$GITHUB_ENV` — no new auth plumbing needed. If it needs a new
   external API key, add it as a repo secret and export it the same way the
   other keys are.
4. If enrichment is expensive/rate-limited, scope it by filtering to
   fixtures with `Date >= today` (same pattern `cleanup_old_fixtures_airtable.py`
   uses) rather than the whole table.

## Known rough edges

- `sync_all_sports.py` and `run_all_sports.sh` are unused by CI and hardcode
  a local Mac path (`/Users/fergalclarke/matchday v2`) that doesn't match
  this checkout — stale, safe to ignore or delete.
- LGFA sync exists and works standalone but is disabled in the workflow.
