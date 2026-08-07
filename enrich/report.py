import os
from dataclasses import dataclass, field

from .match import Decision, Outcome


@dataclass
class SportReport:
    key: str
    candidates: int = 0
    listings: int = 0
    source_ok: bool = True
    error: str = ""
    aborted: str = ""
    decisions: list[Decision] = field(default_factory=list)

    @property
    def matched(self) -> int:
        return sum(1 for d in self.decisions if d.outcome == Outcome.WRITE)

    @property
    def defaulted(self) -> int:
        return sum(1 for d in self.decisions if d.outcome == Outcome.DEFAULT)

    @property
    def flagged(self) -> list[Decision]:
        return [d for d in self.decisions if d.outcome == Outcome.FLAG]


def _table(header: list[str], rows: list[list[str]]) -> list[str]:
    if not rows:
        return []
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    lines.append("")
    return lines


def build_summary(
    reports: list[SportReport], *, dry_run: bool, date_from: str, date_to: str, written: int
) -> str:
    mode = "DRY RUN — nothing written" if dry_run else "LIVE"
    lines = [
        "# TV enrichment",
        "",
        f"**Mode:** {mode}  ",
        f"**Window:** {date_from} → {date_to}  ",
        f"**Fields written:** {written}",
        "",
    ]

    # Source health first -- a bad scrape is the failure mode that matters most,
    # and the match rate is the tell.
    health_rows = []
    for r in reports:
        if r.aborted:
            status = f"⛔ aborted — {r.aborted}"
        elif not r.source_ok:
            status = f"❌ failed — {r.error}"
        else:
            status = "✅ ok"
        health_rows.append(
            [r.key, status, r.listings, r.candidates, r.matched, r.defaulted, len(r.flagged)]
        )

    lines += ["## Sources", ""]
    lines += _table(
        ["sport", "status", "listings", "candidates", "matched", "defaulted", "review"],
        health_rows,
    )

    changes, overwrites, reviews = [], [], []
    for r in reports:
        for d in r.decisions:
            if d.outcome in (Outcome.WRITE, Outcome.DEFAULT):
                for key, value in d.fields.items():
                    old = d.previous.get(key)
                    row = [
                        d.sport_key,
                        d.date,
                        d.label,
                        key,
                        f"`{old if old not in (None, '') else '—'}` → `{value}`",
                        d.reason,
                    ]
                    (overwrites if key in d.overwrites else changes).append(row)
            elif d.outcome == Outcome.FLAG:
                reviews.append([d.sport_key, d.date, d.label, d.reason, d.source_note])

    if changes:
        lines += ["## Changes", ""]
        lines += _table(
            ["sport", "date", "fixture", "field", "change", "why"], changes
        )

    if overwrites:
        lines += [
            "## Overwrote existing values",
            "",
            "These replaced a value that was already set (not `TBC`). If one of "
            "these was a deliberate manual edit, it has been reverted.",
            "",
        ]
        lines += _table(
            ["sport", "date", "fixture", "field", "change", "why"], overwrites
        )

    if reviews:
        lines += [
            "## Needs review",
            "",
            "Nothing was written for these.",
            "",
        ]
        lines += _table(["sport", "date", "fixture", "reason", "source said"], reviews)

    if not changes and not overwrites and not reviews:
        if any(not r.source_ok or r.aborted for r in reports):
            lines += ["No changes — see the source failures above.", ""]
        else:
            lines += ["Nothing to do — every candidate fixture was already correct.", ""]

    return "\n".join(lines)


def emit(summary: str) -> None:
    print(summary)
    path = os.getenv("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(summary + "\n")
