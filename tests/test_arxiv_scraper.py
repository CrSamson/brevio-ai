r"""
tests/test_arxiv_scraper.py - unit tests for ArxivScraper.

Plain runnable script. Drives parsing via ._parse_atom_bytes() so no HTTP
or sleep mocking is needed for parse-side tests.

Covers:
  1. Parses a real arXiv Atom fixture into Paper objects.
  2. Every parsed arxiv_id matches the strict regex ^\d{4}\.\d{4,5}$.
  3. Keyword filter narrows results and every retained paper contains the keyword.
  4. Volume gate returns [] when entry count exceeds the configured limit.
  5. Rate-limit helper invokes time.sleep when a call lands within MIN_INTERVAL_S.

Run:
    python tests/test_arxiv_scraper.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.arxiv_scraper import ArxivScraper
from scrapers.schemas import Paper


FIXTURE = Path(__file__).parent / "fixtures" / "arxiv_cs_lg.xml"

# Strict pattern (anchored). The scraper extracts via \b...\b which is
# non-anchored; the strict version verifies the captured group is clean.
ARXIV_ID_STRICT = re.compile(r"^\d{4}\.\d{4,5}$")


def _make(**overrides) -> ArxivScraper:
    cfg = {
        "id":             "arxiv_test",
        "type":           "arxiv",
        "categories":     ["cs.LG", "cs.AI"],
        "max_results":    10,
        "keyword_filter": None,
    }
    cfg.update(overrides)
    return ArxivScraper(cfg)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_parses_correctly() -> None:
    raw   = FIXTURE.read_bytes()
    items = _make()._parse_atom_bytes(raw)

    assert len(items) > 0, "expected >=1 paper"
    p = items[0]
    assert isinstance(p, Paper)
    assert p.sources == ["arxiv"]
    assert p.arxiv_id, "arxiv_id must be set"
    assert p.url
    assert p.pdf_url
    assert p.title
    assert p.authors, "expected authors list"
    assert p.abstract is not None
    assert p.categories, "expected at least one category"
    assert p.published_at is not None
    assert p.hf_upvotes is None      # not set by arxiv path
    print(f"  ok - parsed {len(items)} papers; first arxiv_id={p.arxiv_id}")


def test_arxiv_id_regex_matches_all() -> None:
    raw   = FIXTURE.read_bytes()
    items = _make()._parse_atom_bytes(raw)

    bad = [p.arxiv_id for p in items if not ARXIV_ID_STRICT.match(p.arxiv_id or "")]
    assert not bad, f"non-conforming arxiv_ids: {bad}"
    print(f"  ok - all {len(items)} arxiv_ids match {ARXIV_ID_STRICT.pattern}")


def test_keyword_filter_narrows_and_matches() -> None:
    raw       = FIXTURE.read_bytes()
    no_filter = _make(keyword_filter=None)._parse_atom_bytes(raw)
    assert no_filter, "fixture must have papers"

    # Pick a keyword guaranteed to be in at least one paper: a substantive
    # word from the first paper's title. Avoids brittle keyword choices.
    candidate = next(
        (w for w in no_filter[0].title.split() if len(w) >= 5 and w.isalpha()),
        no_filter[0].title.split()[0],
    ).lower()

    filtered = _make(keyword_filter=[candidate])._parse_atom_bytes(raw)
    assert 1 <= len(filtered) <= len(no_filter)
    for p in filtered:
        haystack = (p.title + " " + (p.abstract or "")).lower()
        assert candidate in haystack, f"{candidate!r} not in {p.title!r}"
    print(f"  ok - no filter={len(no_filter)}, filter[{candidate!r}]={len(filtered)}")


def test_volume_gate_returns_empty_when_tripped() -> None:
    import scrapers.arxiv_scraper as mod
    raw = FIXTURE.read_bytes()
    original = mod.VOLUME_GATE
    try:
        mod.VOLUME_GATE = 1   # force the gate to trip on any non-empty fixture
        items = _make()._parse_atom_bytes(raw)
        assert items == [], "volume gate should return [] when tripped"
    finally:
        mod.VOLUME_GATE = original
    print("  ok - volume gate returns [] when entry count exceeds limit")


def test_rate_limit_invokes_sleep() -> None:
    import scrapers.arxiv_scraper as mod
    sleep_calls: list[float] = []
    real_sleep = mod.time.sleep
    mod.time.sleep = lambda s: sleep_calls.append(s)
    try:
        # Mark a call as having "just happened" so the next call must wait.
        mod._last_call_at = mod.time.monotonic()
        mod._wait_for_rate_limit()
    finally:
        mod.time.sleep = real_sleep

    assert sleep_calls, "expected time.sleep to be invoked"
    assert 0.0 < sleep_calls[0] <= mod.MIN_INTERVAL_S
    print(f"  ok - rate limit asked for ~{sleep_calls[0]:.2f}s sleep")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

TESTS = [
    test_parses_correctly,
    test_arxiv_id_regex_matches_all,
    test_keyword_filter_narrows_and_matches,
    test_volume_gate_returns_empty_when_tripped,
    test_rate_limit_invokes_sleep,
]


def main() -> int:
    if not FIXTURE.exists():
        print(f"FAIL: fixture missing - {FIXTURE}", file=sys.stderr)
        return 1

    failed = 0
    for fn in TESTS:
        print(f"{fn.__name__} ...")
        try:
            fn()
        except AssertionError as e:
            failed += 1
            print(f"  FAIL: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  ERROR: {type(e).__name__}: {e}")

    print()
    if failed:
        print(f"{failed} test(s) failed.")
        return 1
    print(f"All {len(TESTS)} tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
