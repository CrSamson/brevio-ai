"""
tools/compare_extractors.py — Phase 3a A/B: Docling vs trafilatura.

For each URL in URLS, runs both extractors, times them, and writes the raw
markdown output to tools/extractor_comparison/<slug>.{docling,trafilatura}.md
for eyeball review. Emits a markdown comparison table to stdout.

Acceptance bar (from the plan):
  - Trafilatura succeeds (returns non-empty markdown) on >=8/10 URLs.
  - For URLs where both succeed, trafilatura output length >=50% of Docling's.
  - Manual eyeball on 3 URLs.

Run:
    python tools/compare_extractors.py
"""
from __future__ import annotations

import re
import sys
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# 10 URLs: 4 already-in-DB sources, 5 new candidate sources, 1 deliberate 404.
URLS: list[tuple[str, str]] = [
    # existing in DB - representative sources
    ("anthropic_news",     "https://www.anthropic.com/news/claude-for-creative-work"),
    ("openai_news",        "https://openai.com/index/advanced-account-security"),
    ("aws_ml",             "https://aws.amazon.com/blogs/machine-learning/reinforcement-fine-tuning-with-llm-as-a-judge/"),
    ("techcrunch_ai",      "https://techcrunch.com/2026/04/30/chatgpt-images-2-0-is-a-hit-in-india-but-not-a-big-winner-elsewhere-yet/"),

    # new candidate sources - real article each
    ("wired",              "https://www.wired.com/review/tovala-oven-meal-kit-review-2026/"),
    ("the_verge",          "https://www.theverge.com/gadgets/922288/native-union-anker-2-in-1-usb-c-cable-mothers-day-sale-deal"),
    ("cnbc",               "https://www.cnbc.com/2026/05/01/spirit-airlines-trump-bailout.html"),
    ("phys_org",           "https://phys.org/news/2026-04-fields-midwest-spur-farm-solutions.html"),
    ("forbes_paywallish",  "https://www.forbes.com/sites/hughmcintyre/2026/05/02/taylor-swifts-surprise-album-arrives-at-a-chart-milestone/"),

    # deliberate 404 - should fail in both extractors
    ("404_edge_case",      "https://www.cnbc.com/this-page-definitely-does-not-exist-12345/"),
]

OUTPUT_DIR = Path(__file__).parent / "extractor_comparison"
OUTPUT_DIR.mkdir(exist_ok=True)


@dataclass
class ExtractResult:
    label: str
    url: str
    extractor: str
    success: bool
    length: int
    runtime_s: float
    title_in_body: Optional[bool]
    paragraph_count: int
    error: str = ""

    @property
    def md_path(self) -> Path:
        slug = re.sub(r"[^a-z0-9]+", "_", self.label.lower()).strip("_")
        return OUTPUT_DIR / f"{slug}.{self.extractor}.md"


def _detect_title_in_body(content: str, title_hint: str) -> Optional[bool]:
    """Trafilatura strips the title; Docling embeds it. We don't require either,
    just record. Without the article's true title we can't actually answer this
    deterministically - so just check for any H1/H2 in the markdown as a proxy."""
    if not content:
        return None
    return bool(re.search(r"^#{1,2}\s+\S", content, flags=re.MULTILINE))


def _count_paragraphs(content: str) -> int:
    if not content:
        return 0
    # crude: count blocks separated by blank lines
    return sum(1 for chunk in re.split(r"\n\s*\n", content) if chunk.strip())


def extract_docling(url: str) -> tuple[bool, str, float, str]:
    t0 = time.monotonic()
    try:
        from docling.document_converter import DocumentConverter
        converter = DocumentConverter()
        result = converter.convert(url)
        md = result.document.export_to_markdown()
        return True, md or "", time.monotonic() - t0, ""
    except Exception as e:  # noqa: BLE001
        return False, "", time.monotonic() - t0, f"{type(e).__name__}: {e}"


def extract_trafilatura(url: str) -> tuple[bool, str, float, str]:
    t0 = time.monotonic()
    try:
        import trafilatura
        html = trafilatura.fetch_url(url)
        if not html:
            return False, "", time.monotonic() - t0, "fetch_url returned None"
        md = trafilatura.extract(html, output_format="markdown", include_links=False)
        if not md:
            return False, "", time.monotonic() - t0, "extract returned None"
        return True, md, time.monotonic() - t0, ""
    except Exception as e:  # noqa: BLE001
        return False, "", time.monotonic() - t0, f"{type(e).__name__}: {e}"


def run_one(label: str, url: str) -> tuple[ExtractResult, ExtractResult]:
    print(f"\n{label}  {url}")

    print("  docling      ...", end="", flush=True)
    ok_d, md_d, rt_d, err_d = extract_docling(url)
    print(f" {'ok' if ok_d else 'FAIL':>4}  {len(md_d):>6} chars  {rt_d:5.2f}s  {err_d[:60]}")

    print("  trafilatura  ...", end="", flush=True)
    ok_t, md_t, rt_t, err_t = extract_trafilatura(url)
    print(f" {'ok' if ok_t else 'FAIL':>4}  {len(md_t):>6} chars  {rt_t:5.2f}s  {err_t[:60]}")

    res_d = ExtractResult(label=label, url=url, extractor="docling",
                          success=ok_d, length=len(md_d), runtime_s=rt_d,
                          title_in_body=_detect_title_in_body(md_d, ""),
                          paragraph_count=_count_paragraphs(md_d), error=err_d)
    res_t = ExtractResult(label=label, url=url, extractor="trafilatura",
                          success=ok_t, length=len(md_t), runtime_s=rt_t,
                          title_in_body=_detect_title_in_body(md_t, ""),
                          paragraph_count=_count_paragraphs(md_t), error=err_t)

    # Save outputs for eyeball review
    if ok_d:
        res_d.md_path.write_text(md_d, encoding="utf-8")
    if ok_t:
        res_t.md_path.write_text(md_t, encoding="utf-8")
    return res_d, res_t


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

    print("# Phase 3a — Docling vs trafilatura A/B comparison\n")
    print(f"Output directory: {OUTPUT_DIR.relative_to(Path.cwd())}\n")

    results: list[tuple[ExtractResult, ExtractResult]] = []
    for label, url in URLS:
        results.append(run_one(label, url))

    # Summary table
    print("\n## Comparison table\n")
    print("| Source | Docling len | Trafil. len | Δ (T/D) | Docling time | Trafil. time | Trafil. ok? | Has H1/H2? (T) | Paragraphs (T) |")
    print("|---|---:|---:|---:|---:|---:|---|---|---:|")
    for d, t in results:
        delta = f"{(t.length / d.length * 100):>5.0f}%" if d.success and t.success and d.length > 0 else "—"
        d_len = f"{d.length}" if d.success else "FAIL"
        t_len = f"{t.length}" if t.success else "FAIL"
        d_time = f"{d.runtime_s:.1f}s"
        t_time = f"{t.runtime_s:.1f}s"
        t_ok = "ok" if t.success else f"FAIL ({t.error[:40]})"
        t_has_heading = "yes" if t.title_in_body else "no" if t.title_in_body is False else "—"
        t_paras = t.paragraph_count if t.success else "—"
        print(f"| `{d.label}` | {d_len} | {t_len} | {delta} | {d_time} | {t_time} | {t_ok} | {t_has_heading} | {t_paras} |")

    # Acceptance gate
    print("\n## Acceptance gate\n")

    trafil_success = sum(1 for _, t in results if t.success)
    print(f"- Trafilatura succeeded on **{trafil_success}/{len(results)}** URLs (plan: >=8/10).  "
          f"{'PASS' if trafil_success >= 8 else 'STOP'}")

    both_ok = [(d, t) for d, t in results if d.success and t.success and d.length > 0]
    short_count = sum(1 for d, t in both_ok if t.length < d.length * 0.50)
    print(f"- Of {len(both_ok)} URLs where both succeeded, trafilatura was <50% length on **{short_count}** "
          f"(plan: ideally 0).")
    if short_count:
        print("  Short cases:")
        for d, t in both_ok:
            if t.length < d.length * 0.50:
                pct = (t.length / d.length) * 100 if d.length else 0
                print(f"    - `{d.label}`  trafil={t.length}  docling={d.length}  ({pct:.0f}%)")

    overall_pass = trafil_success >= 8 and short_count == 0
    print(f"\n- Overall: **{'PASS' if overall_pass else 'INVESTIGATE'}**")
    print("\nReview the per-URL files in `tools/extractor_comparison/` for the "
          "manual eyeball check (3 URLs minimum) before approving the swap in 3b.")

    return 0 if overall_pass else 1


if __name__ == "__main__":
    sys.exit(main())
