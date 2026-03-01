"""
main.py — Entry point for the AI News Aggregator.

Collects articles from Anthropic blogs and videos from YouTube channels
published in the last 24 hours.
"""

from runner import Runner


def main() -> None:
    runner = Runner(
        hours=100,                  # look back this many hours (default: 24)
        fetch_content=True,       # set True to download full article text via Docling
        fetch_transcripts=True,    # set True to download YouTube transcripts
    )
    report = runner.run()

    # `report` dict is available for downstream use (e.g. pass to an agent)
    # Keys: generated_at, hours, anthropic, youtube


if __name__ == "__main__":
    main()
