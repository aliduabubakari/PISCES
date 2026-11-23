"""Export sanitized saved summaries for the frontend; never run a workload."""

import argparse
import json
from pathlib import Path

from pisces.report_views import load_report
from pisces.report_views.campaigns import build_campaign_view


def build(sessions: Path, suite: Path):
    report = load_report(sessions.read_bytes())
    for session in report["sessions"]:
        session.pop("directory", None)
    suite_report = load_report(suite.read_bytes())
    return [
        {
            "filename": "saved-scaling-sessions.json",
            "report": report,
            "view": build_campaign_view(report),
        },
        {
            "filename": "saved-suite-summary.json",
            "report": suite_report,
            "view": build_campaign_view(suite_report),
        },
    ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sessions", type=Path)
    parser.add_argument("suite", type=Path)
    parser.add_argument(
        "--out", type=Path, default=Path("frontend/src/test/fixtures/campaigns.json")
    )
    args = parser.parse_args()
    args.out.write_text(
        json.dumps(build(args.sessions, args.suite), indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
