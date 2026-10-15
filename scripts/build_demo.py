"""Prepare presentation cases from recorded evidence and labeled controls."""

import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from undertow.core import analyze, ingest

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "src/undertow/web"
DEMOS = WEB / "demos"


def main():
    slow = json.loads((DEMOS / "slow-drain.json").read_text())
    drain = next(
        f
        for f in slow["analysis"]["findings"]
        if f["code"] == "strategy_drain" and f["end"] - f["start"] > 2900
    )
    # This is a prefix of real records, not newly invented events.
    events = [e for e in slow["events"] if e["ts"] <= drain["start"] + 1000]
    truncated = analyze(
        events,
        "An incomplete capture",
        provenance={
            "type": "truncated-recorded-capture",
            "description": "Prefix of the real slow-drain run, deliberately cut before the join completes. A negative control for incomplete evidence.",
        },
    )
    (DEMOS / "truncated.json").write_text(json.dumps(truncated, separators=(",", ":")))
    native = analyze(
        [e for e in slow["events"] if e["evidence"] == "log"],
        "Native logs: what remains unknown",
        provenance={
            "type": "native-only-recorded-capture",
            "description": "The real slow-drain capture with lab instrumentation removed. Processing and successful broker-commit timing must remain unknown.",
        },
    )
    assert (
        not native["analysis"]["coverage"]["processed"]
        and not native["analysis"]["coverage"]["confirmed_commits"]
    )
    (DEMOS / "native-only.json").write_text(json.dumps(native, separators=(",", ":")))
    # 64 consumers, 64 partitions. Explicitly synthetic scale/visual control.
    sources = []
    for i in range(64):
        rows = []

        def row(ms, message, extension=None):
            from datetime import datetime, timezone

            obj = {
                "timestamp": datetime.fromtimestamp(
                    (1792065600000 + ms) / 1000, timezone.utc
                ).isoformat(timespec="milliseconds"),
                "consumer": f"worker-{i:02d}",
                "group_id": "synthetic-64",
                "message": message,
                "logger": "arroyo.processing.processor",
            }
            if extension:
                obj["undertow"] = extension
            rows.append(obj)

        part = f"Partition(topic=Topic(name='synthetic-events'), index={i})"
        row(i * 10, "Starting")
        row(200 + i * 15, f"New partitions assigned: {{{part}: 0}}")
        for j in range(12):
            row(
                400 + i * 15 + j * 650,
                "Synthetic progress marker",
                {
                    "kind": "processed",
                    "partitions": [
                        {"topic": "synthetic-events", "partition": i, "offset": j + 1}
                    ],
                },
            )
        row(9000 + i * 5, f"Partitions to revoke: [{part}]")
        row(9001 + i * 5, "Waiting for <synthetic strategy> to exit...")
        delay = 6000 if i == 17 else 20 + i * 3
        row(9001 + i * 5 + delay, "<synthetic strategy> exited successfully")
        row(9002 + i * 5 + delay, "Partition revocation complete.")
        row(9500 + i * 5 + delay, f"New partitions assigned: {{{part}: 12}}")
        sources.append(
            {
                "name": f"worker-{i:02d}.jsonl",
                "content": "\n".join(json.dumps(r) for r in rows),
            }
        )
    scale = ingest(sources, "64 consumers, one slow handoff")
    scale["provenance"] = {
        "type": "synthetic-scale-control",
        "description": "Generated 64-consumer fixture for navigation and parser scale. No real Kafka execution is represented by this case.",
    }
    (DEMOS / "scale-64.json").write_text(json.dumps(scale, separators=(",", ":")))
    index = [
        {"label": label, "path": f"demos/{key}.json"}
        for key, label in [
            ("slow-drain", "01 · The slow strategy join"),
            ("baseline", "02 · Healthy rebalance"),
            ("crash", "03 · Crash and replacement"),
            ("auto", "04 · Auto-commit ambiguity"),
            ("native-only", "05 · Native logs only"),
            ("truncated", "06 · Incomplete evidence"),
            ("scale-64", "07 · 64-consumer scale control"),
        ]
    ]
    (DEMOS / "index.json").write_text(json.dumps(index, indent=2))
    # Isolated Sites source contains presentation assets only, never private notes.
    dist = ROOT / "site/dist"
    dist.mkdir(parents=True, exist_ok=True)
    for filename in ["index.html", "styles.css", "app.js", "engine.js"]:
        shutil.copyfile(WEB / filename, dist / filename)
    shutil.copytree(DEMOS, dist / "demos", dirs_exist_ok=True)
    print("Prepared seven cases and static presentation assets.")


if __name__ == "__main__":
    main()
