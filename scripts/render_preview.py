"""Render a repo-native SVG timeline from the recorded slow-drain evidence."""

import json
from html import escape
from pathlib import Path

root = Path(__file__).resolve().parents[1]
c = json.loads((root / "src/undertow/web/demos/slow-drain.json").read_text())
a = c["analysis"]
span = max(1, a["end"] - a["start"])


def x(t):
    return 230 + (t - a["start"]) / span * 900


def text(xx, y, size, value, fill="#172627", weight="400", font="sans-serif"):
    return f'<text x="{xx}" y="{y}" font-family="{font}" font-size="{size}" fill="{fill}" font-weight="{weight}">{escape(str(value))}</text>'


parts = [
    '<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="530" viewBox="0 0 1200 530" role="img" aria-labelledby="title desc"><title id="title">Undertow recorded consumer timeline</title><desc id="desc">Real local Kafka capture, with observed assignment and revocation intervals for three consumers.</desc><rect width="1200" height="530" rx="16" fill="#f5f4ef"/><rect x="24" y="24" width="1152" height="482" rx="10" fill="#fff" stroke="#d9ded7"/>'
]
parts += [
    text(52, 68, 26, "≈ undertow", weight="700"),
    text(830, 65, 11, "RECORDED LOCAL EXECUTION", fill="#187d70", font="monospace"),
    text(52, 122, 30, c["title"], weight="500"),
    text(
        52,
        151,
        12,
        "Apache Kafka 4.1.0 · Arroyo 2.46.0 · synthetic workload",
        fill="#677271",
    ),
]
for i in range(6):
    xx = 230 + i * 180
    parts.append(f'<line x1="{xx}" x2="{xx}" y1="195" y2="380" stroke="#e5eade"/>')
    parts.append(
        text(
            xx - 8, 184, 10, f"{span * i / 5000:.2f}s", fill="#889480", font="monospace"
        )
    )
for i, lane in enumerate(a["consumers"]):
    y = 215 + i * 55
    parts.append(text(52, y + 20, 13, lane.rsplit(" / ", 1)[-1], font="monospace"))
    for ep in a["epochs"]:
        if ep["consumer"] == lane:
            parts.append(
                f'<rect x="{x(ep["start"]):.2f}" y="{y}" width="{max(2, x(ep["end"]) - x(ep["start"])):.2f}" height="28" rx="4" fill="#dbe8ce" opacity=".7"/>'
            )
    for w in a["revocations"]:
        if w["consumer"] == lane:
            parts.append(
                f'<rect x="{x(w["start"]):.2f}" y="{y - 3}" width="{max(2, x(w["end"]) - x(w["start"])):.2f}" height="34" rx="4" fill="#ead4b7"/>'
            )
    for event in c["events"]:
        if event["consumer"] == lane and event["kind"] in [
            "assign",
            "revoke",
            "revoked",
            "stuck",
        ]:
            color = (
                "#be524d"
                if event["kind"] == "stuck"
                else "#187d70"
                if event["kind"] == "assign"
                else "#af804d"
            )
            parts.append(
                f'<circle cx="{x(event["ts"]):.2f}" cy="{y + 14}" r="4" fill="{color}" stroke="#fff"/>'
            )
duration = max(
    f["end"] - f["start"] for f in a["findings"] if f["code"] == "strategy_drain"
)
parts += [
    '<line x1="52" x2="1128" y1="399" y2="399" stroke="#d9ded7"/>',
    text(52, 433, 11, "OBSERVED STRATEGY JOIN", fill="#677271", font="monospace"),
    text(52, 472, 28, f"{duration / 1000:.3f}s", weight="600"),
    text(300, 433, 11, "SOURCE-LEVEL EVIDENCE", fill="#677271", font="monospace"),
    text(300, 465, 13, "Native logs + explicitly labeled instrumentation"),
    text(790, 433, 11, "INTERPRETATION", fill="#677271", font="monospace"),
    text(790, 465, 13, "An observed callback, not global stability."),
    "</svg>",
]
(root / "docs/preview.svg").write_text("\n".join(parts))
print("Rendered evidence-derived README preview.")
