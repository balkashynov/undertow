"""Bounded parser scale check. Synthetic input; not a Kafka benchmark."""

import hashlib
import json
import platform
import resource
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from undertow.core import ingest

root = Path(__file__).resolve().parents[1]
lines = []
for i in range(100_000):
    t = datetime.fromtimestamp(1792065600 + i / 1000, timezone.utc).isoformat(
        timespec="milliseconds"
    )
    lines.append(
        json.dumps(
            {
                "timestamp": t,
                "consumer": f"worker-{i % 64:02d}",
                "message": "Native observation",
            },
            separators=(",", ":"),
        )
    )
source = "\n".join(lines)
begin = time.perf_counter()
case = ingest(
    [{"name": "scale.jsonl", "content": source}], "100,000-line synthetic scale check"
)
elapsed = time.perf_counter() - begin
assert len(case["events"]) == 100_000 and len(case["analysis"]["consumers"]) == 64
assert not case["diagnostics"]
result = {
    "type": "synthetic-parser-scale",
    "records": 100000,
    "consumers": 64,
    "input_bytes": len(source.encode()),
    "input_sha256": hashlib.sha256(source.encode()).hexdigest(),
    "parse_and_analyze_seconds": round(elapsed, 4),
    "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    * (1 if sys.platform == "darwin" else 1024),
    "python": platform.python_version(),
    "platform": platform.system() + " " + platform.machine(),
    "limits": "Single warm local process; includes synthetic input construction in process RSS, excludes browser rendering; not a Kafka throughput result.",
}
(root / "docs/evidence/scale.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result))
