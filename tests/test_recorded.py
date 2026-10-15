"""Check provenance and recomputation against the shipped real captures."""

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from undertow.core import ingest, validate_bundle

ROOT = Path(__file__).parents[1]


@pytest.mark.parametrize("name", ["baseline", "slow-drain", "crash", "auto"])
def test_recorded_source_hashes_and_recomputed_events(name):
    folder = ROOT / "docs/evidence/lab-final" / name
    report = json.loads((folder / "verification.json").read_text())
    sources = []
    for filename, digest in report["source_sha256"].items():
        file = folder / filename
        assert hashlib.sha256(file.read_bytes()).hexdigest() == digest
        sources.append({"name": filename, "content": file.read_text()})
    c = ingest(sources)
    shipped = json.loads((ROOT / "src/undertow/web/demos" / f"{name}.json").read_text())
    assert c["events"] == shipped["events"]
    assert c["analysis"] == shipped["analysis"]
    assert report["producer_acks"] == 96 and report["unique_processed_ids"] == 96
    assert not report["missing_ids"]
    assert all(p["offset"] == 16 for p in report["broker_next_offsets"])


@pytest.mark.parametrize(
    "name",
    ["baseline", "slow-drain", "crash", "auto", "native-only", "truncated", "scale-64"],
)
def test_every_demo_has_python_browser_analysis_parity(name):
    path = ROOT / "src/undertow/web/demos" / f"{name}.json"
    c = validate_bundle(json.loads(path.read_text()))
    script = "const E=require(process.argv[1]),fs=require('fs');process.stdout.write(JSON.stringify(E.validate(JSON.parse(fs.readFileSync(process.argv[2],'utf8'))).analysis));"
    result = json.loads(
        subprocess.check_output(
            ["node", "-e", script, str(ROOT / "src/undertow/web/engine.js"), str(path)]
        )
    )
    assert result == c["analysis"]
