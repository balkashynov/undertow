import json
import subprocess
from pathlib import Path

import pytest

from undertow.core import InputError, analyze, ingest, mermaid, redact, validate_bundle


def record(ms, message, consumer="worker-a", extension=None, **extra):
    result = {
        "timestamp": f"2026-10-15T12:00:{ms // 1000:02d}.{ms % 1000:03d}Z",
        "consumer": consumer,
        "message": message,
        "logger": "arroyo.processing.processor",
        **extra,
    }
    if extension:
        result["undertow"] = extension
    return result


def case(records):
    return ingest(
        [
            {
                "name": "capture.jsonl",
                "content": "\n".join(json.dumps(r) for r in records),
            }
        ]
    )


def assign(*partitions):
    return (
        "New partitions assigned: {"
        + ", ".join(
            f"Partition(topic=Topic(name='events'), index={p}): 0" for p in partitions
        )
        + "}"
    )


def test_native_commit_log_does_not_establish_commit_or_processing():
    c = case(
        [
            record(0, assign(0)),
            record(
                500,
                "Waited 0.0001 seconds for offsets to be committed to <KafkaConsumer>.",
            ),
        ]
    )
    assert c["analysis"]["epochs"][0]["first_commit"] is None
    assert c["analysis"]["epochs"][0]["first_processed"] is None
    assert c["analysis"]["coverage"]["confirmed_commits"] is False
    assert c["analysis"]["findings"][0]["code"] == "commit_ambiguity"


def test_partial_cooperative_revoke_preserves_other_partition():
    c = case(
        [
            record(0, assign(0, 1)),
            record(
                100,
                "Partitions to revoke: [Partition(topic=Topic(name='events'), index=0)]",
            ),
            record(200, "Partition revocation complete."),
            record(
                250,
                "done",
                extension={
                    "kind": "processed",
                    "partitions": [{"topic": "events", "partition": 1, "offset": 0}],
                },
            ),
        ]
    )
    p0, p1 = c["analysis"]["epochs"]
    assert p0["end"] - p0["start"] == 100
    assert p1["first_processed"] - p1["start"] == 250
    assert (
        c["analysis"]["revocations"][0]["end"]
        - c["analysis"]["revocations"][0]["start"]
        == 100
    )


def test_group_and_instance_identity_are_not_merged():
    c = case(
        [
            record(0, assign(0), group_id="a"),
            record(0, assign(0), group_id="b"),
            record(1, assign(0), group_id="a", instance_id="restart"),
        ]
    )
    assert len(c["analysis"]["consumers"]) == 3


def test_incomplete_capture_and_silence_are_not_stall_diagnosis():
    c = case(
        [
            record(0, "Partitions to revoke: []"),
            record(100, "Waiting for <strategy> to exit..."),
            record(9000, "other note", "worker-b"),
        ]
    )
    assert not c["analysis"]["revocations"][0]["complete"]
    assert {f["code"] for f in c["analysis"]["findings"]} == {
        "open_revoke",
        "open_drain",
    }
    assert c["analysis"]["counts"].get("stuck", 0) == 0


def test_tied_millisecond_events_preserve_source_line_order():
    rows = [record(0, "note") for _ in range(8)] + [
        record(0, assign(0)),
        record(
            0,
            "done",
            extension={
                "kind": "processed",
                "partitions": [{"topic": "events", "partition": 0, "offset": 1}],
            },
        ),
    ]
    c = case(rows)
    assert c["analysis"]["epochs"][0]["first_processed"] is not None


def test_gcp_json_and_plain_are_supported_with_timezone_required():
    obj = {
        "timestamp": "2026-10-15T06:00:00-06:00",
        "textPayload": assign(0),
        "resource": {"labels": {"pod_name": "pod-a", "namespace_name": "ns"}},
    }
    c = ingest([{"name": "gcp.json", "content": json.dumps([obj])}])
    assert c["events"][0]["consumer"] == "ns/pod-a"
    assert c["analysis"]["counts"]["assign"] == 1
    plain = (
        "2026-10-15 12:00:00,000 INFO [worker-a] arroyo.processing.processor: Starting"
    )
    with pytest.raises(InputError):
        ingest([{"name": "plain.log", "content": plain}])
    c = ingest([{"name": "plain.log", "content": plain}], assume_utc=True)
    assert c["events"][0]["ts"] == obj_to_ms(obj)


def obj_to_ms(obj):
    from undertow.core import timestamp

    return timestamp(obj["timestamp"])


def test_bad_lines_reported_without_guessing_timestamp():
    content = (
        json.dumps(record(0, "Starting"))
        + "\nnot a log\n"
        + json.dumps({"message": "New partitions assigned: {}"})
    )
    c = ingest([{"name": "x.log", "content": content}])
    assert len(c["events"]) == 1 and len(c["diagnostics"]) == 2


def test_gcp_duplicate_ids_are_ignored_but_independent_events_survive():
    r = record(0, "Starting", insertId="x", logName="projects/test/logs/worker")
    c = case([r, r, {**r, "insertId": "y"}])
    assert len(c["events"]) == 2
    assert "Duplicate GCP" in c["diagnostics"][0]["reason"]


def test_crash_traceback_is_preserved_and_long_identity_rejected():
    c = case(
        [
            record(
                0,
                "Caught exception, shutting down...",
                traceback="ValueError: source evidence",
            )
        ]
    )
    assert "ValueError: source evidence" in c["events"][0]["message"]
    with pytest.raises(InputError):
        case([record(0, "Starting", consumer="x" * 251)])


def test_instrumented_progress_requires_offsets_and_correct_evidence_label():
    with pytest.raises(InputError):
        case([record(0, "done", extension={"kind": "processed", "partitions": []})])
    c = case(
        [
            record(
                0,
                "done",
                extension={
                    "kind": "processed",
                    "partitions": [{"topic": "events", "partition": 0, "offset": 1}],
                },
            )
        ]
    )
    c["events"][0]["evidence"] = "log"
    with pytest.raises(InputError):
        validate_bundle(c)


def test_pseudonymization_removes_unstructured_identifiers_and_absolute_time():
    c = case(
        [
            record(0, assign(0), consumer="secret-worker"),
            record(500, "secret@example.com token=abc </script>"),
        ]
    )
    r = redact(c)
    assert r["analysis"]["end"] - r["analysis"]["start"] == 500
    serialized = json.dumps(r)
    for secret in ["secret", "capture.jsonl", "2026", "token=", "</script>"]:
        assert secret not in serialized
    assert r["analysis"]["start"] == 946684800000


def test_untrusted_bundle_analysis_is_recomputed_and_duplicate_ids_rejected():
    c = case([record(0, assign(0))])
    c["analysis"] = {"made_up": True}
    assert "epochs" in validate_bundle(c)["analysis"]
    c["events"].append(c["events"][0])
    with pytest.raises(InputError):
        validate_bundle(c)


def test_constructor_and_mermaid_injection_are_data():
    c = case(
        [
            record(
                0,
                "New partitions assigned: __import__('os').system('echo no')",
                consumer="evil\nend\nsequenceDiagram",
            )
        ]
    )
    assert c["events"][0]["partitions"] == []
    assert "evil" not in mermaid(c)


def test_browser_engine_matches_python_analysis_and_redaction():
    rows = [
        record(0, assign(0, 1)),
        record(
            100,
            "done",
            extension={
                "kind": "processed",
                "partitions": [{"topic": "events", "partition": 0, "offset": 0}],
            },
        ),
        record(
            200,
            "broker",
            extension={
                "kind": "commit_confirmed",
                "partitions": [{"topic": "events", "partition": 0, "offset": 1}],
            },
        ),
        record(
            300,
            "Partitions to revoke: [Partition(topic=Topic(name='events'), index=0)]",
        ),
        record(301, "Waiting for <x> to exit..."),
        record(2301, "<x> exited successfully"),
        record(2302, "Partition revocation complete."),
        record(
            3000, "main thread stuck for more than 2 seconds, stacks: x", "worker-b"
        ),
        record(9000, "Waited 0.0001 seconds for offsets to be committed to <x>."),
    ]
    c = case(rows)
    engine = Path(__file__).parents[1] / "src/undertow/web/engine.js"
    script = "const E=require(process.argv[1]);let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{const c=JSON.parse(s);process.stdout.write(JSON.stringify({analysis:E.validate(c).analysis,redacted:E.redact(E.validate(c)),mermaid:E.mermaid(E.validate(c))}));});"
    result = json.loads(
        subprocess.check_output(
            ["node", "-e", script, str(engine)], input=json.dumps(c).encode()
        )
    )
    assert result["analysis"] == c["analysis"]
    assert result["redacted"] == redact(c)
    assert result["mermaid"] == mermaid(c)


def test_html_export_cannot_break_out_of_embedded_json():
    from undertow.server import standalone_html

    c = case([record(0, "</script><script>alert('xss')</script>")])
    html = standalone_html(c)
    assert "</script><script>alert('xss')" not in html
    assert "window.UNDERTOW_EMBEDDED" in html and 'src="app.js"' not in html
    assert 'href="styles.css"' not in html and 'src="engine.js"' not in html
