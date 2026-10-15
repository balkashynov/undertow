"""Bounded parsing and conservative, deterministic incident analysis.

Input text is data. No eval, imports, shell execution, or network calls are used.
Raw log strings are retained locally but excluded by pseudonymized export.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

MAX_BYTES = 20 * 1024 * 1024
MAX_EVENTS = 100_000
MAX_LINE = 64_000
SCHEMA = "undertow.incident/v1"
KINDS = {
    "start",
    "assign",
    "revoke",
    "revoked",
    "strategy_ready",
    "drain_start",
    "drain_end",
    "commit_attempt",
    "commit_confirmed",
    "processed",
    "crash",
    "stop",
    "pause",
    "resume",
    "stuck",
    "commit_error",
    "note",
}
PARTITION = re.compile(
    r"Partition\(topic=Topic\(name=['\"]([^'\"]+)['\"]\), index=(\d+)\)(?:\s*:\s*(-?\d+))?"
)
PLAIN = re.compile(
    r"^\[?(?P<ts>\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(?:[.,]\d+)?(?:Z|[+-]\d\d:\d\d)?)\]?\s+(?P<level>DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL)\s+\[(?P<consumer>[^\]]+)\]\s+(?:(?P<logger>[\w.]+):\s+)?(?P<message>.*)$"
)


class InputError(ValueError):
    pass


def timestamp(value: Any, assume_utc: bool = False) -> int:
    if not isinstance(value, str) or len(value) > 80:
        raise InputError("Timestamp must be an ISO 8601 string")
    try:
        dt = datetime.fromisoformat(value.replace(",", ".").replace("Z", "+00:00"))
        if dt.tzinfo is None:
            if not assume_utc:
                raise InputError(
                    "Timestamp has no timezone; choose an explicit UTC assumption"
                )
            dt = dt.replace(tzinfo=timezone.utc)
        result = round(dt.timestamp() * 1000)
        if not 0 <= result <= 32_503_680_000_000:
            raise ValueError()
        return result
    except (ValueError, OverflowError, OSError) as exc:
        raise InputError("Invalid or unsupported timestamp") from exc


def text(value: Any, limit: int = 500) -> str:
    return str(value or "")[:limit]


def classify(message: str) -> str:
    if "main thread stuck for more than" in message:
        return "stuck"
    if "Commit failed:" in message:
        return "commit_error"
    if "pausing consumer" in message:
        return "pause"
    if "New partitions assigned:" in message:
        return "assign"
    if "Partitions to revoke:" in message:
        return "revoke"
    if "Partition revocation complete." in message:
        return "revoked"
    if "Initialized processing strategy:" in message:
        return "strategy_ready"
    if "Waiting for " in message and " to exit" in message:
        return "drain_start"
    if "exited successfully" in message:
        return "drain_end"
    if re.search(r"Waited [\d.]+ seconds for offsets to be committed", message):
        return "commit_attempt"
    if "Caught exception, shutting down" in message or " crashed" in message:
        return "crash"
    if "Processor terminated" in message or message == "Stopped":
        return "stop"
    if message == "Starting":
        return "start"
    return "note"


def decode_record(obj: dict, source: str, line: int, assume_utc: bool) -> dict:
    payload = obj.get("jsonPayload")
    payload = payload if isinstance(payload, dict) else obj
    resource = obj.get("resource", {})
    labels = resource.get("labels", {}) if isinstance(resource, dict) else {}
    labels = labels if isinstance(labels, dict) else {}
    pod = "/".join(
        str(labels[k])
        for k in ("cluster_name", "namespace_name", "pod_name", "container_name")
        if labels.get(k)
    )
    consumer = payload.get("consumer") or payload.get("consumer_id") or pod or source
    group = payload.get("group_id") or payload.get("group")
    instance = payload.get("instance_id")
    if instance:
        consumer = f"{consumer}@{instance}"
    if group:
        consumer = f"{group} / {consumer}"
    if not isinstance(consumer, str) or not 1 <= len(consumer) <= 250:
        raise InputError(
            "Consumer identity must be 1 to 250 characters; it is never silently truncated"
        )
    message = payload.get("message") or obj.get("textPayload") or ""
    if not isinstance(message, str):
        raise InputError("message/textPayload must be text")
    traceback = payload.get("traceback")
    if isinstance(traceback, str):
        message += "\n" + traceback
    if len(message) > MAX_LINE:
        raise InputError("Message plus traceback exceeds 64,000 characters")
    explicit = payload.get("undertow")
    kind = classify(message)
    partitions = [
        {"topic": t, "partition": int(p), **({"offset": int(o)} if o else {})}
        for t, p, o in PARTITION.findall(message)
    ]
    evidence = "log"
    if explicit is not None:
        if not isinstance(explicit, dict) or explicit.get("kind") not in KINDS:
            raise InputError("Invalid Undertow extension event")
        kind = explicit["kind"]
        partitions = explicit.get("partitions", [])
        evidence = "instrumented"  # An assertion from the supplied producer, not independently trusted.
    validate_partitions(partitions)
    if kind in {"processed", "commit_confirmed"} and (
        not partitions or any("offset" not in p or p["offset"] < 0 for p in partitions)
    ):
        raise InputError(
            "Progress/commit instrumentation requires partition and nonnegative next offset"
        )
    return {
        "id": f"e{source}-{line}",
        "ts": timestamp(obj.get("timestamp") or payload.get("timestamp"), assume_utc),
        "consumer": consumer,
        "kind": kind,
        "partitions": partitions,
        "level": text(obj.get("severity") or payload.get("level") or "INFO", 20),
        "logger": text(payload.get("logger"), 180),
        "message": text(message, MAX_LINE),
        "source": source,
        "line": line,
        "evidence": evidence,
    }


def validate_partitions(parts: Any) -> None:
    if not isinstance(parts, list) or len(parts) > 4096:
        raise InputError("partitions must be a bounded array")
    for p in parts:
        if not isinstance(p, dict) or set(p) - {"topic", "partition", "offset"}:
            raise InputError("Invalid partition fields")
        if not isinstance(p.get("topic"), str) or not 1 <= len(p["topic"]) <= 249:
            raise InputError("Invalid topic")
        if type(p.get("partition")) is not int or not 0 <= p["partition"] <= 2**31 - 1:
            raise InputError("Invalid partition number")
        if "offset" in p and (
            type(p["offset"]) is not int or not -(2**53 - 1) <= p["offset"] <= 2**53 - 1
        ):
            raise InputError("Invalid offset")


def parse_sources(
    sources: list[dict], assume_utc: bool = False
) -> tuple[list, list, list]:
    if not isinstance(sources, list) or not 1 <= len(sources) <= 128:
        raise InputError("Supply between 1 and 128 sources")
    size = sum(
        len(s.get("content", "").encode("utf-8"))
        for s in sources
        if isinstance(s, dict) and isinstance(s.get("content"), str)
    )
    if size > MAX_BYTES:
        raise InputError("Input exceeds 20 MiB")
    events, diagnostics, manifest = [], [], []
    record_count = 0
    gcp_seen = set()
    for number, source in enumerate(sources):
        if not isinstance(source, dict) or not isinstance(source.get("content"), str):
            raise InputError("Each source needs text content")
        name = text(source.get("name") or f"source-{number + 1}", 180)
        content = source["content"]
        manifest.append(
            {
                "name": name,
                "sha256": hashlib.sha256(content.encode()).hexdigest(),
                "bytes": len(content.encode()),
            }
        )
        stripped = content.lstrip()
        if stripped.startswith("["):
            try:
                items = json.loads(content)
                if not isinstance(items, list):
                    raise InputError("Expected JSON array")
            except json.JSONDecodeError:
                items = content.splitlines()  # Bracketed plain log timestamp.
        else:
            items = content.splitlines()
        for line, item in enumerate(items, 1):
            record_count += 1
            if record_count > MAX_EVENTS:
                raise InputError(
                    "Input exceeds 100,000 records including rejected lines"
                )
            if isinstance(item, str) and not item.strip():
                continue
            try:
                if isinstance(item, str):
                    if len(item) > MAX_LINE:
                        raise InputError("Line exceeds 64,000 characters")
                    if item.lstrip().startswith("{"):
                        obj = json.loads(item)
                    else:
                        match = PLAIN.match(item)
                        if not match:
                            raise InputError(
                                "Unrecognized line format (including traceback continuations)"
                            )
                        obj = {**match.groupdict(), "timestamp": match["ts"]}
                else:
                    obj = item
                if not isinstance(obj, dict):
                    raise InputError("Log entry must be an object")
                if obj.get("insertId") and obj.get("logName"):
                    key = (
                        text(obj["insertId"]),
                        text(obj["logName"]),
                        text(obj.get("timestamp")),
                    )
                    if key in gcp_seen:
                        raise InputError(
                            "Duplicate GCP insertId/logName/timestamp ignored"
                        )
                    gcp_seen.add(key)
                event = decode_record(obj, name, line, assume_utc)
                event["id"] = f"e{number + 1}-{line}"
                events.append(event)
                if len(events) > MAX_EVENTS:
                    raise InputError("Input exceeds 100,000 events")
            except (ValueError, TypeError) as exc:
                if str(exc) == "Input exceeds 100,000 events":
                    raise
                diagnostics.append(
                    {"source": name, "line": line, "reason": str(exc)[:200]}
                )
    return events, diagnostics, manifest


def analyze(
    events: list[dict],
    title: str = "Untitled incident",
    diagnostics: list | None = None,
    sources: list | None = None,
    provenance: dict | None = None,
) -> dict:
    """Correlate only within the observed consumer/partition assignment epoch.

    Silence has no known cause. Rebalance windows are local revoke callbacks,
    not inferred Kafka group generations. Processing and commit lag require
    explicit instrumentation; built-in commit-attempt logs never satisfy them.
    """
    events = sorted(
        copy.deepcopy(events), key=lambda e: (e["ts"], e["source"], e["line"], e["id"])
    )
    end = events[-1]["ts"] if events else 0
    start = events[0]["ts"] if events else 0
    lanes: dict[str, list] = defaultdict(list)
    for event in events:
        lanes[event["consumer"]].append(event)
    epochs, windows, findings = [], [], []
    for consumer, lane in sorted(lanes.items()):
        active: dict[tuple, dict] = {}
        pending_revoke = None
        drain = None
        last_ts = None
        for event in lane:
            kind, ts = event["kind"], event["ts"]
            if last_ts is not None and ts - last_ts > 5000:
                findings.append(
                    {
                        "severity": "info",
                        "code": "observation_gap",
                        "consumer": consumer,
                        "start": last_ts,
                        "end": ts,
                        "event_ids": [event["id"]],
                        "message": f"No recorded events for {(ts - last_ts) / 1000:.2f}s. Silence alone does not establish a stall.",
                    }
                )
            last_ts = ts
            if kind == "assign":
                for p in event["partitions"]:
                    key = (p["topic"], p["partition"])
                    if key in active:
                        active[key]["end"] = ts
                        active[key]["end_reason"] = "reassigned"
                    ep = {
                        "consumer": consumer,
                        **p,
                        "start": ts,
                        "end": end,
                        "end_reason": "observation_end",
                        "first_processed": None,
                        "first_commit": None,
                        "assignment_event": event["id"],
                    }
                    epochs.append(ep)
                    active[key] = ep
            elif kind == "revoke":
                if pending_revoke is not None:
                    windows.append({**pending_revoke, "end": ts, "complete": False})
                pending_revoke = {
                    "consumer": consumer,
                    "start": ts,
                    "event_id": event["id"],
                    "partitions": event["partitions"],
                }
                # Ownership changes at revoke start; subsequent drain events are not a new assignment.
                for p in event["partitions"]:
                    ep = active.pop((p["topic"], p["partition"]), None)
                    if ep:
                        ep.update(end=ts, end_reason="revoke")
            elif kind == "revoked" and pending_revoke is not None:
                windows.append({**pending_revoke, "end": ts, "complete": True})
                pending_revoke = None
            elif kind in {"processed", "commit_confirmed"}:
                for p in event["partitions"]:
                    ep = active.get((p["topic"], p["partition"]))
                    if ep:
                        key = (
                            "first_processed" if kind == "processed" else "first_commit"
                        )
                        if ep[key] is None:
                            ep[key] = ts
            elif kind == "drain_start":
                drain = event
            elif kind == "drain_end" and drain:
                duration = ts - drain["ts"]
                findings.append(
                    {
                        "severity": "warning" if duration > 1000 else "info",
                        "code": "strategy_drain",
                        "consumer": consumer,
                        "start": drain["ts"],
                        "end": ts,
                        "event_ids": [drain["id"], event["id"]],
                        "message": f"Strategy join took {duration / 1000:.3f}s in the recorded log clock.",
                    }
                )
                drain = None
            if kind in {"crash", "stop"}:
                for ep in active.values():
                    ep.update(end=ts, end_reason=kind)
                active.clear()
            if kind in {"crash", "stuck", "commit_error"}:
                messages = {
                    "crash": "Crash/shutdown exception recorded. Inspect this event and neighboring evidence.",
                    "stuck": "Arroyo's stuck detector reported the main thread. This is detector evidence, not a diagnosis of the cause.",
                    "commit_error": "A commit failure was logged. Later recovery and broker state need separate evidence.",
                }
                findings.append(
                    {
                        "severity": "error",
                        "code": kind,
                        "consumer": consumer,
                        "start": ts,
                        "end": ts,
                        "event_ids": [event["id"]],
                        "message": messages[kind],
                    }
                )
        if pending_revoke:
            windows.append({**pending_revoke, "end": end, "complete": False})
        if drain:
            findings.append(
                {
                    "severity": "warning",
                    "code": "open_drain",
                    "consumer": consumer,
                    "start": drain["ts"],
                    "end": end,
                    "event_ids": [drain["id"]],
                    "message": "Strategy join began but no completion is present. The capture may be incomplete.",
                }
            )
    for window in windows:
        if not window["complete"]:
            findings.append(
                {
                    "severity": "warning",
                    "code": "open_revoke",
                    "consumer": window["consumer"],
                    "start": window["start"],
                    "end": window["end"],
                    "event_ids": [window["event_id"]],
                    "message": "Revocation callback has no observed completion in this capture.",
                }
            )
    counts = Counter(e["kind"] for e in events)
    if counts["commit_attempt"]:
        findings.append(
            {
                "severity": "info",
                "code": "commit_ambiguity",
                "consumer": None,
                "start": start,
                "end": end,
                "event_ids": [],
                "message": f"{counts['commit_attempt']} commit-path logs do not prove broker commits. Auto-commit and empty mappings can produce the same log.",
            }
        )
    diagnostics = diagnostics or []
    return {
        "schema": SCHEMA,
        "title": text(title, 180),
        "events": events,
        "diagnostics": diagnostics,
        "sources": sources or [],
        "provenance": provenance
        or {"type": "user-supplied", "description": "Unverified source assertions"},
        "analysis": {
            "start": start,
            "end": end,
            "consumers": sorted(lanes),
            "epochs": epochs,
            "revocations": windows,
            "findings": sorted(findings, key=lambda f: (f["start"], f["code"])),
            "counts": dict(counts),
            "recognized": sum(v for k, v in counts.items() if k != "note"),
            "coverage": {
                "processed": bool(counts["processed"]),
                "confirmed_commits": bool(counts["commit_confirmed"]),
                "rejected_lines": len(diagnostics),
            },
        },
        "limits": [
            "Cross-consumer alignment assumes synchronized source clocks; no clock correction is inferred.",
            "Revocation windows are observed callbacks, not Kafka group-generation barriers.",
            "Missing processing or commit evidence means unknown, not zero latency or a stall.",
            "Custom events are source assertions. Only documented lab probes have been independently checked.",
        ],
    }


def ingest(
    sources: list, title: str = "Imported incident", assume_utc: bool = False
) -> dict:
    events, diagnostics, manifest = parse_sources(sources, assume_utc)
    if not events:
        raise InputError(
            "No timestamped events parsed. Use JSON/JSONL, GCP LogEntry exports, or the documented plain format."
        )
    return analyze(events, title, diagnostics, manifest)


def validate_bundle(data: Any) -> dict:
    if not isinstance(data, dict) or data.get("schema") != SCHEMA:
        raise InputError("Unsupported incident schema")
    events = data.get("events")
    if not isinstance(events, list) or not 1 <= len(events) <= MAX_EVENTS:
        raise InputError("Incident must contain 1 to 100,000 events")
    ids = set()
    required = {
        "id",
        "ts",
        "consumer",
        "kind",
        "partitions",
        "message",
        "source",
        "line",
        "evidence",
        "level",
        "logger",
    }
    for e in events:
        if not isinstance(e, dict) or set(e) != required:
            raise InputError("Invalid event fields")
        if e["kind"] not in KINDS or e["evidence"] not in {"log", "instrumented"}:
            raise InputError("Invalid event kind/evidence")
        if type(e["ts"]) is not int or not 0 <= e["ts"] <= 32_503_680_000_000:
            raise InputError("Invalid event timestamp")
        if type(e["line"]) is not int or e["line"] < 1:
            raise InputError("Invalid event line")
        for key in ("id", "consumer", "message", "source", "level", "logger"):
            if not isinstance(e[key], str) or len(e[key]) > (
                MAX_LINE if key == "message" else 250
            ):
                raise InputError("Invalid event text")
        if not e["consumer"] or e["id"] in ids:
            raise InputError("Missing consumer or duplicate event ID")
        ids.add(e["id"])
        validate_partitions(e["partitions"])
        if e["kind"] in {"processed", "commit_confirmed"} and (
            not e["partitions"]
            or any("offset" not in p or p["offset"] < 0 for p in e["partitions"])
        ):
            raise InputError("Progress/commit events need partition next offsets")
        if (
            e["kind"] in {"processed", "commit_confirmed"}
            and e["evidence"] != "instrumented"
        ):
            raise InputError(
                "Progress/commit events must be labeled instrumented assertions"
            )
    # Never trust imported derived analysis; recompute it.
    return analyze(
        events,
        text(data.get("title")),
        provenance={
            "type": "imported-bundle",
            "description": "Imported evidence; derived analysis recomputed",
        },
    )


def redact(bundle: dict) -> dict:
    """Pseudonymize structured identifiers; discard arbitrary source strings."""
    events = copy.deepcopy(bundle["events"])
    consumers = {
        v: f"consumer-{i + 1:02d}"
        for i, v in enumerate(sorted({e["consumer"] for e in events}))
    }
    topics = {
        v: f"topic-{i + 1:02d}"
        for i, v in enumerate(
            sorted({p["topic"] for e in events for p in e["partitions"]})
        )
    }
    origin = min(e["ts"] for e in events) if events else 0
    for i, e in enumerate(events):
        e.update(
            id=f"e{i + 1}",
            consumer=consumers[e["consumer"]],
            source="redacted",
            line=i + 1,
            logger="redacted",
            message=f"{e['kind']} (original message removed)",
            ts=946684800000 + e["ts"] - origin,
            level="ERROR" if e["kind"] == "crash" else "INFO",
        )
        for p in e["partitions"]:
            p["topic"] = topics[p["topic"]]
    return analyze(
        events,
        "Pseudonymized incident",
        provenance={
            "type": "pseudonymized",
            "description": "Names and text removed; relative timing, offsets and partition counts retained",
        },
    )


def mermaid(bundle: dict, limit: int = 250) -> str:
    consumers = bundle["analysis"]["consumers"]
    aliases = {c: f"C{i + 1}" for i, c in enumerate(consumers)}
    out = ["sequenceDiagram", "    autonumber"]
    # Only generated labels are emitted, preventing Mermaid directive injection.
    out.extend(
        f"    participant {alias} as Consumer {i + 1}"
        for i, alias in enumerate(aliases.values())
    )
    selected = [
        e for e in bundle["events"] if e["kind"] not in {"note", "commit_attempt"}
    ]
    for e in selected[:limit]:
        label = f"{(e['ts'] - bundle['analysis']['start']) / 1000:.3f}s {e['kind']}"
        out.append(f"    Note over {aliases[e['consumer']]}: {label}")
    if len(selected) > limit:
        out.append(
            f"    %% Truncated to {limit} events; use JSON for complete evidence."
        )
    return "\n".join(out) + "\n"


def comparison(a: dict, b: dict) -> dict:
    def metrics(case: dict) -> dict:
        rev = case["analysis"]["revocations"]
        complete = [w["end"] - w["start"] for w in rev if w["complete"]]
        epochs = case["analysis"]["epochs"]
        processed = [
            e["first_processed"] - e["start"]
            for e in epochs
            if e["first_processed"] is not None
        ]
        commits = [
            e["first_commit"] - e["start"]
            for e in epochs
            if e["first_commit"] is not None
        ]
        return {
            "events": len(case["events"]),
            "consumers": len(case["analysis"]["consumers"]),
            "crashes": case["analysis"]["counts"].get("crash", 0),
            "open_revocations": sum(not w["complete"] for w in rev),
            "max_revoke_ms": max(complete, default=None),
            "max_first_processing_ms": max(processed, default=None),
            "max_first_confirmed_commit_ms": max(commits, default=None),
        }

    left, right = metrics(a), metrics(b)
    return {
        "baseline": a["title"],
        "candidate": b["title"],
        "metrics": [
            {
                "name": k,
                "baseline": left[k],
                "candidate": right[k],
                "delta": right[k] - left[k]
                if left[k] is not None and right[k] is not None
                else None,
            }
            for k in left
        ],
        "limit": "Descriptive comparison of captures. Different workloads, duration, clocks and logging levels can change these values.",
    }
