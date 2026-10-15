# Input and analysis contract

Contract version: `undertow.incident/v1`. Native message patterns were checked against Arroyo commit `49c24ec2cd0296a11d6d681de14db4f76394ce3e` (2.46.0). Other versions may have different logs; unrecognized timestamped records remain `note` events.

## Import

Accepted envelopes:

1. JSONL objects with `timestamp`, `message`, and `consumer` or `consumer_id`.
2. GCP LogEntry JSON arrays or JSONL. `timestamp` is required. `textPayload` and `jsonPayload.message` are recognized. `receiveTimestamp` is not substituted for event time. Exact repeated `(insertId, logName, timestamp)` tuples are ignored with a diagnostic when both identifier fields are supplied.
3. Plain lines: `ISO_TIMESTAMP LEVEL [consumer] logger.name: message`. The logger is optional; the timestamp, level and bracketed identity are required.

Timestamp parsing accepts ISO 8601 offsets and `Z`. Timezone-less values are rejected unless the user explicitly selects `--assume-utc`. Display/correlation uses integer milliseconds; finer fractional timestamps are rounded to milliseconds. Ties preserve source/line order, then event ID. This is a deterministic display order, not a cross-machine happens-before claim.

`group_id` (or `group`) and optional `instance_id` are included in the consumer lane identity. If neither explicit consumer field exists, GCP cluster/namespace/pod/container labels are combined. The input filename is the last fallback. When identifiers are missing, one file must describe one consumer instance. Group-level state is never inferred by joining consumer lanes.

Bounds: 128 input sources, 20 MiB total raw input, 100,000 input lines/records, 64,000 characters per message including optional `traceback`, 250 characters per consumer identity, and 4,096 partition entries per event. Partition offsets are signed integers in the JavaScript-safe range `[-(2^53-1), 2^53-1]`. Larger values are rejected rather than silently losing precision. Known native negative offset sentinels can be retained in assignment data; progress instrumentation requires a nonnegative next offset.

Unsupported or malformed nonempty lines are reported, including free-standing traceback continuations. Structured `traceback` text is retained with its parent message. Unknown messages with valid timestamps remain visible in Evidence log. No constructor, expression, Python code or shell text from a log is executed.

## Extensible events

A producer can attach an `undertow` object to a JSON log record. All such events are labeled `instrumented`, including imported assertions from third-party producers. A label does not authenticate their origin.

```json
{
  "timestamp": "2026-10-15T12:00:01.000Z",
  "consumer": "worker-a",
  "group_id": "ingest",
  "message": "Application callback completed",
  "undertow": {
    "kind": "processed",
    "partitions": [{"topic": "events", "partition": 0, "offset": 43}]
  }
}
```

Offsets use **next-offset** semantics: after processing record 42, report 43. `commit_confirmed` requires evidence of a successful broker commit. The lab emits it only after an explicitly manual `KafkaConsumer.commit_offsets()` returns a nonempty successful mapping. It independently queries final broker offsets as an additional check. The generic importer does not contact Kafka to validate user-supplied assertions.

Supported event kinds: `start`, `assign`, `revoke`, `revoked`, `strategy_ready`, `drain_start`, `drain_end`, `commit_attempt`, `commit_confirmed`, `processed`, `crash`, `stop`, `pause`, `resume`, `stuck`, `commit_error`, and `note`. Custom systems such as Snuba can map their meaningful events into this schema without adding executable parser plugins.

## Derived facts

- **Assignment epoch:** starts at that partition's assignment log. Ends at its revocation start, reassignment, crash/stop, or the capture boundary. Partial revocations remove only listed partitions.
- **Revocation duration:** observed `Partitions to revoke` to `Partition revocation complete` on one consumer. An unmatched callback is incomplete. It is not a Kafka group-generation barrier or proof of global stability.
- **Strategy join:** observed `Waiting for ... to exit` to `... exited successfully` on one consumer. Duration uses source log time; absent completion is censored at the capture end.
- **First processing/commit:** first corresponding instrumented partition event inside an assignment epoch. Missing evidence stays null/unknown. Commits during revocation drain are not retroactively assigned to a new epoch.
- **Stuck:** an explicit Arroyo stuck-detector report. A source event does not establish the exact onset or cause of the stall.
- **Observation gap:** more than five seconds between recorded events for one lane. It does not imply failure or lack of work.
- **Crash:** a recognized callback/processor exception log. Multiple exception lines may describe one underlying failure; counts are logged events, not deduplicated incidents.

Cross-machine clock synchronization, complete participant coverage, and log completeness are external assumptions. Undertow does not infer broker generation, global stability, end-to-end delivery guarantees, or production root cause.

## Export

Original JSON retains source messages and metadata. Imported bundles are schema-checked and their derived analysis is recomputed. Browser and Python engines share tested semantics. The browser paginates evidence at 50 rows, displays up to 128 matching consumer lanes and 160 markers per lane, and shows the first 500 partition epochs. Filter or use JSON/CLI for the complete evidence.

Pseudonymized JSON removes arbitrary source messages, logger names, original source paths, consumer/topic names and absolute time. It uses generated aliases and shifts the first event to 2000-01-01 UTC. Relative timing, offsets, partition counts, event kinds and ordering remain. It discards source hashes and parser diagnostics because these may identify input. This is not a guarantee of anonymity.

Mermaid uses generated consumer aliases and generated event labels, never arbitrary log text. It includes up to 250 lifecycle/progress events and declares truncation. Standalone HTML embeds all application assets and evidence with HTML-significant JSON characters escaped. No external requests are required to investigate that embedded case.
