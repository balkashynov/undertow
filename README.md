<div align="center">

# ≈ Undertow

### Your consumers moved. Find what didn't.

**An evidence-first incident workbench for Kafka consumers and Sentry Arroyo.**

[Explore the demo](https://balkashynov.github.io/undertow/) · [Run locally](#run-locally) · [Evidence contract](docs/CONTRACT.md) · [Validation](docs/VALIDATION.md)

![Undertow investigation overview](docs/preview.svg)

</div>

A consumer group rebalances. One worker is draining its processing strategy. Another has revoked its partitions. A third is waiting for work. Their individual logs do not explain the transition.

Undertow puts those observations on one timeline, keeps every conclusion connected to its source line, and makes the investigation portable. It is built around the concrete workflow requested in [Sentry Arroyo #461](https://github.com/getsentry/arroyo/issues/461).

Independent project by Ivan Balkashynov. No Sentry affiliation, endorsement, production deployment, or internal-tooling claim.

## What you can do

- **Investigate consumer transitions:** zoom and filter a multi-consumer timeline, inspect observed revocation callbacks, and trace partition ownership through partial cooperative rebalances.
- **Find the evidence:** click an event or finding to inspect the original message, traceback, timestamp, source file and line. Search the full log with paginated results.
- **Measure only supported facts:** strategy-join durations and revocation callback durations come from native logs. First processing and broker-commit timing require explicit instrumentation.
- **Compare two captures:** view event counts, incomplete callbacks and observed timing differences without replacing unknown values with zero.
- **Import your own data:** GCP LogEntry arrays, structured JSONL, documented plain logs, or an Undertow incident bundle. The local app saves cases on your computer.
- **Share deliberately:** export complete JSON, pseudonymized JSON, a Mermaid sequence, or a self-contained interactive HTML file.
- **Reproduce the demonstration:** run real Arroyo workers against a disposable, pinned Apache Kafka broker. All data is synthetic.

## Run locally

Python 3.11 or newer. The application itself has **zero third-party runtime dependencies**.

```bash
git clone https://github.com/balkashynov/undertow
cd undertow
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/undertow serve
```

Open **http://127.0.0.1:8767/**. The server binds to loopback, checks the Host and Origin, and requires a per-session token for imports/exports. It has no upload service, telemetry, cloud account, or production Kafka connection. Imported cases are saved under `.undertow/cases/` in the working directory. Use `--data-dir /path/to/cases` to choose another folder.

The hosted demo is a static browser viewer with seven prepared cases. It supports importing **Undertow JSON bundles**, comparison, JSON/pseudonymized export and Mermaid. Raw log parsing, the persistent library, and HTML export run in the local app or CLI.

## A three-minute investigation

1. Open **The slow strategy join**. It is a real local Kafka run with a deliberately delayed processing-strategy join.
2. Select the slow **strategy drain** finding. Inspect the native `Waiting for ... to exit` source line. Select its neighboring events in the evidence log.
3. Open **Compare captures** and choose **Healthy rebalance** as the baseline.
4. Open **Native logs only**. First processing and successful broker-commit timing correctly become **unknown**.
5. Open **Incomplete evidence**. A callback without its completion remains incomplete, rather than being declared successful or permanently stuck.
6. Export a pseudonymized case. Raw messages, names, source paths and absolute dates are removed; partition counts, offsets and relative timing remain.

The 64-consumer case is separately labeled **synthetic**. It exercises navigation, not an actual 64-worker Kafka deployment.

## CLI

```bash
# Parse multiple consumer exports. Timestamps need an explicit timezone.
undertow analyze consumer-a.jsonl consumer-b.jsonl \
  --title "Rebalance after deploy" -o incident.json --html incident.html

# Explicitly interpret timezone-less timestamps as UTC, if that is justified.
undertow analyze workers.log --assume-utc -o incident.json

# Portable output and a descriptive comparison.
undertow export incident.json --redact -o shareable.json
undertow export incident.json --redact --format html -o shareable.html
undertow export incident.json --format mermaid -o sequence.mmd
undertow compare baseline.json incident.json
```

`incident.html` contains the viewer, styles and evidence in one file. It works without an HTTP server or external resources. Mermaid export uses generated consumer aliases and is bounded to 250 lifecycle/progress events; JSON retains the complete capture.

## Bring logs, not assumptions

Arroyo does not mandate a log envelope. Supply a timezone and consumer identity through JSONL, GCP resource labels, or this plain format:

```text
2026-10-15T12:00:00.000Z INFO [worker-a] arroyo.processing.processor: New partitions assigned: {Partition(topic=Topic(name='events'), index=0): 42}
```

```json
{"timestamp":"2026-10-15T12:00:00.000Z","consumer":"worker-a","group_id":"ingest","logger":"arroyo.processing.processor","message":"New partitions assigned: {Partition(topic=Topic(name='events'), index=0): 42}"}
```

Optional `instance_id` separates restarts that reuse a consumer name. Group identifiers are included in lane identity. GCP `cluster_name`, `namespace_name`, `pod_name` and `container_name` are combined when no explicit consumer identity is supplied. Separate processes/groups must have distinct identities; Undertow cannot recover missing identity metadata.

See [the complete input and analysis contract](docs/CONTRACT.md) for extension events, offsets, duplicate GCP records, timestamp resolution, bounds and export behavior.

## Why the evidence boundary matters

At pinned Arroyo **2.46.0**, the consumer-configuration helper defaults to auto-commit. The native DEBUG message saying it waited for offsets to be committed can be emitted even when `commit_offsets()` returns `None`, or when there are no staged offsets. **That message is a commit-path observation, not proof of a broker commit.**

Likewise, an assignment is not proof that a message was processed. Silence can mean idle input, missing logs, disabled logging, a crash, or a stall. Undertow reports explicit stuck-detector messages and open callbacks; it does not diagnose silence as a root cause.

Sources: [configuration](https://github.com/getsentry/arroyo/blob/49c24ec2cd0296a11d6d681de14db4f76394ce3e/arroyo/backends/kafka/configuration.py), [consumer commit behavior](https://github.com/getsentry/arroyo/blob/49c24ec2cd0296a11d6d681de14db4f76394ce3e/arroyo/backends/kafka/consumer.py), [processor logs](https://github.com/getsentry/arroyo/blob/49c24ec2cd0296a11d6d681de14db4f76394ce3e/arroyo/processing/processor.py).

## Reproduce the real lab

Docker must be running. The script creates a uniquely named, loopback-published Kafka container and removes it on completion or failure. It installs no daemon and does not connect to your existing Kafka clusters.

```bash
.venv/bin/pip install -e '.[lab,dev]'
.venv/bin/python scripts/lab.py --output docs/evidence/reproduction
.venv/bin/python scripts/build_demo.py
.venv/bin/python -m pytest -q
```

The lab uses Apache Kafka 4.1.0 pinned by image digest, Sentry Arroyo 2.46.0, confluent-kafka 2.16.0 and the KIP-848 consumer protocol. Four scenarios each publish 96 IDs across six partitions: healthy rebalance, delayed join, processing crash/replacement, and auto-commit. It checks producer acknowledgments, processing IDs and independently queried broker offsets. The lab's optional instrumentation is explicitly distinguished from native Arroyo logs.

These are bounded local experiments, not a production reliability benchmark or a proof of exactly-once processing. [Full results and limits](docs/VALIDATION.md).

## Architecture

```text
JSONL / GCP / plain logs
          │
     bounded parser        raw source line + structured event
          │
  conservative analyzer    assignment epochs + callbacks + evidence coverage
          │
   ┌──────┴────────┐
   CLI         local HTTP app ── local incident library
   │               │
   └──────┬────────┘
          ├── JSON / pseudonymized case
          ├── Mermaid sequence
          └── offline HTML / static browser viewer
```

The Python core is the canonical parser. The static viewer recomputes derived analysis from normalized events in JavaScript. Cross-runtime parity tests cover correlation, redaction and Mermaid output. Imported bundles cannot supply their own trusted analysis.

## Useful next integrations

The intended adoption path is a small parser/event contract and fixture set that maintainers can reuse, or a standalone local viewer for exported incidents. Existing [runtime logging work](https://github.com/getsentry/arroyo/pull/439), [rebalance tests](https://github.com/getsentry/arroyo/pull/411), and the [Rust runtime work](https://github.com/getsentry/arroyo/pull/560) are complementary. Undertow does not claim to replace them or know Sentry's internal tools.

The next useful validation is a representative, appropriately sanitized incident export and feedback on the distinction between cluster-level state and runtime observations. No maintainer acceptance is claimed.

## Development

```bash
.venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest -q   # Node.js required for browser-engine parity checks
.venv/bin/python scripts/benchmark.py
```

[Contributing](CONTRIBUTING.md) · [Security and privacy](SECURITY.md) · [Apache-2.0 license](LICENSE)
