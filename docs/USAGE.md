# Investigating a capture with Undertow

## Recorded walkthrough

Open the [hosted workbench](https://balkashynov.github.io/undertow/#workbench). It starts with **01 · The slow strategy join**, a real local Kafka/Arroyo run with a deliberately delayed processing-strategy join.

1. Select the **STRATEGY DRAIN** finding showing **3.015s**. Inspect the native `Waiting for ... to exit` source line and its worker, timestamp and source location.
2. Use **Evidence log** to inspect neighboring messages, or **Partition handoffs** to follow individual assignment epochs.
3. Open **Compare captures** and choose **02 · Healthy rebalance** as the baseline. The table compares the longest completed revocation, along with event counts and instrumented processing/commit observations. It does not compare strategy-join durations directly.
4. Select **05 · Native logs only**, then **Timeline**. First processing and successful broker-commit timing correctly become **unknown**.
5. Select **06 · Incomplete evidence**. A callback without its completion remains incomplete, rather than being declared successful or permanently stuck.
6. Export a pseudonymized case. Raw messages, names, source paths and absolute dates are removed; partition counts, offsets and relative timing remain. Review these retained values before sharing.

The hosted demo is a static browser viewer with seven prepared cases. It supports importing **Undertow JSON bundles**, comparison, JSON/pseudonymized export and Mermaid. Raw-log parsing, the persistent library and HTML export run in the [local app or CLI](../README.md#run-locally).

The 64-consumer case is separately labeled **synthetic**. It exercises navigation, not an actual 64-worker Kafka deployment.

## Analyze and export from the CLI

After [installing locally](../README.md#run-locally), run:

```bash
# Parse multiple consumer exports. Timestamps need an explicit timezone.
.venv/bin/undertow analyze consumer-a.jsonl consumer-b.jsonl \
  --title "Rebalance after deploy" -o incident.json --html incident.html

# Explicitly interpret timezone-less timestamps as UTC, if justified.
.venv/bin/undertow analyze workers.log --assume-utc -o incident.json

# Portable output and a descriptive comparison.
.venv/bin/undertow export incident.json --redact -o shareable.json
.venv/bin/undertow export incident.json --redact --format html -o shareable.html
.venv/bin/undertow export incident.json --format mermaid -o sequence.mmd
.venv/bin/undertow compare baseline.json incident.json
```

`incident.html` embeds the viewer, styles and evidence in one file, with no external resources. Direct `file://` playback was not exercised by the browser automation; see the [verification boundary](VALIDATION.md#browser-and-package-verification). Mermaid output uses generated consumer aliases and is bounded to 250 lifecycle/progress events; JSON retains the complete capture.

## Input formats and identity

Arroyo does not mandate a log envelope. Supply a timezone and consumer identity through JSONL, GCP resource labels, or this plain format:

```text
2026-10-15T12:00:00.000Z INFO [worker-a] arroyo.processing.processor: New partitions assigned: {Partition(topic=Topic(name='events'), index=0): 42}
```

```json
{"timestamp":"2026-10-15T12:00:00.000Z","consumer":"worker-a","group_id":"ingest","logger":"arroyo.processing.processor","message":"New partitions assigned: {Partition(topic=Topic(name='events'), index=0): 42}"}
```

Optional `instance_id` separates restarts that reuse a consumer name. Group identifiers are included in lane identity. GCP `cluster_name`, `namespace_name`, `pod_name` and `container_name` are combined when no explicit consumer identity is supplied. Separate processes/groups must have distinct identities; Undertow cannot recover missing identity metadata.

See the [complete input and analysis contract](CONTRACT.md) for extension events, offsets, duplicate GCP records, timestamp resolution, bounds and export behavior.

## Interpreting the evidence

At pinned Arroyo **2.46.0**, the consumer-configuration helper defaults to auto-commit. The native DEBUG message saying it waited for offsets to be committed can be emitted even when `commit_offsets()` returns `None`, or when there are no staged offsets. **That message is a commit-path observation, not proof of a broker commit.**

Likewise, an assignment is not proof that a message was processed. Silence can mean idle input, missing logs, disabled logging, a crash or a stall. Undertow reports explicit stuck-detector messages and open callbacks; it does not diagnose silence as a root cause.

Pinned sources: [configuration](https://github.com/getsentry/arroyo/blob/49c24ec2cd0296a11d6d681de14db4f76394ce3e/arroyo/backends/kafka/configuration.py), [consumer commit behavior](https://github.com/getsentry/arroyo/blob/49c24ec2cd0296a11d6d681de14db4f76394ce3e/arroyo/backends/kafka/consumer.py), [processor logs](https://github.com/getsentry/arroyo/blob/49c24ec2cd0296a11d6d681de14db4f76394ce3e/arroyo/processing/processor.py).

## Implementation map

```text
JSONL / GCP / plain logs
          |
     bounded parser        raw source line + structured event
          |
  conservative analyzer    assignment epochs + callbacks + evidence coverage
          |
   +------+--------+
   CLI         local HTTP app + local incident library
   |               |
   +------+--------+
          +-- JSON / pseudonymized case
          +-- Mermaid sequence
          +-- offline HTML / static browser viewer
```

The Python core is the canonical parser. The static viewer recomputes derived analysis from normalized events in JavaScript. Cross-runtime parity tests cover correlation, redaction and Mermaid output. Imported bundles cannot supply their own trusted analysis.
