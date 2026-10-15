# Validation and limits

Local verification date: **2026-10-15**. The recorded cases are actual executions of a synthetic workload, not production Sentry incidents. The 64-consumer navigation case and 100,000-line parser input are separately generated controls.

## Real runtime experiment

Environment: macOS ARM64, Python 3.12.11, Sentry Arroyo 2.46.0, confluent-kafka 2.16.0. Apache Kafka 4.1.0 runs in a disposable local Docker container, pinned to `sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f`. Workers use the KIP-848 consumer protocol. Six partitions and 96 message IDs per scenario.

| Scenario | Producer acknowledgments | Unique processed IDs | Duplicate processing observations | Missing IDs | Longest observed strategy join |
| --- | ---: | ---: | ---: | ---: | ---: |
| Healthy rebalance | 96 | 96 | 0 | 0 | 0.015 s |
| Delayed strategy join | 96 | 96 | 0 | 0 | 3.015 s |
| Processing crash and replacement | 96 | 96 | 0 | 0 | 0.011 s |
| Auto-commit | 96 | 96 | 0 | 0 | 0.000 s at millisecond resolution |

Every scenario independently queried Kafka's committed next offsets: all six partitions reached **16**. All native/structured capture lines parsed without rejection. These finite observations do not prove exactly-once semantics or absence of duplication/loss in other executions.

The delayed join injected a 3,000 ms sleep into a real processing strategy's join method. A separate monotonic measurement recorded **3,004.225 ms**. The parser derived **3,015 ms** from native `Waiting for ... to exit` and `... exited successfully` logs, which also include the rest of the join work. The encompassing observed revocation callback was **3,016 ms**. The real Arroyo stuck detector emitted one report during this controlled delay.

The auto-commit run produced **10 commit-path DEBUG messages** and **zero confirmed-commit instrumentation events**. Undertow leaves first broker-commit timing unknown. The independent final broker query confirmed the final offsets; it does not establish the timing of each commit from the logs.

The crash scenario raises a deliberate exception before a synthetic message callback completes, then starts a replacement worker. It is not a process-kill, network-partition or broker-crash test. The final local broker and all worker processes were removed/stopped by the harness.

Full [summary](evidence/lab-final/summary.json), [baseline](evidence/lab-final/baseline/verification.json), [slow join](evidence/lab-final/slow-drain/verification.json), [crash](evidence/lab-final/crash/verification.json), and [auto-commit](evidence/lab-final/auto/verification.json) retain source SHA-256 hashes, independent observations, counters and exact outcomes. Source JSONL files and complete incident bundles are stored alongside those reports. Local filesystem prefixes in tracebacks are replaced with `<checkout>` and `<home>` before sharing; log messages otherwise retain their native runtime content.

## Controls and automated checks

**27 tests pass locally**, covering:

- Commit-path ambiguity, explicit progress requirements and exact offset handling.
- Partial cooperative revocations, separate group/instance identity, millisecond ties, and incomplete callback evidence.
- GCP/JSONL/plain parsing, duplicate GCP record IDs, missing timestamps and rejected lines.
- Traceback preservation, source identity bounds, inert constructor/HTML input and safe Mermaid generation.
- Pseudonymization and self-contained HTML embedding.
- Local HTTP import/persistence, token requirement, cross-origin rejection, Host checking and invalid case paths.
- Recorded source hashes and full recomputation for all four real scenarios.
- Python/JavaScript analysis parity for all seven shipped cases, plus redaction/Mermaid parity.

The native-only control removes all lab instrumentation from a real capture. Both processing and successful-commit coverage become unknown. The incomplete-capture control truncates the real delayed-join run before completion and produces open-drain/open-revoke findings. Neither control invents a failure event.

The fault harness explicitly checks that the delay actually occurred. Kafka can select either original consumer for revocation, so both original workers are eligible for the delay after the first message batch completes. The final tests do not assume which worker the broker chooses.

## Parser scale

A generated **100,000-line / 64-consumer / 9,999,999-byte** input parsed and analyzed in **0.779 seconds** in one local Python process. Peak process RSS was **225,509,376 bytes**, including input construction and the resulting event graph. This excludes browser rendering and is not a Kafka throughput measurement. [Machine, input hash and limits](evidence/scale.json).

The browser's separate 64-consumer case exercises navigation and filtering. Large DOM views are bounded: 50 evidence rows per page, 128 visible matching lanes, 160 markers per lane and 500 partition epochs. Complete evidence is retained in the exported JSON.

## Browser and package verification

Browser QA covers the landing page, real-case navigation, source inspection, comparison, unknown/incomplete controls, raw-log import, inert script-tag text, local persistence and downloaded exports. At a 390×844 viewport the document had no horizontal overflow; timeline and case strips can scroll independently. [Browser evidence checklist](evidence/browser-qa.json).

The distribution includes a wheel and source archive. The wheel contains the viewer and recorded cases; the source archive excludes private contact notes, virtual environments, local incident storage and Sites credentials. Install and CLI export are checked in a clean environment. Hosted CI additionally checks Linux/macOS core behavior and runs a fresh Linux Kafka lab; consult the actual workflow status for the current revision rather than inferring it from this local report.

## Boundaries

- One local Kafka broker and synthetic, small workloads. No multi-broker failover, remote network, power loss, production load, Rust-runtime execution or Sentry internal deployment.
- Native log parsing is pinned to known Arroyo 2.46.0 patterns. GCP support is format/fixture-tested, not validated against a production cloud export.
- Cross-consumer timestamps assume synchronized source clocks. Display order is not causal ordering.
- Local callback windows do not prove Kafka group stability. Missing observations remain unknown.
- An imported instrumented event is a source assertion. Undertow does not authenticate producers or contact Kafka to verify arbitrary imports.
- Pseudonymization retains timing, offsets and topology counts. Review outputs before sharing.
- No maintainer acceptance, incident-time savings, production suitability, referral or hiring result has been established.
