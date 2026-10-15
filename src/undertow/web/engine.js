/* Pure browser analysis. Kept in parity with core.py by cross-runtime tests. */
(() => {
  "use strict";
  const kinds = new Set([
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
  ]);
  const cmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
  const clone = (x) => JSON.parse(JSON.stringify(x));
  function validate(data) {
    if (
      !data ||
      data.schema !== "undertow.incident/v1" ||
      !Array.isArray(data.events) ||
      !data.events.length ||
      data.events.length > 100000
    )
      throw Error("Expected an Undertow v1 incident with 1 to 100,000 events.");
    const ids = new Set();
    const required = [
      "consumer",
      "evidence",
      "id",
      "kind",
      "level",
      "line",
      "logger",
      "message",
      "partitions",
      "source",
      "ts",
    ]
      .sort()
      .join(",");
    for (const e of data.events) {
      if (!e || Object.keys(e).sort().join(",") !== required)
        throw Error("Unexpected event fields.");
      if (
        !e ||
        !kinds.has(e.kind) ||
        !["log", "instrumented"].includes(e.evidence) ||
        !Number.isSafeInteger(e.ts) ||
        e.ts < 0 ||
        e.ts > 32503680000000 ||
        !Number.isInteger(e.line) ||
        e.line < 1
      )
        throw Error("Invalid event fields.");
      for (const k of [
        "id",
        "consumer",
        "message",
        "source",
        "level",
        "logger",
      ])
        if (
          typeof e[k] !== "string" ||
          e[k].length > (k === "message" ? 64000 : 250)
        )
          throw Error("Invalid event text.");
      if (!e.consumer || ids.has(e.id))
        throw Error("Missing consumer or duplicate event ID.");
      ids.add(e.id);
      if (!Array.isArray(e.partitions) || e.partitions.length > 4096)
        throw Error("Invalid partitions.");
      for (const p of e.partitions)
        if (
          !p ||
          Object.keys(p).some(
            (k) => !["topic", "partition", "offset"].includes(k),
          ) ||
          typeof p.topic !== "string" ||
          !p.topic.length ||
          p.topic.length > 249 ||
          !Number.isInteger(p.partition) ||
          p.partition < 0 ||
          p.partition > 2147483647 ||
          ("offset" in p && !Number.isSafeInteger(p.offset))
        )
          throw Error(
            "Invalid partition or offset. Browser supports exact offsets up to 2^53-1.",
          );
      if (
        ["processed", "commit_confirmed"].includes(e.kind) &&
        (!e.partitions.length ||
          e.partitions.some((p) => !("offset" in p) || p.offset < 0))
      )
        throw Error("Progress/commit events require nonnegative next offsets.");
      if (
        ["processed", "commit_confirmed"].includes(e.kind) &&
        e.evidence !== "instrumented"
      )
        throw Error("Progress/commit events must be instrumented assertions.");
    }
    return analyze(
      data.events,
      String(data.title || "Imported incident").slice(0, 180),
      [],
      [],
      {
        type: "imported-bundle",
        description:
          "Imported evidence; derived analysis recomputed in this browser",
      },
    );
  }
  function analyze(
    input,
    title = "Untitled incident",
    diagnostics = [],
    sources = [],
    provenance = {
      type: "user-supplied",
      description: "Unverified source assertions",
    },
  ) {
    const events = clone(input).sort(
        (a, b) =>
          a.ts - b.ts ||
          cmp(a.source, b.source) ||
          a.line - b.line ||
          cmp(a.id, b.id),
      ),
      start = events[0]?.ts || 0,
      end = events.at(-1)?.ts || 0;
    const lanes = new Map(),
      epochs = [],
      windows = [],
      findings = [],
      counts = {};
    for (const e of events) {
      if (!lanes.has(e.consumer)) lanes.set(e.consumer, []);
      lanes.get(e.consumer).push(e);
      counts[e.kind] = (counts[e.kind] || 0) + 1;
    }
    for (const consumer of [...lanes.keys()].sort(cmp)) {
      const active = new Map();
      let revoke = null,
        drain = null,
        last = null;
      for (const e of lanes.get(consumer)) {
        const k = e.kind,
          t = e.ts;
        if (last !== null && t - last > 5000)
          findings.push({
            severity: "info",
            code: "observation_gap",
            consumer,
            start: last,
            end: t,
            event_ids: [e.id],
            message: `No recorded events for ${((t - last) / 1000).toFixed(2)}s. Silence alone does not establish a stall.`,
          });
        last = t;
        if (k === "assign")
          for (const p of e.partitions) {
            const key = JSON.stringify([p.topic, p.partition]);
            if (active.has(key))
              Object.assign(active.get(key), {
                end: t,
                end_reason: "reassigned",
              });
            const ep = {
              consumer,
              ...p,
              start: t,
              end,
              end_reason: "observation_end",
              first_processed: null,
              first_commit: null,
              assignment_event: e.id,
            };
            epochs.push(ep);
            active.set(key, ep);
          }
        else if (k === "revoke") {
          if (revoke) windows.push({ ...revoke, end: t, complete: false });
          revoke = {
            consumer,
            start: t,
            event_id: e.id,
            partitions: e.partitions,
          };
          for (const p of e.partitions) {
            const key = JSON.stringify([p.topic, p.partition]);
            if (active.has(key)) {
              Object.assign(active.get(key), { end: t, end_reason: "revoke" });
              active.delete(key);
            }
          }
        } else if (k === "revoked" && revoke) {
          windows.push({ ...revoke, end: t, complete: true });
          revoke = null;
        } else if (k === "processed" || k === "commit_confirmed") {
          for (const p of e.partitions) {
            const ep = active.get(JSON.stringify([p.topic, p.partition])),
              key = k === "processed" ? "first_processed" : "first_commit";
            if (ep && ep[key] === null) ep[key] = t;
          }
        } else if (k === "drain_start") drain = e;
        else if (k === "drain_end" && drain) {
          const duration = t - drain.ts;
          findings.push({
            severity: duration > 1000 ? "warning" : "info",
            code: "strategy_drain",
            consumer,
            start: drain.ts,
            end: t,
            event_ids: [drain.id, e.id],
            message: `Strategy join took ${(duration / 1000).toFixed(3)}s in the recorded log clock.`,
          });
          drain = null;
        }
        if (k === "crash" || k === "stop") {
          for (const ep of active.values())
            Object.assign(ep, { end: t, end_reason: k });
          active.clear();
        }
        const messages = {
          crash:
            "Crash/shutdown exception recorded. Inspect this event and neighboring evidence.",
          stuck:
            "Arroyo's stuck detector reported the main thread. This is detector evidence, not a diagnosis of the cause.",
          commit_error:
            "A commit failure was logged. Later recovery and broker state need separate evidence.",
        };
        if (messages[k])
          findings.push({
            severity: "error",
            code: k,
            consumer,
            start: t,
            end: t,
            event_ids: [e.id],
            message: messages[k],
          });
      }
      if (revoke) windows.push({ ...revoke, end, complete: false });
      if (drain)
        findings.push({
          severity: "warning",
          code: "open_drain",
          consumer,
          start: drain.ts,
          end,
          event_ids: [drain.id],
          message:
            "Strategy join began but no completion is present. The capture may be incomplete.",
        });
    }
    for (const w of windows)
      if (!w.complete)
        findings.push({
          severity: "warning",
          code: "open_revoke",
          consumer: w.consumer,
          start: w.start,
          end: w.end,
          event_ids: [w.event_id],
          message:
            "Revocation callback has no observed completion in this capture.",
        });
    if (counts.commit_attempt)
      findings.push({
        severity: "info",
        code: "commit_ambiguity",
        consumer: null,
        start,
        end,
        event_ids: [],
        message: `${counts.commit_attempt} commit-path logs do not prove broker commits. Auto-commit and empty mappings can produce the same log.`,
      });
    return {
      schema: "undertow.incident/v1",
      title,
      events,
      diagnostics,
      sources,
      provenance,
      analysis: {
        start,
        end,
        consumers: [...lanes.keys()].sort(cmp),
        epochs,
        revocations: windows,
        findings: findings.sort(
          (a, b) => a.start - b.start || cmp(a.code, b.code),
        ),
        counts,
        recognized: events.length - (counts.note || 0),
        coverage: {
          processed: !!counts.processed,
          confirmed_commits: !!counts.commit_confirmed,
          rejected_lines: diagnostics.length,
        },
      },
      limits: [
        "Cross-consumer alignment assumes synchronized source clocks; no clock correction is inferred.",
        "Revocation windows are observed callbacks, not Kafka group-generation barriers.",
        "Missing processing or commit evidence means unknown, not zero latency or a stall.",
        "Custom events are source assertions. Only documented lab probes have been independently checked.",
      ],
    };
  }
  function redact(bundle) {
    const events = clone(bundle.events),
      consumers = [...new Set(events.map((e) => e.consumer))].sort(cmp),
      topics = [
        ...new Set(events.flatMap((e) => e.partitions.map((p) => p.topic))),
      ].sort(cmp),
      origin = Math.min(...events.slice(0, 1).map((e) => e.ts));
    events.forEach((e, i) => {
      Object.assign(e, {
        id: `e${i + 1}`,
        consumer: `consumer-${String(consumers.indexOf(e.consumer) + 1).padStart(2, "0")}`,
        source: "redacted",
        line: i + 1,
        logger: "redacted",
        message: `${e.kind} (original message removed)`,
        ts: 946684800000 + e.ts - origin,
        level: e.kind === "crash" ? "ERROR" : "INFO",
      });
      for (const p of e.partitions)
        p.topic = `topic-${String(topics.indexOf(p.topic) + 1).padStart(2, "0")}`;
    });
    return analyze(events, "Pseudonymized incident", [], [], {
      type: "pseudonymized",
      description:
        "Names and text removed; relative timing, offsets and partition counts retained",
    });
  }
  function mermaid(bundle) {
    const consumers = bundle.analysis.consumers,
      lines = [
        "sequenceDiagram",
        "    autonumber",
        ...consumers.map(
          (c, i) => `    participant C${i + 1} as Consumer ${i + 1}`,
        ),
      ],
      selected = bundle.events.filter(
        (e) => !["note", "commit_attempt"].includes(e.kind),
      );
    for (const e of selected.slice(0, 250))
      lines.push(
        `    Note over C${consumers.indexOf(e.consumer) + 1}: ${((e.ts - bundle.analysis.start) / 1000).toFixed(3)}s ${e.kind}`,
      );
    if (selected.length > 250)
      lines.push(
        "    %% Truncated to 250 events; use JSON for complete evidence.",
      );
    return lines.join("\n") + "\n";
  }
  function metrics(c) {
    const a = c.analysis,
      rev = a.revocations.filter((w) => w.complete).map((w) => w.end - w.start),
      proc = a.epochs
        .filter((e) => e.first_processed !== null)
        .map((e) => e.first_processed - e.start),
      comm = a.epochs
        .filter((e) => e.first_commit !== null)
        .map((e) => e.first_commit - e.start);
    const maximum = (x) =>
      x.length ? x.reduce((a, b) => Math.max(a, b)) : null;
    return {
      events: c.events.length,
      consumers: a.consumers.length,
      crashes: a.counts.crash || 0,
      open_revocations: a.revocations.filter((w) => !w.complete).length,
      max_revoke_ms: maximum(rev),
      max_first_processing_ms: maximum(proc),
      max_first_confirmed_commit_ms: maximum(comm),
    };
  }
  const api = { validate, analyze, redact, mermaid, metrics };
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  if (typeof window !== "undefined") window.UndertowEngine = api;
})();
