/* No third-party scripts, analytics, uploads, or remote fonts. */
document.addEventListener("DOMContentLoaded", () => {
  "use strict";
  const $ = (s) => document.querySelector(s),
    $$ = (s) => [...document.querySelectorAll(s)],
    E = window.UndertowEngine;
  const state = {
    case: null,
    baseline: null,
    demos: [],
    local: false,
    token: null,
    page: 0,
    selected: null,
    tab: "timeline",
  };
  const colors = {
    assign: "#5d8967",
    revoke: "#b77a42",
    revoked: "#a88c61",
    drain_start: "#c18547",
    drain_end: "#b39a6c",
    strategy_ready: "#8272b0",
    processed: "#167c70",
    commit_confirmed: "#325c9d",
    commit_attempt: "#9da69a",
    crash: "#be524d",
    stuck: "#be524d",
    commit_error: "#be524d",
    start: "#687b5e",
    stop: "#687b5e",
    pause: "#b77a42",
    note: "#b9c2b2",
  };
  const labels = {
    assign: "Assigned",
    revoke: "Revocation begins",
    revoked: "Revocation complete",
    drain_start: "Strategy join begins",
    drain_end: "Strategy joined",
    strategy_ready: "Strategy initialized",
    processed: "Processed (instrumented)",
    commit_confirmed: "Commit (instrumented)",
    commit_attempt: "Commit path logged",
    crash: "Crash",
    stuck: "Stuck detector",
    commit_error: "Commit error",
    start: "Started",
    stop: "Stopped",
    pause: "Paused",
    resume: "Resumed",
    note: "Log observation",
  };
  const el = (tag, cls, value) => {
    const n = document.createElement(tag);
    if (cls) n.className = cls;
    if (value !== undefined) n.textContent = value;
    return n;
  };
  const sec = (v) =>
    v === null || v === undefined ? "unknown" : `${(v / 1000).toFixed(3)}s`;
  const rel = (t) => sec(t - state.case.analysis.start);
  const clear = (n) => n.replaceChildren();
  let toastTimer;
  function toast(message) {
    const n = $("#toast");
    n.textContent = message;
    n.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => (n.hidden = true), 4500);
  }
  function route() {
    const open = location.hash === "#workbench";
    $("#landing").hidden = open;
    $("#workbench").hidden = !open;
    if (open && state.case) renderTimeline();
  }
  function open() {
    location.hash = "workbench";
    route();
    window.scrollTo(0, 0);
  }
  $$("[data-open]").forEach((b) => b.addEventListener("click", open));
  window.addEventListener("hashchange", route);
  $$(".close-dialog").forEach((b) =>
    b.addEventListener("click", () => b.closest("dialog").close()),
  );
  $$("dialog").forEach((d) =>
    d.addEventListener("click", (e) => {
      if (e.target === d) {
        const r = d.getBoundingClientRect();
        if (
          e.clientX < r.left ||
          e.clientX > r.right ||
          e.clientY < r.top ||
          e.clientY > r.bottom
        )
          d.close();
      }
    }),
  );
  $("#copy-install").onclick = async () => {
    try {
      await navigator.clipboard.writeText($("#install-command").textContent);
      toast("Install commands copied.");
    } catch {
      toast("Select and copy the commands in the terminal panel.");
    }
  };
  async function api(path, body) {
    const response = await fetch(path, {
      method: body ? "POST" : "GET",
      headers: body
        ? {
            "Content-Type": "application/json",
            "X-Undertow-Token": state.token,
          }
        : {},
      body: body ? JSON.stringify(body) : undefined,
    });
    const data = await response.json();
    if (!response.ok) throw Error(data.error || "Request failed");
    return data;
  }
  function setCase(c, verified = false) {
    const safe = E.validate(c);
    if (verified) {
      safe.provenance = c.provenance;
      safe.diagnostics = Array.isArray(c.diagnostics) ? c.diagnostics : [];
      safe.sources = Array.isArray(c.sources) ? c.sources : [];
      safe.analysis.coverage.rejected_lines = safe.diagnostics.length;
    }
    state.case = safe;
    state.page = 0;
    state.selected = null;
    $("#case-title").textContent = safe.title;
    const provenance = $("#case-provenance");
    clear(provenance);
    provenance.append(
      el("span", "tag", String(safe.provenance.type).toUpperCase()),
      document.createTextNode(
        safe.provenance.description || "Imported evidence",
      ),
    );
    $("#zoom").value = 1;
    $("#pan").value = 0;
    $("#consumer-filter").value = "";
    $("#kind-filter").value = "";
    $("#evidence-search").value = "";
    $("#inspector-title").textContent = "Select an event to inspect it";
    clear($("#inspector-content"));
    $("#inspector-content").append(
      el(
        "p",
        "muted",
        "Click a timeline marker, finding, or log row to inspect its source evidence.",
      ),
    );
    render();
  }
  function render() {
    renderMetrics();
    renderTimeline();
    renderPartitions();
    renderEvidence();
    renderFindings();
    renderCompare();
  }
  function renderMetrics() {
    const c = state.case,
      a = c.analysis,
      container = $("#metrics");
    clear(container);
    const ms = E.metrics(c);
    for (const [name, value, sub] of [
      ["Consumers", a.consumers.length, "Observed identities in this capture"],
      [
        "Captured events",
        c.events.length.toLocaleString(),
        `${a.recognized.toLocaleString()} recognized lifecycle/progress events`,
      ],
      [
        "Capture window",
        sec(a.end - a.start),
        "First to last timestamped observation",
      ],
      [
        "Longest revocation",
        sec(ms.max_revoke_ms),
        `${a.revocations.length} observed callbacks · ${ms.open_revocations} incomplete`,
      ],
    ]) {
      const card = el("div", "metric");
      card.append(
        el("div", "metric-label", name),
        el("div", "metric-value", value),
        el("div", "metric-sub", sub),
      );
      container.append(card);
    }
  }
  function svgNode(tag, attrs = {}, content) {
    const n = document.createElementNS("http://www.w3.org/2000/svg", tag);
    for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, String(v));
    if (content !== undefined) n.textContent = content;
    return n;
  }
  function renderTimeline() {
    if (!state.case) return;
    const c = state.case,
      a = c.analysis,
      host = $("#timeline");
    clear(host);
    const filter = $("#consumer-filter").value.toLowerCase(),
      kind = $("#kind-filter").value,
      allConsumers = a.consumers.filter((x) =>
        x.toLowerCase().includes(filter),
      ),
      consumers = allConsumers.slice(0, 128);
    if (!consumers.length) {
      host.append(el("p", "empty", "No consumers match this filter."));
      return;
    }
    const width = Math.max(600, host.clientWidth || 750),
      left = 150,
      right = 20,
      row = 47,
      zoom = Number($("#zoom").value),
      full = Math.max(a.end - a.start, 1),
      span = full / zoom,
      start = a.start + ((full - span) * Number($("#pan").value)) / 100,
      end = start + span,
      x = (t) => left + ((t - start) / span) * (width - left - right);
    const svg = svgNode("svg", {
      viewBox: `0 0 ${width} ${50 + consumers.length * row}`,
      role: "img",
      "aria-label":
        "Consumer timeline. Tab through event markers to inspect their evidence.",
    });
    for (let i = 0; i <= 5; i++) {
      const xx = left + (i * (width - left - right)) / 5;
      svg.append(
        svgNode("line", {
          x1: xx,
          x2: xx,
          y1: 29,
          y2: 50 + consumers.length * row,
          stroke: "#edf0e8",
        }),
        svgNode(
          "text",
          {
            x: xx,
            y: 18,
            "font-size": 8,
            "font-family": "monospace",
            fill: "#8a977f",
            "text-anchor": i === 5 ? "end" : "start",
          },
          sec(start + (i * span) / 5 - a.start),
        ),
      );
    }
    let hiddenMarkers = 0;
    consumers.forEach((consumer, i) => {
      const y = 38 + i * row;
      svg.append(
        svgNode("line", {
          x1: 0,
          x2: width,
          y1: y + row - 5,
          y2: y + row - 5,
          stroke: "#edf0e8",
        }),
      );
      const name = svgNode(
        "text",
        {
          x: 3,
          y: y + 22,
          "font-size": 9,
          "font-family": "monospace",
          fill: "#4e6645",
        },
        consumer.length > 21 ? "…" + consumer.slice(-20) : consumer,
      );
      name.append(svgNode("title", {}, consumer));
      svg.append(name);
      for (const ep of a.epochs.filter(
        (ep) => ep.consumer === consumer && ep.end >= start && ep.start <= end,
      )) {
        const rect = svgNode("rect", {
          x: x(Math.max(ep.start, start)),
          y: y + 8,
          width: Math.max(
            2,
            x(Math.min(ep.end, end)) - x(Math.max(ep.start, start)),
          ),
          height: 19,
          rx: 3,
          fill: "#dce9cf",
          opacity: 0.7,
        });
        rect.append(
          svgNode(
            "title",
            {},
            `${ep.topic} [${ep.partition}] assigned; activity not inferred`,
          ),
        );
        svg.append(rect);
      }
      for (const w of a.revocations.filter(
        (w) => w.consumer === consumer && w.end >= start && w.start <= end,
      )) {
        const r = svgNode("rect", {
          x: x(Math.max(w.start, start)),
          y: y + 5,
          width: Math.max(
            2,
            x(Math.min(w.end, end)) - x(Math.max(w.start, start)),
          ),
          height: 25,
          rx: 3,
          fill: "#ecd9be",
          stroke: w.complete ? "none" : "#bc8c55",
          "stroke-dasharray": "3 2",
        });
        r.append(
          svgNode(
            "title",
            {},
            `Revocation ${sec(w.end - w.start)}${w.complete ? "" : " (no completion observed)"}`,
          ),
        );
        svg.append(r);
      }
      const selected = c.events.filter(
        (e) =>
          e.consumer === consumer &&
          e.ts >= start &&
          e.ts <= end &&
          (kind
            ? e.kind === kind
            : ![
                "note",
                "commit_attempt",
                "processed",
                "commit_confirmed",
              ].includes(e.kind)),
      );
      // Keep lifecycle markers readable. Exact events remain available in Evidence log.
      hiddenMarkers += Math.max(0, selected.length - 160);
      for (const event of selected.slice(0, 160)) {
        const marker = svgNode("circle", {
          cx: x(event.ts),
          cy: y + 17,
          r: state.selected === event.id ? 5.5 : 4,
          fill: colors[event.kind] || "#a0ad98",
          class: "marker",
          tabindex: 0,
          role: "button",
          "aria-label": `${consumer}, ${labels[event.kind] || event.kind}, ${rel(event.ts)}`,
        });
        marker.append(
          svgNode(
            "title",
            {},
            `${labels[event.kind] || event.kind} · ${rel(event.ts)}`,
          ),
        );
        marker.onclick = () => inspect(event);
        marker.onkeydown = (e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            inspect(event);
          }
        };
        svg.append(marker);
      }
    });
    host.append(svg);
    if (allConsumers.length > 128)
      host.append(
        el(
          "p",
          "fineprint",
          "Showing 128 matching lanes. Narrow the consumer filter to inspect the remaining identities.",
        ),
      );
    if (hiddenMarkers)
      host.append(
        el(
          "p",
          "fineprint",
          `${hiddenMarkers} additional markers omitted for readability. Filter/zoom or inspect the complete Evidence log.`,
        ),
      );
    renderEpochSummary();
  }
  function table(headers, rows, onClick) {
    const wrap = el("div", "table-wrap"),
      t = el("table", "data-table"),
      head = el("thead"),
      tr = el("tr");
    headers.forEach((h) => tr.append(el("th", "", h)));
    head.append(tr);
    t.append(head);
    const body = el("tbody");
    rows.forEach((row, i) => {
      const r = el("tr", onClick ? "clickable" : "");
      for (const v of row) {
        const td = el(
          "td",
          typeof v === "object" && v?.class ? v.class : "",
          typeof v === "object" ? v.value : v,
        );
        r.append(td);
      }
      if (onClick) {
        r.tabIndex = 0;
        r.onclick = () => onClick(i);
        r.onkeydown = (e) => {
          if (e.key === "Enter") onClick(i);
        };
      }
      body.append(r);
    });
    t.append(body);
    wrap.append(t);
    return wrap;
  }
  function renderEpochSummary() {
    const host = $("#epoch-table");
    clear(host);
    const epochs = state.case.analysis.epochs.filter((e) =>
      e.consumer
        .toLowerCase()
        .includes($("#consumer-filter").value.toLowerCase()),
    );
    host.append(
      el(
        "p",
        "table-caption",
        "AFTER ASSIGNMENT · instrumented observations only",
      ),
    );
    host.append(
      table(
        ["Consumer / partition", "First processed", "First confirmed commit"],
        epochs
          .slice(0, 8)
          .map((e) => [
            `${e.consumer} · p${e.partition}`,
            {
              value:
                e.first_processed === null
                  ? "unknown"
                  : sec(e.first_processed - e.start),
              class: e.first_processed === null ? "unknown" : "",
            },
            {
              value:
                e.first_commit === null
                  ? "unknown"
                  : sec(e.first_commit - e.start),
              class: e.first_commit === null ? "unknown" : "",
            },
          ]),
      ),
    );
    if (epochs.length > 8)
      host.append(
        el(
          "p",
          "fineprint",
          `${epochs.length - 8} more epochs in Partition handoffs.`,
        ),
      );
  }
  function renderPartitions() {
    const host = $("#partition-table");
    clear(host);
    const allEpochs = state.case.analysis.epochs,
      epochs = allEpochs.slice(0, 500);
    if (allEpochs.length > 500)
      host.append(
        el(
          "p",
          "fineprint",
          `Showing the first 500 of ${allEpochs.length} assignment epochs. Full evidence remains in the JSON export.`,
        ),
      );
    host.append(
      table(
        [
          "Topic / partition",
          "Consumer",
          "Assigned at",
          "Ended at",
          "End evidence",
          "First processed",
          "First commit",
        ],
        epochs.map((e) => [
          `${e.topic} [${e.partition}]`,
          { value: e.consumer, class: "consumer-cell" },
          rel(e.start),
          rel(e.end),
          e.end_reason,
          e.first_processed === null
            ? "unknown"
            : sec(e.first_processed - e.start),
          e.first_commit === null ? "unknown" : sec(e.first_commit - e.start),
        ]),
        (i) =>
          inspect(
            state.case.events.find((x) => x.id === epochs[i].assignment_event),
          ),
      ),
    );
    if (!epochs.length)
      host.append(
        el(
          "p",
          "empty",
          "No partition assignments were parsed from this capture.",
        ),
      );
  }
  function renderEvidence() {
    const host = $("#evidence-table");
    clear(host);
    const q = $("#evidence-search").value.toLowerCase();
    const events = state.case.events.filter((e) =>
      `${e.message} ${e.consumer} ${e.kind}`.toLowerCase().includes(q),
    );
    const pages = Math.max(1, Math.ceil(events.length / 50));
    state.page = Math.min(state.page, pages - 1);
    const slice = events.slice(state.page * 50, state.page * 50 + 50);
    host.append(
      table(
        ["Time", "Consumer", "Event", "Message"],
        slice.map((e) => [
          rel(e.ts),
          { value: e.consumer, class: "consumer-cell" },
          labels[e.kind] || e.kind,
          { value: e.message.slice(0, 260), class: "message-cell" },
        ]),
        (i) => inspect(slice[i]),
      ),
    );
    $("#page-label").textContent =
      `${events.length.toLocaleString()} events · page ${state.page + 1} of ${pages}`;
    $("#prev-page").disabled = state.page === 0;
    $("#next-page").disabled = state.page >= pages - 1;
    const diagnostics = state.case.diagnostics || [];
    $("#diagnostic-label").textContent =
      `Parser diagnostics · ${diagnostics.length} rejected lines`;
    clear($("#diagnostic-list"));
    for (const d of diagnostics.slice(0, 100))
      $("#diagnostic-list").append(
        el("p", "", `${d.source}:${d.line} · ${d.reason}`),
      );
    if (diagnostics.length > 100)
      $("#diagnostic-list").append(
        el(
          "p",
          "",
          `Showing 100 of ${diagnostics.length}; full diagnostics in original JSON.`,
        ),
      );
  }
  function renderFindings() {
    const a = state.case.analysis,
      coverage = $("#coverage");
    clear(coverage);
    const box = el("div", "coverage");
    box.append(el("h3", "", "EVIDENCE COVERAGE"));
    for (const [name, value] of [
      ["Processing signals", a.coverage.processed ? "Instrumented" : "Unknown"],
      [
        "Broker commit assertions",
        a.coverage.confirmed_commits ? "Instrumented" : "Unknown",
      ],
      ["Rejected lines", a.coverage.rejected_lines],
    ]) {
      const row = el("div");
      row.append(el("span", "", name), el("b", "", value));
      box.append(row);
    }
    coverage.append(box);
    const host = $("#findings");
    clear(host);
    const order = { error: 0, warning: 1, info: 2 };
    const findings = [...a.findings].sort(
      (a, b) => order[a.severity] - order[b.severity] || a.start - b.start,
    );
    for (const f of findings.slice(0, 30)) {
      const button = el("button", `finding ${f.severity}`);
      button.append(
        el("span", "", f.code.replaceAll("_", " ")),
        el("p", "", f.message),
        el(
          "small",
          "",
          f.consumer
            ? `${f.consumer} · ${rel(f.start)}`
            : "Capture-wide interpretation",
        ),
      );
      button.onclick = () => {
        const e = state.case.events.find((e) => e.id === f.event_ids[0]);
        if (e) inspect(e);
        else toast(f.message);
      };
      host.append(button);
    }
    if (!findings.length)
      host.append(
        el(
          "p",
          "empty",
          "No flagged transitions in this capture. This does not establish cluster health.",
        ),
      );
    if (findings.length > 30)
      host.append(
        el(
          "p",
          "fineprint",
          `${findings.length - 30} more findings are retained in the JSON case.`,
        ),
      );
    clear($("#limits"));
    state.case.limits.forEach((l) => $("#limits").append(el("li", "", l)));
  }
  function inspect(event) {
    if (!event) return;
    state.selected = event.id;
    $("#inspector-title").textContent = labels[event.kind] || event.kind;
    const host = $("#inspector-content");
    clear(host);
    const dl = el("dl");
    for (const [k, v] of Object.entries({
      Consumer: event.consumer,
      Time: `${new Date(event.ts).toISOString()} (+${rel(event.ts)})`,
      Source: `${event.source}:${event.line}`,
      Evidence:
        event.evidence === "instrumented"
          ? "Instrumented source assertion"
          : "Native log observation",
      Logger: event.logger || "not supplied",
      Partitions: event.partitions.length
        ? event.partitions
            .map(
              (p) =>
                `${p.topic}[${p.partition}]${"offset" in p ? ` @ ${p.offset}` : ""}`,
            )
            .join(", ")
        : "not present",
    })) {
      dl.append(el("dt", "", k), el("dd", "", v));
    }
    host.append(dl, el("pre", "", event.message));
    renderTimeline();
    $("#inspector").scrollIntoView({ behavior: "smooth", block: "nearest" });
  }
  function renderCompare() {
    const host = $("#comparison");
    clear(host);
    if (!state.baseline) {
      host.append(
        el(
          "p",
          "empty",
          "Choose a recorded baseline above to compare it with the current capture.",
        ),
      );
      return;
    }
    host.append(
      el(
        "p",
        "compare-heading",
        `${state.baseline.title} → ${state.case.title}`,
      ),
    );
    const a = E.metrics(state.baseline),
      b = E.metrics(state.case);
    const names = {
      events: "Captured events",
      consumers: "Observed consumers",
      crashes: "Logged crash events",
      open_revocations: "Incomplete revocations",
      max_revoke_ms: "Longest completed revocation",
      max_first_processing_ms: "Max assignment → first processing",
      max_first_confirmed_commit_ms: "Max assignment → first commit",
    };
    host.append(
      table(
        ["Measurement", "Baseline", "Candidate", "Δ candidate"],
        Object.keys(a).map((k) => {
          const format = (v) =>
            v === null ? "unknown" : k.endsWith("_ms") ? sec(v) : v;
          const delta = a[k] === null || b[k] === null ? null : b[k] - a[k];
          return [
            names[k],
            format(a[k]),
            format(b[k]),
            {
              value:
                delta === null
                  ? "unknown"
                  : `${delta > 0 ? "+" : ""}${format(delta)}`,
              class: k.endsWith("_ms")
                ? delta > 0
                  ? "delta-bad"
                  : delta < 0
                    ? "delta-good"
                    : ""
                : "",
            },
          ];
        }),
      ),
    );
  }
  async function loadDemo(path) {
    const response = await fetch(path);
    if (!response.ok)
      throw Error(
        "Recorded capture is unavailable. Import a bundle or run the local lab.",
      );
    const c = await response.json();
    setCase(c, true);
    return c;
  }
  async function initialize() {
    if (window.UNDERTOW_EMBEDDED) {
      setCase(window.UNDERTOW_EMBEDDED, true);
      state.demos = [];
      $("#baseline-select").append(
        el("option", "", "Import a baseline bundle"),
      );
      open();
      $("#mode-pill").textContent = "Offline investigation";
      $("#mode-description").textContent =
        "Self-contained evidence. No network required.";
      $("#demo-list").append(
        el("p", "fineprint", "This export contains one complete capture."),
      );
      return;
    }
    try {
      const config = await api("/api/config");
      if (config.mode === "local") {
        state.local = true;
        state.token = config.token;
        $("#mode-pill").textContent = "Local app · connected";
        $("#mode-description").textContent =
          "Logs are parsed and saved on this computer.";
        $("#import-explainer").textContent =
          "Parse raw logs locally, or reopen an Undertow bundle. Files never leave this computer.";
      }
    } catch {}
    try {
      const response = await fetch("demos/index.json");
      if (!response.ok) throw Error("No recorded cases installed yet.");
      state.demos = await response.json();
      const list = $("#demo-list"),
        select = $("#baseline-select");
      clear(list);
      clear(select);
      const placeholder = el("option", "", "Choose baseline…");
      placeholder.value = "";
      select.append(placeholder);
      for (const d of state.demos) {
        const b = el("button", "side-item", d.label);
        b.onclick = async () => {
          try {
            await loadDemo(d.path);
            open();
            $$("#demo-list .side-item").forEach((x) =>
              x.classList.toggle("active", x === b),
            );
          } catch (e) {
            toast(e.message);
          }
        };
        list.append(b);
        const option = el("option", "", d.label);
        option.value = d.path;
        select.append(option);
      }
      if (state.demos.length) {
        await loadDemo(state.demos[0].path);
        list.firstChild.classList.add("active");
      }
    } catch (e) {
      $("#case-title").textContent = "Start an investigation";
      $("#case-provenance").textContent = e.message;
    }
    route();
  }
  $$("[data-tab]").forEach(
    (b) =>
      (b.onclick = () => {
        state.tab = b.dataset.tab;
        $$("[data-tab]").forEach((x) =>
          x.setAttribute("aria-selected", x === b ? "true" : "false"),
        );
        $$("[role=tabpanel]").forEach(
          (x) => (x.hidden = x.id !== `tab-${state.tab}`),
        );
        if (state.tab === "timeline") renderTimeline();
      }),
  );
  for (const id of ["consumer-filter", "kind-filter", "zoom", "pan"])
    $("#" + id).addEventListener("input", renderTimeline);
  $("#reset-view").onclick = () => {
    $("#zoom").value = 1;
    $("#pan").value = 0;
    $("#consumer-filter").value = "";
    $("#kind-filter").value = "";
    renderTimeline();
  };
  $("#evidence-search").oninput = () => {
    state.page = 0;
    renderEvidence();
  };
  $("#prev-page").onclick = () => {
    state.page--;
    renderEvidence();
  };
  $("#next-page").onclick = () => {
    state.page++;
    renderEvidence();
  };
  $("#baseline-select").onchange = async (e) => {
    if (!e.target.value) {
      state.baseline = null;
      renderCompare();
      return;
    }
    try {
      const r = await fetch(e.target.value);
      state.baseline = E.validate(await r.json());
      renderCompare();
    } catch (err) {
      toast(err.message);
    }
  };
  $("#baseline-import").onclick = () => $("#baseline-file").click();
  $("#baseline-file").onchange = async (e) => {
    try {
      const file = e.target.files[0];
      if (!file) return;
      if (file.size > 20 * 1024 * 1024) throw Error("Baseline exceeds 20 MiB.");
      state.baseline = E.validate(JSON.parse(await file.text()));
      $("#baseline-select").value = "";
      renderCompare();
      toast("Baseline imported.");
    } catch (err) {
      toast(err.message);
    }
    e.target.value = "";
  };
  const showImport = () => {
    $("#import-error").textContent = "";
    $("#import-dialog").showModal();
  };
  for (const id of ["import-button", "import-side"])
    $("#" + id).onclick = showImport;
  $("#import-files").onchange = (e) =>
    ($("#file-list").textContent = [...e.target.files]
      .map((f) => `${f.name} (${Math.ceil(f.size / 1024)} KiB)`)
      .join(" · "));
  $("#import-form").onsubmit = async (e) => {
    e.preventDefault();
    const button = $("#analyze-button");
    button.disabled = true;
    $("#import-error").textContent = "";
    try {
      const files = [...$("#import-files").files],
        pasted = $("#import-text").value;
      if (
        files.reduce((n, f) => n + f.size, 0) + new Blob([pasted]).size >
        20 * 1024 * 1024
      )
        throw Error("Capture exceeds the 20 MiB input limit.");
      const sources = await Promise.all(
        files.map(async (f) => ({ name: f.name, content: await f.text() })),
      );
      if (pasted.trim()) sources.push({ name: "pasted.log", content: pasted });
      if (!sources.length) throw Error("Choose files or paste a capture.");
      let bundle = null;
      if (sources.length === 1) {
        try {
          const data = JSON.parse(sources[0].content);
          if (data.schema === "undertow.incident/v1") bundle = data;
        } catch {}
      }
      if (bundle) {
        if (state.local) {
          const result = await api("/api/import", bundle);
          setCase(result.case, true);
        } else setCase(bundle);
      } else {
        if (!state.local)
          throw Error(
            "Raw log parsing runs locally: install Undertow and run undertow serve. This hosted/offline viewer accepts Undertow JSON bundles.",
          );
        const result = await api("/api/analyze", {
          sources,
          title: $("#import-title").value || "Imported incident",
          assume_utc: $("#assume-utc").checked,
        });
        setCase(result.case, true);
      }
      $("#import-dialog").close();
      open();
      toast(
        state.local
          ? "Incident analyzed and saved locally."
          : "Incident imported in this tab.",
      );
    } catch (err) {
      $("#import-error").textContent = err.message;
    } finally {
      button.disabled = false;
    }
  };
  function download(content, name, type = "application/json") {
    const url = URL.createObjectURL(new Blob([content], { type })),
      a = el("a");
    a.href = url;
    a.download = name;
    document.body.append(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  $("#export-button").onclick = () => {
    if (!state.case) return toast("Open a capture first.");
    $("#export-dialog").showModal();
  };
  $$("[data-export]").forEach(
    (b) =>
      (b.onclick = async () => {
        if (!state.case) return;
        try {
          const type = b.dataset.export;
          if (type === "json")
            download(
              JSON.stringify(state.case, null, 2),
              "undertow-incident.json",
            );
          if (type === "redacted")
            download(
              JSON.stringify(E.redact(state.case), null, 2),
              "undertow-pseudonymized.json",
            );
          if (type === "mermaid")
            download(
              E.mermaid(state.case),
              "undertow-sequence.mmd",
              "text/plain",
            );
          if (type === "html") {
            if (!state.local)
              throw Error(
                "Use the local app or CLI: undertow export incident.json --format html -o incident.html",
              );
            const r = await fetch("/api/html", {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "X-Undertow-Token": state.token,
              },
              body: JSON.stringify(state.case),
            });
            if (!r.ok) throw Error("HTML export failed");
            download(await r.text(), "undertow-incident.html", "text/html");
          }
          toast("Export prepared. Review it before sharing.");
        } catch (e) {
          toast(e.message);
        }
      }),
  );
  $("#library-side").onclick = async () => {
    const host = $("#library-list");
    clear(host);
    $("#library-dialog").showModal();
    if (!state.local) {
      host.append(
        el(
          "p",
          "muted",
          "The incident library is available in the local app. Browser imports live only in this tab; export a bundle to keep them.",
        ),
      );
      return;
    }
    try {
      const cases = await api("/api/cases");
      if (!cases.length)
        host.append(
          el(
            "p",
            "empty",
            "No saved incidents yet. Import a raw capture or an Undertow bundle to save it here.",
          ),
        );
      for (const c of cases) {
        const b = el("button", "library-case");
        b.append(
          el("strong", "", c.title),
          el("span", "", `${c.consumers} consumers · ${c.events} events`),
        );
        b.onclick = async () => {
          try {
            setCase(await api(`/api/cases/${c.id}`), true);
            $("#library-dialog").close();
            open();
          } catch (e) {
            toast(e.message);
          }
        };
        host.append(b);
      }
    } catch (e) {
      host.append(el("p", "form-error", e.message));
    }
  };
  $("#case-home").onclick = () => {
    open();
    $("[data-tab=timeline]").click();
  };
  window.addEventListener("resize", () => {
    if (!$("#workbench").hidden) renderTimeline();
  });
  initialize();
});
