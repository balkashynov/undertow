# Contributing

Start with a reproducible input and the conclusion the current viewer gets wrong. Include the relevant source format/version and a minimal synthetic or properly sanitized fixture. Do not include production logs, tokens, customer payloads or identifying infrastructure details in public issues.

Keep facts distinct from inferences. A new native event pattern needs a pinned source reference and a negative control. A new derived metric needs explicit evidence requirements and handling for incomplete captures. Changes to analysis must preserve Python/browser parity.

Set up with `python3 -m venv .venv` and `.venv/bin/pip install -e '.[dev]'`. Run `.venv/bin/python -m pytest -q`; Node.js is needed for browser-engine parity. For runtime changes, install `.[lab]` and run `scripts/lab.py` with Docker available. The lab creates only synthetic data and a disposable broker.

Use issue discussion before a broad scope change. No production deployment or performance claim should be added without representative evidence and stated limits.
