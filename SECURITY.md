# Security and privacy

Undertow is a local incident-analysis tool, not a multi-user hosted ingestion service. Do not expose its HTTP server through a reverse proxy or public tunnel. It binds to 127.0.0.1 and checks Host, Origin, request bounds and a per-session token. These safeguards do not protect against other software already running with access to your local account.

The static demo has no ingestion backend or analytics. Raw log parsing runs in the local CLI/server. Browser bundle imports remain in memory until export or navigation. Local imports persist under the configured case directory. That directory is not encrypted by Undertow; use your system's normal access controls and disk protection.

Original bundles and HTML exports contain raw messages, tracebacks and identifiers. Inspect them before sharing. Pseudonymization strips arbitrary text and names but preserves timing, offsets, partition counts and structure; these can still identify workloads. Never assume that a pseudonymized file is suitable for public release without review.

Report a vulnerability privately through the repository's GitHub security reporting feature when available. Avoid publishing sensitive examples. There is no arbitrary command execution, remote-cluster configuration, or executable parser-plugin endpoint in the app.
