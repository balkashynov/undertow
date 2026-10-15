from __future__ import annotations

import argparse
import json
from pathlib import Path

from .core import InputError, comparison, ingest, mermaid, redact, validate_bundle


def main():
    parser = argparse.ArgumentParser(
        description="Undertow: evidence-first consumer incident workbench"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser(
        "analyze", help="Analyze JSONL, GCP JSON or documented plain logs"
    )
    run.add_argument("files", nargs="+", type=Path)
    run.add_argument("--title", default="Consumer incident")
    run.add_argument("--assume-utc", action="store_true")
    run.add_argument("--redact", action="store_true")
    run.add_argument("--output", "-o", type=Path, required=True)
    run.add_argument("--html", type=Path)
    export = sub.add_parser(
        "export", help="Export an incident as JSON, standalone HTML or Mermaid"
    )
    export.add_argument("file", type=Path)
    export.add_argument("--format", choices=["json", "html", "mermaid"], default="json")
    export.add_argument("--redact", action="store_true")
    export.add_argument("--output", "-o", type=Path, required=True)
    compare = sub.add_parser("compare", help="Compare two recorded incidents")
    compare.add_argument("baseline", type=Path)
    compare.add_argument("candidate", type=Path)
    server = sub.add_parser("serve", help="Open the local web app")
    server.add_argument("--port", type=int, default=8767)
    server.add_argument("--data-dir", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "serve":
            from .server import serve

            return serve(args.port, args.data_dir)
        if args.command == "compare":
            result = comparison(
                validate_bundle(json.loads(args.baseline.read_text())),
                validate_bundle(json.loads(args.candidate.read_text())),
            )
            print(json.dumps(result, indent=2))
            return
        if args.command == "analyze":
            case = ingest(
                [{"name": f.name, "content": f.read_text()} for f in args.files],
                args.title,
                args.assume_utc,
            )
        else:
            case = validate_bundle(json.loads(args.file.read_text()))
        if args.redact:
            case = redact(case)
        fmt = getattr(args, "format", "json")
        if fmt == "html":
            from .server import standalone_html

            content = standalone_html(case)
        elif fmt == "mermaid":
            content = mermaid(case)
        else:
            content = json.dumps(case, indent=2) + "\n"
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(content)
        if getattr(args, "html", None):
            from .server import standalone_html

            args.html.write_text(standalone_html(case))
        print(
            f"{len(case['events'])} events, {len(case['analysis']['consumers'])} consumers, {len(case['diagnostics'])} rejected lines -> {args.output}"
        )
    except (InputError, OSError, ValueError) as exc:
        parser.exit(2, f"undertow: {exc}\n")


if __name__ == "__main__":
    main()
