from __future__ import annotations

import argparse
import sys

import uvicorn

from mstransfer.client.sender import resolve_inputs, send_batch
from mstransfer.log import console, setup_logging
from mstransfer.server.app import create_app


def parse_target(target: str) -> tuple[str, int]:
    """Parse 'host:port' or 'host' into (host, port)."""
    default_port = 1319
    if ":" in target:
        host, port_str = target.rsplit(":", 1)
        try:
            port = int(port_str)
        except ValueError:
            console.print(f"[red]Invalid port in target: {target}")
            sys.exit(1)
        return host, port
    return target, default_port


def cmd_serve(args: argparse.Namespace) -> None:
    setup_logging()
    app = create_app(output_dir=args.output_dir, store_as=args.store_as)
    console.print(
        f"[bold green]mstransfer server[/] starting on "
        f"[cyan]{args.host}:{args.port}[/] "
        f"(store-as={args.store_as}, output={args.output_dir})"
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


def cmd_upload(args: argparse.Namespace) -> None:
    setup_logging()

    if len(args.targets) < 2:
        console.print("[red]Usage: mstransfer upload <paths...> <target>")
        sys.exit(1)

    *raw_paths, target = args.targets
    host, port = parse_target(target)

    try:
        file_paths = resolve_inputs(raw_paths, recursive=args.recursive)
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}")
        sys.exit(1)

    console.print(
        f"Sending [bold]{len(file_paths)}[/] file(s) to "
        f"[cyan]{host}:{port}[/] (parallel={args.parallel})"
    )

    results = send_batch(file_paths, host, port, parallel=args.parallel)

    ok = sum(1 for r in results if r and r.get("state") == "done")
    fail = len(results) - ok
    if fail:
        console.print(f"\n[green]{ok} succeeded[/], [red]{fail} failed[/]")
        for r in results:
            if r and r.get("state") != "done":
                name = r.get("filename", "?")
                err = r.get("error", r.get("state", "unknown"))
                console.print(f"  [red]- {name}: {err}")
    else:
        console.print(f"\n[green]All {ok} file(s) transferred successfully.")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="mstransfer",
        description=("Transfer mass spectrometry files (mzML/MSZ) between endpoints"),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- serve ---
    lp = sub.add_parser("serve", help="Start the mstransfer server")
    lp.add_argument("--host", default="0.0.0.0", help="Bind address")
    lp.add_argument("--port", type=int, default=1319, help="Listen port")
    lp.add_argument(
        "--output-dir",
        default="./received",
        help="Directory for received files",
    )
    lp.add_argument(
        "--store-as",
        choices=["msz", "mzml"],
        default="msz",
        help="Storage format (default: msz)",
    )
    lp.set_defaults(func=cmd_serve)

    # --- upload ---
    sp = sub.add_parser("upload", help="Upload files to a mstransfer server")
    sp.add_argument(
        "targets",
        nargs="+",
        help="File/directory paths followed by target host[:port]",
    )
    sp.add_argument(
        "--recursive",
        "-r",
        action="store_true",
        help="Recurse into directories",
    )
    sp.add_argument(
        "--parallel",
        "-p",
        type=int,
        default=4,
        help="Concurrent uploads (default: 4)",
    )
    sp.set_defaults(func=cmd_upload)

    args = parser.parse_args()
    args.func(args)
