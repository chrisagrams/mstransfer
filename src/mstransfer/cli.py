from __future__ import annotations

import argparse
import socket
import sys
from typing import TYPE_CHECKING

import httpx
import uvicorn
from rich.live import Live
from rich.table import Table

from mstransfer.client.sender import resolve_inputs, send_batch

if TYPE_CHECKING:
    from pathlib import Path

    from rich.progress import TaskID

from mstransfer.log import (
    console,
    make_file_progress,
    make_overall_progress,
    setup_logging,
)
from mstransfer.server.app import create_app
from mstransfer.server.models import TransferState, UploadResponse


def parse_target(target: str) -> str:
    """Parse a target string into a base URL.

    Accepts formats like:
      - host              → http://host:1319
      - host:port         → http://host:port
      - http://host:port  → http://host:port  (passed through)
      - https://host:port → https://host:port (passed through)
    """
    default_port = 1319

    # If the target already has a scheme, use it as-is.
    if target.startswith(("http://", "https://")):
        return target.rstrip("/")

    if ":" in target:
        host, port_str = target.rsplit(":", 1)
        try:
            port = int(port_str)
        except ValueError:
            console.print(f"[red]Invalid port in target: {target}")
            sys.exit(1)
        return f"http://{host}:{port}"
    return f"http://{target}:{default_port}"


def cmd_serve(args: argparse.Namespace) -> None:
    setup_logging()

    # Fail fast if the port is already in use.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((args.host, args.port))
        except OSError:
            console.print(
                f"[red]Port {args.port} is already in use. "
                "Is another mstransfer server running?"
            )
            sys.exit(1)

    app = create_app(output_dir=args.output_dir, store_as=args.store_as)
    console.print(
        f"[bold green]mstransfer server[/] starting on "
        f"[cyan]{args.host}:{args.port}[/] "
        f"(store-as={args.store_as}, output={args.output_dir})"
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


class UploadProgressDisplay:
    """Rich-based implementation of BatchProgressCallback for the CLI."""

    def __init__(self, total_files: int) -> None:
        self.overall = make_overall_progress()
        self.files = make_file_progress()
        self.overall_task = self.overall.add_task("Transferring", total=total_files)
        self.table = Table.grid()
        self.table.add_row(self.overall)
        self.table.add_row(self.files)
        self._task_ids: dict[int, TaskID] = {}

    def file_started(
        self, index: int, file_path: Path, total_bytes: int | None,
    ) -> None:
        task_id = self.files.add_task(file_path.name, total=total_bytes)
        self._task_ids[index] = task_id

    def file_progress(self, index: int, delta: int) -> None:
        self.files.advance(self._task_ids[index], delta)

    def file_done(self, index: int, result: UploadResponse) -> None:
        task_id = self._task_ids[index]
        desc = self.files.tasks[task_id].description
        self.files.update(task_id, description=f"[green]{desc}")
        self.overall.advance(self.overall_task)

    def file_error(self, index: int, exc: Exception) -> None:
        task_id = self._task_ids[index]
        desc = self.files.tasks[task_id].description
        self.files.update(task_id, description=f"[red]{desc}")
        self.overall.advance(self.overall_task)


def cmd_upload(args: argparse.Namespace) -> None:
    setup_logging()

    if len(args.targets) < 2:
        console.print("[red]Usage: mstransfer upload <paths...> <target>")
        sys.exit(1)

    *raw_paths, target = args.targets
    base_url = parse_target(target)

    try:
        file_paths = resolve_inputs(raw_paths, recursive=args.recursive)
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}")
        sys.exit(1)

    # Quick healthcheck before starting uploads.
    try:
        httpx.get(f"{base_url}/v1/health", timeout=5.0)
    except httpx.ConnectError:
        console.print(f"[red]Cannot connect to server at {base_url}. Is it running?")
        sys.exit(1)
    except httpx.TimeoutException:
        console.print(f"[red]Server at {base_url} did not respond in time.")
        sys.exit(1)

    console.print(
        f"Sending [bold]{len(file_paths)}[/] file(s) to "
        f"[cyan]{base_url}[/] (parallel={args.parallel})"
    )

    display = UploadProgressDisplay(len(file_paths))

    with Live(display.table, console=console, refresh_per_second=10):
        results = send_batch(
            file_paths, base_url,
            parallel=args.parallel,
            chunk_size=args.chunk_size,
            progress=display,
        )

    ok = sum(
        1 for r in results
        if r.response and r.response.state == TransferState.DONE
    )
    fail = len(results) - ok
    if fail:
        console.print(f"\n[green]{ok} succeeded[/], [red]{fail} failed[/]")
        for r in results:
            if r.response and r.response.state == TransferState.DONE:
                continue
            err = r.error or (str(r.response.state.value) if r.response else "unknown")
            console.print(f"  [red]- {r.filename}: {err}")
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
    sp.add_argument(
        "--chunk-size",
        type=int,
        default=1_048_576,
        help="Upload chunk size in bytes (default: 1048576)",
    )
    sp.set_defaults(func=cmd_upload)

    args = parser.parse_args()
    args.func(args)
