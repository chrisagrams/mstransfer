from __future__ import annotations

import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

import httpx
from rich.live import Live
from rich.table import Table

from mstransfer.log import (
    console,
    make_file_progress,
    make_overall_progress,
)

logger = logging.getLogger(__name__)

VALID_EXTENSIONS = {".mzml", ".msz"}


def resolve_inputs(paths: list[str], recursive: bool = False) -> list[Path]:
    """Resolve files and directories into a sorted list of valid MS files."""
    result: list[Path] = []
    for p in paths:
        path = Path(p)
        if path.is_file():
            if path.suffix.lower() in VALID_EXTENSIONS:
                result.append(path)
            else:
                logger.warning("Skipping unsupported file: %s", path)
        elif path.is_dir():
            for ext in ("*.mzML", "*.msz", "*.mzml", "*.MSZ"):
                if recursive:
                    result.extend(path.rglob(ext))
                else:
                    result.extend(path.glob(ext))
            result = list(dict.fromkeys(result))
        else:
            logger.warning("Path does not exist: %s", path)
    if not result:
        raise FileNotFoundError("No valid .mzML or .msz files found in the given paths")
    return sorted(set(result))


def _counting_generator(iterator, callback: Callable[[int], None] | None = None):
    """Wrap an iterator, calling callback with byte count per chunk."""
    for chunk in iterator:
        if callback:
            callback(len(chunk))
        yield chunk


def _file_chunk_generator(
    file_path: Path,
    chunk_size: int = 1_048_576,
    callback: Callable[[int], None] | None = None,
):
    """Read a file in chunks, calling callback with each chunk's size."""
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            if callback:
                callback(len(chunk))
            yield chunk


def send_file(
    file_path: Path,
    host: str,
    port: int,
    progress_callback: Callable[[int], None] | None = None,
    timeout: float = 3600.0,
) -> dict:
    """Send a single file to the mstransfer listener.

    Returns the final transfer status dict from the server.
    """
    from mscompress import MZMLFile
    from mscompress.utils import detect_filetype

    transfer_id = str(uuid.uuid4())
    filetype = detect_filetype(str(file_path))
    base_url = f"http://{host}:{port}"

    headers = {
        "X-Transfer-ID": transfer_id,
        "X-Original-Filename": file_path.name,
        "X-Source-Format": filetype or "unknown",
        "Content-Type": "application/octet-stream",
    }

    if filetype == "mzML":
        mzml = MZMLFile(str(file_path).encode())
        stream = _counting_generator(
            mzml.compress_stream(chunk_size=1_048_576),
            progress_callback,
        )
    elif filetype in ("msz", "mszx"):
        stream = _file_chunk_generator(file_path, callback=progress_callback)
    else:
        raise ValueError(f"Unsupported file type: {filetype} for {file_path}")

    with httpx.Client(timeout=httpx.Timeout(timeout, connect=10.0)) as client:
        resp = client.post(
            f"{base_url}/v1/upload",
            headers=headers,
            content=stream,
        )
        resp.raise_for_status()
        upload_result = resp.json()

    # Poll for server-side processing completion
    state = upload_result.get("state", "")
    if state not in ("done", "error"):
        state = _poll_status(base_url, transfer_id, timeout=timeout)
        upload_result["state"] = state

    return upload_result


def _poll_status(
    base_url: str,
    transfer_id: str,
    timeout: float = 300.0,
    interval: float = 0.5,
) -> str:
    """Poll transfer status until terminal state or timeout."""
    deadline = time.monotonic() + timeout
    with httpx.Client(timeout=10.0) as client:
        while time.monotonic() < deadline:
            resp = client.get(f"{base_url}/v1/transfer/{transfer_id}/status")
            if resp.status_code == 200:
                data = resp.json()
                state = data.get("state", "")
                if state in ("done", "error"):
                    return state
            time.sleep(interval)
    return "timeout"


def send_batch(
    file_paths: list[Path],
    host: str,
    port: int,
    parallel: int = 4,
) -> list[dict]:
    """Send multiple files with configurable parallelism."""
    workers = min(parallel, len(file_paths))
    results: list[dict] = [None] * len(file_paths)

    overall_progress = make_overall_progress()
    file_progress = make_file_progress()

    overall_task = overall_progress.add_task("Transferring", total=len(file_paths))

    table = Table.grid()
    table.add_row(overall_progress)
    table.add_row(file_progress)

    with Live(table, console=console, refresh_per_second=10):  # noqa: SIM117
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_idx = {}
            for idx, fpath in enumerate(file_paths):
                is_msz = fpath.suffix.lower() == ".msz"
                task_id = file_progress.add_task(
                    fpath.name,
                    total=(fpath.stat().st_size if is_msz else None),
                )

                def make_callback(tid):
                    def cb(delta: int):
                        file_progress.advance(tid, delta)

                    return cb

                future = pool.submit(
                    send_file,
                    fpath,
                    host,
                    port,
                    progress_callback=make_callback(task_id),
                )
                future_to_idx[future] = (idx, task_id)

            for future in as_completed(future_to_idx):
                idx, task_id = future_to_idx[future]
                fname = file_paths[idx].name
                try:
                    result = future.result()
                    results[idx] = result
                    file_progress.update(
                        task_id,
                        description=f"[green]{fname}",
                    )
                except Exception as exc:
                    results[idx] = {
                        "error": str(exc),
                        "filename": fname,
                    }
                    file_progress.update(
                        task_id,
                        description=f"[red]{fname}",
                    )
                    logger.error(
                        "Failed to send %s: %s",
                        file_paths[idx],
                        exc,
                    )
                finally:
                    overall_progress.advance(overall_task)

    return results
