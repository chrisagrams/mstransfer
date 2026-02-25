"""Tests for CLI argument parsing and helpers."""

from __future__ import annotations

import argparse
import socket
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from mstransfer.cli import (
    UploadProgressDisplay,
    cmd_serve,
    cmd_upload,
    main,
    parse_target,
)
from mstransfer.client.sender import FileResult
from mstransfer.server.models import (
    TransferState,
    UploadResponse,
)


class TestParseTarget:
    def test_host_and_port(self):
        assert parse_target("192.168.1.1:8080") == "http://192.168.1.1:8080"

    def test_host_only(self):
        assert parse_target("myserver") == "http://myserver:1319"

    def test_localhost(self):
        assert parse_target("localhost:5000") == "http://localhost:5000"

    def test_default_port(self):
        assert parse_target("example.com") == "http://example.com:1319"

    def test_invalid_port_exits(self):
        with pytest.raises(SystemExit):
            parse_target("host:notaport")

    def test_http_url_passthrough(self):
        assert parse_target("http://proxy.example.com:8080") == "http://proxy.example.com:8080"

    def test_https_url_passthrough(self):
        assert parse_target("https://proxy.example.com") == "https://proxy.example.com"

    def test_url_trailing_slash_stripped(self):
        assert parse_target("https://proxy.example.com/") == "https://proxy.example.com"


class TestCliParsing:
    def test_serve_defaults(self):
        """Verify serve subcommand parses with defaults."""
        with patch.object(sys, "argv", ["mstransfer", "serve"]):
            parser = argparse.ArgumentParser(prog="mstransfer")
            sub = parser.add_subparsers(dest="command")
            lp = sub.add_parser("serve")
            lp.add_argument("--host", default="0.0.0.0")
            lp.add_argument("--port", type=int, default=1319)
            lp.add_argument("--output-dir", default="./received")
            lp.add_argument("--store-as", default="msz")
            args = parser.parse_args(["serve"])
            assert args.host == "0.0.0.0"
            assert args.port == 1319
            assert args.output_dir == "./received"
            assert args.store_as == "msz"

    def test_serve_custom(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(dest="command")
        lp = sub.add_parser("serve")
        lp.add_argument("--host", default="0.0.0.0")
        lp.add_argument("--port", type=int, default=1319)
        lp.add_argument("--output-dir", default="./received")
        lp.add_argument("--store-as", default="msz")
        args = parser.parse_args(
            ["serve", "--host", "127.0.0.1", "--port", "9999", "--store-as", "mzml"]
        )
        assert args.host == "127.0.0.1"
        assert args.port == 9999
        assert args.store_as == "mzml"

    def test_upload_parsing(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(dest="command")
        sp = sub.add_parser("upload")
        sp.add_argument("targets", nargs="+")
        sp.add_argument("--recursive", "-r", action="store_true")
        sp.add_argument("--parallel", "-p", type=int, default=4)
        sp.add_argument("--chunk-size", type=int, default=1_048_576)
        args = parser.parse_args(
            ["upload", "/data/file.mzML", "/data/dir", "myhost:1319", "-r", "-p", "8"]
        )
        assert args.targets == ["/data/file.mzML", "/data/dir", "myhost:1319"]
        assert args.recursive is True
        assert args.parallel == 8
        assert args.chunk_size == 1_048_576

    def test_upload_custom_chunk_size(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(dest="command")
        sp = sub.add_parser("upload")
        sp.add_argument("targets", nargs="+")
        sp.add_argument("--recursive", "-r", action="store_true")
        sp.add_argument("--parallel", "-p", type=int, default=4)
        sp.add_argument("--chunk-size", type=int, default=1_048_576)
        args = parser.parse_args(
            ["upload", "file.mzML", "host:1319", "--chunk-size", "8388608"]
        )
        assert args.chunk_size == 8_388_608


# ---------------------------------------------------------------------------
# cmd_serve tests
# ---------------------------------------------------------------------------


class TestCmdServe:
    def _make_args(self, **overrides):
        defaults = {
            "host": "0.0.0.0",
            "port": 1319,
            "output_dir": "./received",
            "store_as": "msz",
        }
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    @patch("mstransfer.cli.uvicorn.run")
    @patch("mstransfer.cli.create_app")
    def test_serve_starts_uvicorn(self, mock_create_app, mock_run):
        mock_app = MagicMock()
        mock_create_app.return_value = mock_app
        args = self._make_args()

        cmd_serve(args)

        mock_create_app.assert_called_once_with(
            output_dir="./received", store_as="msz",
        )
        mock_run.assert_called_once_with(
            mock_app, host="0.0.0.0", port=1319, log_level="warning",
        )

    @patch("mstransfer.cli.uvicorn.run")
    @patch("mstransfer.cli.create_app")
    def test_serve_custom_args(self, mock_create_app, mock_run):
        args = self._make_args(
            host="127.0.0.1", port=9999, output_dir="/tmp/out", store_as="mzml",
        )
        cmd_serve(args)
        mock_create_app.assert_called_once_with(
            output_dir="/tmp/out", store_as="mzml",
        )
        mock_run.assert_called_once_with(
            mock_create_app.return_value,
            host="127.0.0.1", port=9999, log_level="warning",
        )

    def test_serve_port_in_use_exits(self):
        """If the port is already bound, cmd_serve should exit with code 1."""
        # Bind a port so it's "in use"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]

            args = self._make_args(host="127.0.0.1", port=port)
            with pytest.raises(SystemExit, match="1"):
                cmd_serve(args)


# ---------------------------------------------------------------------------
# cmd_upload tests
# ---------------------------------------------------------------------------


class TestCmdUpload:
    def _make_args(self, targets, **overrides):
        defaults = {
            "targets": targets,
            "recursive": False,
            "parallel": 4,
            "chunk_size": 1_048_576,
        }
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def test_too_few_targets_exits(self):
        """upload requires at least <path> <target>."""
        args = self._make_args(["onlyone"])
        with pytest.raises(SystemExit, match="1"):
            cmd_upload(args)

    @patch(
        "mstransfer.cli.resolve_inputs",
        side_effect=FileNotFoundError("No valid files"),
    )
    def test_no_valid_files_exits(self, _mock_resolve):
        args = self._make_args(["nonexistent.txt", "host:1319"])
        with pytest.raises(SystemExit, match="1"):
            cmd_upload(args)

    @patch(
        "mstransfer.cli.resolve_inputs",
        return_value=[Path("/data/a.mzML")],
    )
    @patch(
        "mstransfer.cli.httpx.get",
        side_effect=httpx.ConnectError("refused"),
    )
    def test_healthcheck_connect_error_exits(self, _mock_get, _mock_resolve):
        args = self._make_args(["/data/a.mzML", "badhost:1319"])
        with pytest.raises(SystemExit, match="1"):
            cmd_upload(args)

    @patch(
        "mstransfer.cli.resolve_inputs",
        return_value=[Path("/data/a.mzML")],
    )
    @patch(
        "mstransfer.cli.httpx.get",
        side_effect=httpx.TimeoutException("timeout"),
    )
    def test_healthcheck_timeout_exits(self, _mock_get, _mock_resolve):
        args = self._make_args(["/data/a.mzML", "slowhost:1319"])
        with pytest.raises(SystemExit, match="1"):
            cmd_upload(args)

    @patch("mstransfer.cli.send_batch")
    @patch("mstransfer.cli.httpx.get")
    @patch("mstransfer.cli.resolve_inputs")
    def test_successful_upload(self, mock_resolve, mock_get, mock_send_batch):
        files = [Path("/data/a.mzML"), Path("/data/b.msz")]
        mock_resolve.return_value = files
        mock_get.return_value = MagicMock(status_code=200)
        mock_send_batch.return_value = [
            FileResult(
                filename="a.mzML",
                response=UploadResponse(
                    transfer_id="t1", filename="a.msz", stored_as="msz",
                    state=TransferState.DONE, bytes_received=100,
                ),
            ),
            FileResult(
                filename="b.msz",
                response=UploadResponse(
                    transfer_id="t2", filename="b.msz", stored_as="msz",
                    state=TransferState.DONE, bytes_received=200,
                ),
            ),
        ]

        args = self._make_args(["/data/a.mzML", "/data/b.msz", "myhost:1319"])
        # Should not raise
        cmd_upload(args)

        mock_send_batch.assert_called_once()
        call_kwargs = mock_send_batch.call_args
        assert call_kwargs[0][0] == files
        assert call_kwargs[0][1] == "http://myhost:1319"

    @patch("mstransfer.cli.send_batch")
    @patch("mstransfer.cli.httpx.get")
    @patch("mstransfer.cli.resolve_inputs")
    def test_partial_failure_prints_errors(
        self, mock_resolve, mock_get, mock_send_batch,
    ):
        mock_resolve.return_value = [Path("/data/a.mzML"), Path("/data/b.msz")]
        mock_get.return_value = MagicMock(status_code=200)
        mock_send_batch.return_value = [
            FileResult(
                filename="a.mzML",
                response=UploadResponse(
                    transfer_id="t1", filename="a.msz", stored_as="msz",
                    state=TransferState.DONE, bytes_received=100,
                ),
            ),
            FileResult(filename="b.msz", error="Connection reset"),
        ]

        args = self._make_args(["/data/a.mzML", "/data/b.msz", "host"])
        # Should not raise — just prints the error summary
        cmd_upload(args)

    @patch("mstransfer.cli.send_batch")
    @patch("mstransfer.cli.httpx.get")
    @patch("mstransfer.cli.resolve_inputs")
    def test_upload_passes_parallel_and_chunk_size(
        self, mock_resolve, mock_get, mock_send_batch,
    ):
        mock_resolve.return_value = [Path("/data/a.mzML")]
        mock_get.return_value = MagicMock(status_code=200)
        mock_send_batch.return_value = [
            FileResult(
                filename="a.mzML",
                response=UploadResponse(
                    transfer_id="t1", filename="a.msz", stored_as="msz",
                    state=TransferState.DONE, bytes_received=100,
                ),
            ),
        ]

        args = self._make_args(
            ["/data/a.mzML", "host"], parallel=8, chunk_size=4_194_304,
        )
        cmd_upload(args)

        _, kwargs = mock_send_batch.call_args
        assert kwargs["parallel"] == 8
        assert kwargs["chunk_size"] == 4_194_304

    @patch("mstransfer.cli.send_batch")
    @patch("mstransfer.cli.httpx.get")
    @patch("mstransfer.cli.resolve_inputs")
    def test_upload_error_response_state(self, mock_resolve, mock_get, mock_send_batch):
        """A file that has a response but with ERROR state is counted as failure."""
        mock_resolve.return_value = [Path("/data/a.mzML")]
        mock_get.return_value = MagicMock(status_code=200)
        mock_send_batch.return_value = [
            FileResult(
                filename="a.mzML",
                response=UploadResponse(
                    transfer_id="t1", filename="a.msz", stored_as="msz",
                    state=TransferState.ERROR, bytes_received=50,
                ),
            ),
        ]

        args = self._make_args(["/data/a.mzML", "host"])
        # Should not raise — reports failure
        cmd_upload(args)


# ---------------------------------------------------------------------------
# UploadProgressDisplay tests
# ---------------------------------------------------------------------------


class TestUploadProgressDisplay:
    def test_file_started_adds_task(self):
        display = UploadProgressDisplay(total_files=2)
        display.file_started(0, Path("/data/a.mzML"), total_bytes=1000)

        assert 0 in display._task_ids
        task = display.files.tasks[display._task_ids[0]]
        assert task.description == "a.mzML"
        assert task.total == 1000

    def test_file_progress_advances(self):
        display = UploadProgressDisplay(total_files=1)
        display.file_started(0, Path("/data/a.mzML"), total_bytes=1000)
        display.file_progress(0, 500)

        task = display.files.tasks[display._task_ids[0]]
        assert task.completed == 500

    def test_file_done_marks_green(self):
        display = UploadProgressDisplay(total_files=1)
        display.file_started(0, Path("/data/a.mzML"), total_bytes=1000)
        resp = UploadResponse(
            transfer_id="t1", filename="a.msz", stored_as="msz",
            state=TransferState.DONE, bytes_received=1000,
        )
        display.file_done(0, resp)

        task = display.files.tasks[display._task_ids[0]]
        assert task.description == "[green]a.mzML"

    def test_file_error_marks_red(self):
        display = UploadProgressDisplay(total_files=1)
        display.file_started(0, Path("/data/a.mzML"), total_bytes=1000)
        display.file_error(0, RuntimeError("boom"))

        task = display.files.tasks[display._task_ids[0]]
        assert task.description == "[red]a.mzML"

    def test_overall_advances_on_done(self):
        display = UploadProgressDisplay(total_files=2)
        display.file_started(0, Path("a.mzML"), total_bytes=100)
        display.file_started(1, Path("b.msz"), total_bytes=200)
        resp = UploadResponse(
            transfer_id="t1", filename="a.msz", stored_as="msz",
            state=TransferState.DONE, bytes_received=100,
        )
        display.file_done(0, resp)

        overall_task = display.overall.tasks[display.overall_task]
        assert overall_task.completed == 1

    def test_overall_advances_on_error(self):
        display = UploadProgressDisplay(total_files=2)
        display.file_started(0, Path("a.mzML"), total_bytes=100)
        display.file_error(0, RuntimeError("fail"))

        overall_task = display.overall.tasks[display.overall_task]
        assert overall_task.completed == 1

    def test_none_total_bytes(self):
        """mzML files may have total_bytes=None (unknown compressed size)."""
        display = UploadProgressDisplay(total_files=1)
        display.file_started(0, Path("a.mzML"), total_bytes=None)

        task = display.files.tasks[display._task_ids[0]]
        assert task.total is None


# ---------------------------------------------------------------------------
# main() entry point tests
# ---------------------------------------------------------------------------


class TestMain:
    @patch("mstransfer.cli.cmd_serve")
    def test_main_dispatches_serve(self, mock_cmd_serve):
        with patch("sys.argv", ["mstransfer", "serve"]):
            main()
        mock_cmd_serve.assert_called_once()

    @patch("mstransfer.cli.cmd_upload")
    def test_main_dispatches_upload(self, mock_cmd_upload):
        with patch("sys.argv", ["mstransfer", "upload", "file.mzML", "host"]):
            main()
        mock_cmd_upload.assert_called_once()

    def test_main_no_command_exits(self):
        with patch("sys.argv", ["mstransfer"]), pytest.raises(SystemExit):
            main()
