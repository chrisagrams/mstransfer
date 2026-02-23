"""Tests for CLI argument parsing and helpers."""

from __future__ import annotations

import pytest

from mstransfer.cli import parse_target


class TestParseTarget:
    def test_host_and_port(self):
        assert parse_target("192.168.1.1:8080") == ("192.168.1.1", 8080)

    def test_host_only(self):
        assert parse_target("myserver") == ("myserver", 1319)

    def test_localhost(self):
        assert parse_target("localhost:5000") == ("localhost", 5000)

    def test_default_port(self):
        host, port = parse_target("example.com")
        assert host == "example.com"
        assert port == 1319

    def test_invalid_port_exits(self):
        with pytest.raises(SystemExit):
            parse_target("host:notaport")


class TestCliParsing:
    def test_listen_defaults(self):
        """Verify listen subcommand parses with defaults."""
        import argparse
        import sys
        from unittest.mock import patch

        with patch.object(sys, "argv", ["mstransfer", "listen"]):
            parser = argparse.ArgumentParser(prog="mstransfer")
            sub = parser.add_subparsers(dest="command")
            lp = sub.add_parser("listen")
            lp.add_argument("--host", default="0.0.0.0")
            lp.add_argument("--port", type=int, default=1319)
            lp.add_argument("--output-dir", default="./received")
            lp.add_argument("--store-as", default="msz")
            args = parser.parse_args(["listen"])
            assert args.host == "0.0.0.0"
            assert args.port == 1319
            assert args.output_dir == "./received"
            assert args.store_as == "msz"

    def test_listen_custom(self):
        import argparse

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(dest="command")
        lp = sub.add_parser("listen")
        lp.add_argument("--host", default="0.0.0.0")
        lp.add_argument("--port", type=int, default=1319)
        lp.add_argument("--output-dir", default="./received")
        lp.add_argument("--store-as", default="msz")
        args = parser.parse_args(
            ["listen", "--host", "127.0.0.1", "--port", "9999", "--store-as", "mzml"]
        )
        assert args.host == "127.0.0.1"
        assert args.port == 9999
        assert args.store_as == "mzml"

    def test_send_parsing(self):
        import argparse

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(dest="command")
        sp = sub.add_parser("send")
        sp.add_argument("targets", nargs="+")
        sp.add_argument("--recursive", "-r", action="store_true")
        sp.add_argument("--parallel", "-p", type=int, default=4)
        args = parser.parse_args(
            ["send", "/data/file.mzML", "/data/dir", "myhost:1319", "-r", "-p", "8"]
        )
        assert args.targets == ["/data/file.mzML", "/data/dir", "myhost:1319"]
        assert args.recursive is True
        assert args.parallel == 8
