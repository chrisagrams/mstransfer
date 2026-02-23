from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from mstransfer.server.app import create_app

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture()
def test_mzml() -> Path:
    """Path to the real test.mzML file."""
    return DATA_DIR / "test.mzML"


@pytest.fixture()
def test_msz() -> Path:
    """Path to the real test.msz file."""
    return DATA_DIR / "test.msz"


@pytest.fixture()
def tmp_output(tmp_path):
    """Temporary output directory for the server."""
    d = tmp_path / "output"
    d.mkdir()
    return d


@pytest.fixture()
def msz_app(tmp_output):
    """FastAPI app configured to store as MSZ."""
    return create_app(output_dir=str(tmp_output), store_as="msz")


@pytest.fixture()
def mzml_app(tmp_output):
    """FastAPI app configured to store as mzML."""
    return create_app(output_dir=str(tmp_output), store_as="mzml")


@pytest.fixture()
def msz_client(msz_app):
    """httpx AsyncClient wired to the msz app via ASGI transport."""
    transport = httpx.ASGITransport(app=msz_app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture()
def mzml_client(mzml_app):
    """httpx AsyncClient wired to the mzml app via ASGI transport."""
    transport = httpx.ASGITransport(app=mzml_app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture()
def sample_files(tmp_path):
    """Create a directory tree with sample .mzML and .msz files for testing."""
    # Top-level files
    (tmp_path / "a.mzML").write_bytes(b"fake mzml content")
    (tmp_path / "b.msz").write_bytes(b"fake msz content")
    (tmp_path / "c.txt").write_bytes(b"not a mass spec file")

    # Nested directory
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "d.mzML").write_bytes(b"nested mzml")
    (sub / "e.msz").write_bytes(b"nested msz")

    return tmp_path
