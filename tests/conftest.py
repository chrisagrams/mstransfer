from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path

import httpx
import pytest

from mstransfer.server.app import create_app
from mstransfer.server.auth import APIKeyAuthProvider

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
def test_mszx(tmp_path) -> Path:
    """Build a minimal .mszx archive from the real test.msz file."""
    msz_src = DATA_DIR / "test.msz"
    mszx_path = tmp_path / "test.mszx"

    manifest = json.dumps(
        {
            "version": "1.0",
            "spectra_file": "spectra.msz",
            "num_spectra": 0,
            "annotations": [],
            "join_key": "scan_number",
        }
    ).encode()

    with tarfile.open(mszx_path, "w") as tar:
        info = tarfile.TarInfo(name="manifest.json")
        info.size = len(manifest)
        tar.addfile(info, io.BytesIO(manifest))
        tar.add(str(msz_src), arcname="spectra.msz")

    return mszx_path


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
def authed_app(tmp_output):
    """FastAPI app configured with API key auth."""
    return create_app(
        output_dir=str(tmp_output),
        store_as="msz",
        auth=APIKeyAuthProvider("test-secret"),
    )


@pytest.fixture()
def authed_client(authed_app):
    """httpx AsyncClient wired to the authed app via ASGI transport."""
    transport = httpx.ASGITransport(app=authed_app)
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
