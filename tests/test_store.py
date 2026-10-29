"""Tests for how downloaded files are stored on disk."""

from __future__ import annotations

import io
import json
import tarfile

import pytest

from mstransfer.client.store import StorePlan, detect_source_format, mszx_to_msz

# ---------------------------------------------------------------------------
# Format detection and destination resolution
# ---------------------------------------------------------------------------


class TestDetectSourceFormat:
    def test_msz_url(self):
        assert detect_source_format("http://host/data/file.msz") == "msz"

    def test_mzml_url(self):
        assert detect_source_format("http://host/data/file.mzML") == "mzml"

    def test_mzml_lowercase(self):
        assert detect_source_format("http://host/data/file.mzml") == "mzml"

    def test_unknown_extension(self):
        assert detect_source_format("http://host/data/file.bin") is None

    def test_query_params_stripped(self):
        assert detect_source_format("http://h/f.msz?token=abc") == "msz"

    def test_fragment_stripped(self):
        assert detect_source_format("http://h/f.mzML#section") == "mzml"

    def test_mszx_url(self):
        assert detect_source_format("http://host/data/file.mszx") == "mszx"

    def test_mszx_uppercase(self):
        assert detect_source_format("http://host/data/file.MSZX") == "mszx"


class TestFinalDest:
    def test_no_store_as(self, tmp_path):
        dest = tmp_path / "file.msz"
        assert StorePlan(dest, "msz", None).final_dest == dest

    def test_same_format(self, tmp_path):
        dest = tmp_path / "file.msz"
        assert StorePlan(dest, "msz", "msz").final_dest == dest

    def test_msz_to_mzml(self, tmp_path):
        dest = tmp_path / "file.msz"
        assert StorePlan(dest, "msz", "mzml").final_dest == tmp_path / "file.mzML"

    def test_mzml_to_msz(self, tmp_path):
        dest = tmp_path / "file.mzML"
        assert StorePlan(dest, "mzml", "msz").final_dest == tmp_path / "file.msz"

    def test_unknown_source_no_change(self, tmp_path):
        dest = tmp_path / "file.bin"
        assert StorePlan(dest, None, "msz").final_dest == dest

    def test_mszx_same_format(self, tmp_path):
        dest = tmp_path / "file.mszx"
        assert StorePlan(dest, "mszx", "mszx").final_dest == dest

    def test_mszx_to_msz(self, tmp_path):
        dest = tmp_path / "file.mszx"
        assert StorePlan(dest, "mszx", "msz").final_dest == tmp_path / "file.msz"

    def test_mszx_to_mzml(self, tmp_path):
        dest = tmp_path / "file.mszx"
        assert StorePlan(dest, "mszx", "mzml").final_dest == tmp_path / "file.mzML"

    def test_msz_to_mszx(self, tmp_path):
        dest = tmp_path / "file.msz"
        assert StorePlan(dest, "msz", "mszx").final_dest == tmp_path / "file.mszx"


class TestStorePlan:
    def test_for_download_detects_source(self, tmp_path):
        plan = StorePlan.for_download("http://h/f.msz", tmp_path / "f.msz", "mzml")
        assert plan.source == "msz"
        assert plan.target == "mzml"
        assert plan.conversion == ("msz", "mzml")

    @pytest.mark.parametrize("source", ["msz", "mzML"])
    def test_for_download_rejects_conversion_to_mszx(self, tmp_path, source):
        with pytest.raises(NotImplementedError, match="to mszx"):
            StorePlan.for_download(
                f"http://h/f.{source}", tmp_path / f"f.{source}", "mszx"
            )

    def test_no_conversion_for_unknown_source(self, tmp_path):
        plan = StorePlan.for_download("http://h/f.bin", tmp_path / "f.bin", "msz")
        assert plan.conversion is None

    def test_part_path_uses_requested_dest(self, tmp_path):
        plan = StorePlan(tmp_path / "f.msz", "msz", "mzml")
        assert plan.part_path == tmp_path / "f.msz.part"

    @pytest.mark.parametrize(
        ("source", "target", "expected"),
        [
            ("mszx", "msz", True),
            ("mszx", "mzml", True),
            ("mszx", "mszx", False),
            ("mszx", None, False),
            ("msz", "mzml", False),
        ],
    )
    def test_may_expand(self, tmp_path, source, target, expected):
        assert StorePlan(tmp_path / "f", source, target).may_expand is expected

    def test_archive_dir(self, tmp_path):
        plan = StorePlan(tmp_path / "run.mszx", "mszx", "msz")
        assert plan.archive_dir == tmp_path / "run"

    def test_existing_none(self, tmp_path):
        assert StorePlan(tmp_path / "run.mszx", "mszx", "msz").existing() is None

    def test_existing_final_dest(self, tmp_path):
        (tmp_path / "run.msz").write_bytes(b"x")
        plan = StorePlan(tmp_path / "run.mszx", "mszx", "msz")
        assert plan.existing() == tmp_path / "run.msz"

    def test_existing_archive_dir(self, tmp_path):
        (tmp_path / "run").mkdir()
        plan = StorePlan(tmp_path / "run.mszx", "mszx", "msz")
        assert plan.existing() == tmp_path / "run"

    def test_existing_ignores_dir_without_expansion(self, tmp_path):
        (tmp_path / "run").mkdir()
        assert StorePlan(tmp_path / "run.msz", "msz", "mzml").existing() is None


class TestMszxToMsz:
    def test_mszx_to_msz_rejects_multi_member_batch(self, test_msz, tmp_path):
        """A v2 batch .mszx with several members can't become one .msz."""
        src = tmp_path / "batch.mszx"
        manifest = json.dumps(
            {
                "version": "2.0",
                "container": "batch",
                "spectra_files": [{"entry": "a.msz"}, {"entry": "b.msz"}],
            }
        ).encode()
        with tarfile.open(src, "w") as tar:
            info = tarfile.TarInfo(name="manifest.json")
            info.size = len(manifest)
            tar.addfile(info, io.BytesIO(manifest))
            tar.add(str(test_msz), arcname="a.msz")
            tar.add(str(test_msz), arcname="b.msz")

        with pytest.raises(NotImplementedError, match="2-member"):
            mszx_to_msz(src, tmp_path / "out.msz")

    def test_mszx_to_msz_rejects_non_file_entry(self, tmp_path):
        """A manifest entry that isn't a regular tar member raises ValueError."""
        src = tmp_path / "bad.mszx"
        manifest = json.dumps(
            {
                "version": "2.0",
                "container": "batch",
                "spectra_files": [{"entry": "spectra.msz"}],
            }
        ).encode()
        with tarfile.open(src, "w") as tar:
            info = tarfile.TarInfo(name="manifest.json")
            info.size = len(manifest)
            tar.addfile(info, io.BytesIO(manifest))
            entry = tarfile.TarInfo(name="spectra.msz")
            entry.type = tarfile.DIRTYPE
            tar.addfile(entry)

        with pytest.raises(ValueError, match="Missing MSZ entry"):
            mszx_to_msz(src, tmp_path / "out.msz")
