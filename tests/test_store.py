"""Tests for how downloaded files are stored on disk."""

from __future__ import annotations

import io
import json
import tarfile

import pytest

from mstransfer.client.store import (
    Conversion,
    MSFormat,
    MszxExpansion,
    StorePlan,
)


class TestMSFormatFromUrl:
    def test_msz_url(self):
        assert MSFormat.from_url("http://host/data/file.msz") == "msz"

    def test_mzml_url(self):
        assert MSFormat.from_url("http://host/data/file.mzML") == "mzml"

    def test_mzml_lowercase(self):
        assert MSFormat.from_url("http://host/data/file.mzml") == "mzml"

    def test_unknown_extension(self):
        assert MSFormat.from_url("http://host/data/file.bin") is None

    def test_no_extension(self):
        assert MSFormat.from_url("http://host/data/file") is None

    def test_host_is_not_a_filename(self):
        assert MSFormat.from_url("http://data.msz") is None

    def test_percent_encoded_path(self):
        assert MSFormat.from_url("http://host/my%20file%2EmzML") == "mzml"

    def test_extension_in_query_ignored(self):
        assert MSFormat.from_url("http://host/download?file=x.msz") is None

    def test_query_params_stripped(self):
        assert MSFormat.from_url("http://h/f.msz?token=abc") == "msz"

    def test_fragment_stripped(self):
        assert MSFormat.from_url("http://h/f.mzML#section") == "mzml"

    def test_mszx_url(self):
        assert MSFormat.from_url("http://host/data/file.mszx") == "mszx"

    def test_mszx_uppercase(self):
        assert MSFormat.from_url("http://host/data/file.MSZX") == "mszx"


class TestMSFormat:
    def test_compares_equal_to_string(self):
        assert MSFormat.MZML == "mzml"
        assert MSFormat("mzml") is MSFormat.MZML

    def test_str_is_value(self):
        assert str(MSFormat.MSZX) == "mszx"

    @pytest.mark.parametrize(
        ("fmt", "ext"),
        [(MSFormat.MSZ, ".msz"), (MSFormat.MZML, ".mzML"), (MSFormat.MSZX, ".mszx")],
    )
    def test_ext(self, fmt, ext):
        assert fmt.ext == ext


class TestConversion:
    @pytest.mark.parametrize(
        ("source", "target"),
        [
            (MSFormat.MSZ, MSFormat.MZML),
            (MSFormat.MZML, MSFormat.MSZ),
            (MSFormat.MSZX, MSFormat.MZML),
            (MSFormat.MSZX, MSFormat.MSZ),
        ],
    )
    def test_supported(self, source, target):
        assert Conversion(source, target).supported

    @pytest.mark.parametrize("source", [MSFormat.MSZ, MSFormat.MZML])
    def test_to_mszx_unsupported(self, source):
        assert not Conversion(source, MSFormat.MSZX).supported


class TestFinalDest:
    def test_no_store_as(self, tmp_path):
        dest = tmp_path / "file.msz"
        assert StorePlan(dest, MSFormat.MSZ, None).final_dest == dest

    def test_same_format(self, tmp_path):
        dest = tmp_path / "file.msz"
        assert StorePlan(dest, MSFormat.MSZ, MSFormat.MSZ).final_dest == dest

    def test_msz_to_mzml(self, tmp_path):
        dest = tmp_path / "file.msz"
        assert (
            StorePlan(dest, MSFormat.MSZ, MSFormat.MZML).final_dest
            == tmp_path / "file.mzML"
        )

    def test_mzml_to_msz(self, tmp_path):
        dest = tmp_path / "file.mzML"
        assert (
            StorePlan(dest, MSFormat.MZML, MSFormat.MSZ).final_dest
            == tmp_path / "file.msz"
        )

    def test_unknown_source_no_change(self, tmp_path):
        dest = tmp_path / "file.bin"
        assert StorePlan(dest, None, MSFormat.MSZ).final_dest == dest

    def test_mszx_same_format(self, tmp_path):
        dest = tmp_path / "file.mszx"
        assert StorePlan(dest, MSFormat.MSZX, MSFormat.MSZX).final_dest == dest

    def test_mszx_to_msz(self, tmp_path):
        dest = tmp_path / "file.mszx"
        assert (
            StorePlan(dest, MSFormat.MSZX, MSFormat.MSZ).final_dest
            == tmp_path / "file.msz"
        )

    def test_mszx_to_mzml(self, tmp_path):
        dest = tmp_path / "file.mszx"
        assert (
            StorePlan(dest, MSFormat.MSZX, MSFormat.MZML).final_dest
            == tmp_path / "file.mzML"
        )

    def test_msz_to_mszx(self, tmp_path):
        dest = tmp_path / "file.msz"
        assert (
            StorePlan(dest, MSFormat.MSZ, MSFormat.MSZX).final_dest
            == tmp_path / "file.mszx"
        )


class TestStorePlan:
    def test_for_download_detects_source(self, tmp_path):
        plan = StorePlan.for_download("http://h/f.msz", tmp_path / "f.msz", "mzml")
        assert plan.source == "msz"
        assert plan.target == "mzml"
        assert plan.conversion == Conversion(MSFormat.MSZ, MSFormat.MZML)

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
        plan = StorePlan(tmp_path / "f.msz", MSFormat.MSZ, MSFormat.MZML)
        assert plan.part_path == tmp_path / "f.msz.part"

    @pytest.mark.parametrize(
        ("source", "target", "expected"),
        [
            (MSFormat.MSZX, MSFormat.MSZ, True),
            (MSFormat.MSZX, MSFormat.MZML, True),
            (MSFormat.MSZX, MSFormat.MSZX, False),
            (MSFormat.MSZX, None, False),
            (MSFormat.MSZ, MSFormat.MZML, False),
        ],
    )
    def test_may_expand(self, tmp_path, source, target, expected):
        assert StorePlan(tmp_path / "f", source, target).may_expand is expected

    def test_archive_dir(self, tmp_path):
        plan = StorePlan(tmp_path / "run.mszx", MSFormat.MSZX, MSFormat.MSZ)
        assert plan.archive_dir == tmp_path / "run"

    def test_existing_none(self, tmp_path):
        assert (
            StorePlan(tmp_path / "run.mszx", MSFormat.MSZX, MSFormat.MSZ).existing()
            is None
        )

    def test_existing_final_dest(self, tmp_path):
        (tmp_path / "run.msz").write_bytes(b"x")
        plan = StorePlan(tmp_path / "run.mszx", MSFormat.MSZX, MSFormat.MSZ)
        assert plan.existing() == tmp_path / "run.msz"

    def test_existing_archive_dir(self, tmp_path):
        (tmp_path / "run").mkdir()
        plan = StorePlan(tmp_path / "run.mszx", MSFormat.MSZX, MSFormat.MSZ)
        assert plan.existing() == tmp_path / "run"

    def test_existing_ignores_dir_without_expansion(self, tmp_path):
        (tmp_path / "run").mkdir()
        assert (
            StorePlan(tmp_path / "run.msz", MSFormat.MSZ, MSFormat.MZML).existing()
            is None
        )


class TestMszxExpansion:
    def test_targets_use_entry_stem_and_target_ext(self, tmp_path):
        expansion = MszxExpansion(
            tmp_path / "a.mszx", ("a.msz", "B.MSZ"), MSFormat.MZML
        )
        assert expansion.targets == [("a.msz", "a.mzML"), ("B.MSZ", "B.mzML")]

    def test_targets_strip_directories(self, tmp_path):
        entries = ("../x.msz", "sub/y.msz", "..\\w.msz")
        expansion = MszxExpansion(tmp_path / "a.mszx", entries, MSFormat.MSZ)
        assert [name for _, name in expansion.targets] == ["x.msz", "y.msz", "w.msz"]

    def test_targets_reject_case_insensitive_collisions(self, tmp_path):
        expansion = MszxExpansion(
            tmp_path / "a.mszx", ("a.msz", "sub/A.msz"), MSFormat.MSZ
        )
        with pytest.raises(ValueError, match="both map to"):
            _ = expansion.targets

    @pytest.mark.parametrize("entry", [".msz", "..", "sub/.hidden.msz"])
    def test_targets_reject_unsafe_names(self, tmp_path, entry):
        expansion = MszxExpansion(tmp_path / "a.mszx", ("ok.msz", entry), MSFormat.MSZ)
        with pytest.raises(ValueError, match="Unsafe"):
            _ = expansion.targets

    @pytest.mark.asyncio
    async def test_write_validates_before_touching_disk(self, tmp_path):
        expansion = MszxExpansion(tmp_path / "a.mszx", (".msz", "b.msz"), MSFormat.MSZ)
        with pytest.raises(ValueError, match="Unsafe"):
            await expansion.write(tmp_path / "a")
        assert list(tmp_path.iterdir()) == []


class TestMszxToMszConversion:
    @pytest.mark.asyncio
    async def test_mszx_to_msz_rejects_multi_member_batch(self, test_msz, tmp_path):
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
            await Conversion(MSFormat.MSZX, MSFormat.MSZ).run(src, tmp_path / "out.msz")

    @pytest.mark.asyncio
    async def test_mszx_to_msz_rejects_non_file_entry(self, tmp_path):
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

        with pytest.raises(ValueError, match="not a regular file"):
            await Conversion(MSFormat.MSZX, MSFormat.MSZ).run(src, tmp_path / "out.msz")
