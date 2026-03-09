import json
import os
import tempfile
from subprocess import CalledProcessError
from unittest.mock import patch

import pytest

from dataflow_transfer.utils.filesystem import (
    check_exit_status,
    find_runs,
    get_run_dir,
    locate_metadata,
    parse_metadata_files,
    rsync_is_running,
    submit_background_process,
)


class TestGetRunDir:
    def test_absolute_path_existing_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            assert get_run_dir(tmpdir) == tmpdir

    def test_relative_path_existing_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                rel_path = "."
                assert os.path.isabs(get_run_dir(rel_path))
            finally:
                os.chdir(original_cwd)

    def test_invalid_path_raises_error(self):
        with pytest.raises(ValueError):
            get_run_dir("/nonexistent/path")


class TestFindRuns:
    def test_find_runs_basic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            os.mkdir(os.path.join(tmpdir, "run1"))
            os.mkdir(os.path.join(tmpdir, "run2"))
            runs = find_runs(tmpdir)
            assert os.path.join(tmpdir, "run1") in runs
            assert os.path.join(tmpdir, "run2") in runs

    def test_find_runs_with_ignore(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            os.mkdir(os.path.join(tmpdir, "run1"))
            os.mkdir(os.path.join(tmpdir, "run2"))
            os.mkdir(os.path.join(tmpdir, "ignore_me"))
            runs = find_runs(tmpdir, ignore_folders=["ignore_me"])
            assert os.path.join(tmpdir, "run1") in runs
            assert os.path.join(tmpdir, "run2") in runs
            assert os.path.join(tmpdir, "ignore_me") not in runs

    def test_find_runs_ignores_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            os.mkdir(os.path.join(tmpdir, "run1"))
            open(os.path.join(tmpdir, "file.txt"), "w").close()
            runs = find_runs(tmpdir)
            assert os.path.join(tmpdir, "run1") in runs
            assert os.path.join(tmpdir, "file.txt") not in runs


class TestRsyncIsRunning:
    @patch("subprocess.check_output")
    def test_rsync_running(self, mock_check_output):
        mock_check_output.return_value = b"12345"
        assert rsync_is_running("/some/path", "/dst/path") is True

    @patch("subprocess.check_output")
    def test_rsync_not_running(self, mock_check_output):
        mock_check_output.side_effect = CalledProcessError(1, "pgrep")
        assert rsync_is_running("/some/path", "/dst/path") is False


class TestSubmitBackgroundProcess:
    @patch("subprocess.Popen")
    def test_submit_background_process(self, mock_popen):
        submit_background_process("echo test")
        mock_popen.assert_called_once()


class TestParseMetadataFiles:
    def test_parse_json_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            json_file = os.path.join(tmpdir, "metadata.json")
            with open(json_file, "w") as f:
                json.dump({"key": "value"}, f)
            metadata = parse_metadata_files([json_file])
            assert "metadata.json" in metadata
            assert metadata["metadata.json"]["key"] == "value"

    def test_parse_xml_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            xml_file = os.path.join(tmpdir, "metadata.xml")
            with open(xml_file, "w") as f:
                f.write("<root><key>value</key></root>")
            metadata = parse_metadata_files([xml_file])
            assert "metadata.xml" in metadata
            assert metadata["metadata.xml"]["root"]["key"] == "value"

    def test_unsupported_file_type(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            txt_file = os.path.join(tmpdir, "metadata.txt")
            with open(txt_file, "w") as f:
                f.write("content")
            metadata = parse_metadata_files([txt_file])
            assert "metadata.txt" not in metadata

    def test_parse_nonexistent_file(self):
        metadata = parse_metadata_files(["/nonexistent/file.json"])
        assert metadata == {}


class TestCheckExitStatus:
    def test_exit_status_zero(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            exit_file = os.path.join(tmpdir, "exit_status")
            with open(exit_file, "w") as f:
                f.write("0")
            assert check_exit_status(exit_file) is True

    def test_exit_status_nonzero(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            exit_file = os.path.join(tmpdir, "exit_status")
            with open(exit_file, "w") as f:
                f.write("1")
            assert check_exit_status(exit_file) is False

    def test_exit_status_file_not_found(self):
        assert check_exit_status("/nonexistent/file") is False


class TestLocateMetadata:
    def test_locate_metadata_found(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "metadata.json")
            open(metadata_file, "w").close()
            located = locate_metadata(["metadata.json"], tmpdir)
            assert len(located) == 1
            assert metadata_file in located

    def test_locate_metadata_not_found(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            located = locate_metadata(["nonexistent.json"], tmpdir)
            assert len(located) == 0

    def test_locate_metadata_multiple_patterns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "meta1.json"), "w").close()
            open(os.path.join(tmpdir, "meta2.json"), "w").close()
            located = locate_metadata(["meta1.json", "meta2.json"], tmpdir)
            assert len(located) == 2
            assert os.path.join(tmpdir, "meta1.json") in located
            assert os.path.join(tmpdir, "meta2.json") in located
