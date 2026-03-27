import os
import tempfile
from subprocess import CalledProcessError
from unittest.mock import patch

import pytest

from dataflow_transfer.utils.filesystem import (
    check_exit_status,
    find_runs,
    get_run_dir,
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
