import os

import pytest

from dataflow_transfer.run_classes import generic_runs, illumina_runs

# TODO: add tests for ONT and ELEMENT runs when those are implemented


@pytest.fixture
def novaseqxplus_testobj(tmp_path):
    config = {
        "log": {"file": "test.log"},
        "transfer_details": {"user": "testuser", "host": "testhost"},
        "statusdb": {
            "username": "dbuser",
            "password": "dbpass",
            "url:": "dburl",
            "database": "dbname",
        },
        "sequencers": {
            "NovaSeqXPlus": {
                "miarka_destination": "/data/NovaSeqXPlus",
                "metadata_for_statusdb": ["RunInfo.xml", "RunParameters.xml"],
                "ignore_folders": ["nosync"],
                "rsync_options": ["--chmod=Dg+s,g+rw"],
            }
        },
    }
    run_id = "20251010_LH00202_0284_B22CVHTLT1"
    run_dir = tmp_path / run_id
    run_dir.mkdir()
    return illumina_runs.NovaSeqXPlusRun(str(run_dir), config)


@pytest.fixture
def nextseq_testobj(tmp_path):
    config = {
        "log": {"file": "test.log"},
        "transfer_details": {"user": "testuser", "host": "testhost"},
        "statusdb": {
            "username": "dbuser",
            "password": "dbpass",
            "url:": "dburl",
            "database": "dbname",
        },
        "sequencers": {
            "NextSeq": {
                "miarka_destination": "/data/NextSeq",
                "metadata_for_statusdb": ["RunInfo.xml", "RunParameters.xml"],
                "ignore_folders": ["nosync"],
                "rsync_options": ["--chmod=Dg+s,g+rw"],
            }
        },
    }
    run_id = "251015_VH00203_572_AAHFHCCM5"
    run_dir = tmp_path / run_id
    run_dir.mkdir()
    return illumina_runs.NextSeqRun(str(run_dir), config)


@pytest.fixture
def miseqseq_testobj(tmp_path):
    config = {
        "log": {"file": "test.log"},
        "transfer_details": {"user": "testuser", "host": "testhost"},
        "statusdb": {
            "username": "dbuser",
            "password": "dbpass",
            "url:": "dburl",
            "database": "dbname",
        },
        "sequencers": {
            "MiSeq": {
                "miarka_destination": "/data/MiSeq",
                "metadata_for_statusdb": ["RunInfo.xml", "RunParameters.xml"],
                "ignore_folders": ["nosync"],
                "rsync_options": ["--chmod=Dg+s,g+rw"],
            }
        },
    }
    run_id = "251015_M01548_0646_000000000-M6D7K"
    run_dir = tmp_path / run_id
    run_dir.mkdir()
    return illumina_runs.MiSeqRun(str(run_dir), config)


@pytest.fixture
def miseqseqi100_testobj(tmp_path):
    config = {
        "log": {"file": "test.log"},
        "transfer_details": {"user": "testuser", "host": "testhost"},
        "statusdb": {
            "username": "dbuser",
            "password": "dbpass",
            "url:": "dburl",
            "database": "dbname",
        },
        "sequencers": {
            "MiSeqi100": {
                "miarka_destination": "/data/MiSeqi100",
                "metadata_for_statusdb": ["RunInfo.xml", "RunParameters.xml"],
                "ignore_folders": ["nosync"],
                "rsync_options": ["--chmod=Dg+s,g+rw"],
            }
        },
    }
    run_id = "20260128_SH01140_0002_ASC2150561-SC3"
    run_dir = tmp_path / run_id
    run_dir.mkdir()
    return illumina_runs.MiSeqi100Run(str(run_dir), config)


@pytest.fixture(autouse=True)
def mock_statusdbsession(monkeypatch):
    class MockStatusdbSession:
        def __init__(self, config):
            pass

        def get_db_doc(self, ddoc, view, run_id):
            return None

        def update_db_doc(self, doc):
            pass

    monkeypatch.setattr(generic_runs, "StatusdbSession", MockStatusdbSession)


@pytest.mark.parametrize(
    "run_fixture, expected_run_type",
    [
        ("novaseqxplus_testobj", "NovaSeqXPlus"),
        ("nextseq_testobj", "NextSeq"),
        ("miseqseq_testobj", "MiSeq"),
        ("miseqseqi100_testobj", "MiSeqi100"),
    ],
)
def test_confirm_run_type(run_fixture, expected_run_type, request):
    run_obj = request.getfixturevalue(run_fixture)
    assert run_obj.run_type == expected_run_type
    # This should not raise an exception
    run_obj.confirm_run_type()
    run_obj.run_id = "invalid_run_id"
    with pytest.raises(ValueError):
        run_obj.confirm_run_type()


@pytest.mark.parametrize(
    "run_fixture",
    [
        "novaseqxplus_testobj",
        "nextseq_testobj",
        "miseqseq_testobj",
        "miseqseqi100_testobj",
    ],
)
def test_sequencing_ongoing(run_fixture, request):
    run_obj = request.getfixturevalue(run_fixture)
    # Initially, the final file does not exist, so sequencing should be ongoing
    assert run_obj.sequencing_ongoing is True
    # Create the final file to simulate sequencing completion
    final_file_path = os.path.join(run_obj.run_dir, run_obj.final_file)
    with open(final_file_path, "w") as f:
        f.write("Sequencing complete")
    assert run_obj.sequencing_ongoing is False


@pytest.mark.parametrize(
    "run_fixture, final_sync",
    [
        ("novaseqxplus_testobj", False),
        ("novaseqxplus_testobj", True),
        ("nextseq_testobj", False),
        ("nextseq_testobj", True),
        ("miseqseq_testobj", False),
        ("miseqseq_testobj", True),
        ("miseqseqi100_testobj", False),
        ("miseqseqi100_testobj", True),
    ],
)
def test_generate_rsync_command(run_fixture, final_sync, request):
    run_obj = request.getfixturevalue(run_fixture)
    rsync_command = run_obj.generate_rsync_command(is_final_sync=final_sync)
    assert "run-one rsync" in rsync_command
    assert "--log-file=" in rsync_command
    assert "--chmod=Dg+s,g+rw" in rsync_command
    assert run_obj.run_dir in rsync_command
    if final_sync:
        assert f"; echo $? > {run_obj.final_rsync_exitcode_file}" in rsync_command


@pytest.mark.parametrize(
    "run_fixture, rsync_running, final",
    [
        ("novaseqxplus_testobj", False, False),
        ("novaseqxplus_testobj", True, False),
        ("novaseqxplus_testobj", False, True),
        ("novaseqxplus_testobj", True, True),
        ("nextseq_testobj", False, False),
        ("nextseq_testobj", True, False),
        ("nextseq_testobj", False, True),
        ("nextseq_testobj", True, True),
        ("miseqseq_testobj", False, False),
        ("miseqseq_testobj", True, False),
        ("miseqseq_testobj", False, True),
        ("miseqseq_testobj", True, True),
        ("miseqseqi100_testobj", False, False),
        ("miseqseqi100_testobj", True, False),
        ("miseqseqi100_testobj", False, True),
        ("miseqseqi100_testobj", True, True),
    ],
)
def test_start_transfer(run_fixture, rsync_running, final, request, monkeypatch):
    run_obj = request.getfixturevalue(run_fixture)

    def mock_rsync_is_running(src):
        return rsync_running

    def mock_submit_background_process(command_str):
        mock_submit_background_process.called = True
        mock_submit_background_process.command_str = command_str

    def mock_update_statusdb(status, additional_info=None):
        mock_update_statusdb.called = True
        mock_update_statusdb.status = status

    monkeypatch.setattr(generic_runs.fs, "rsync_is_running", mock_rsync_is_running)
    monkeypatch.setattr(
        generic_runs.fs, "submit_background_process", mock_submit_background_process
    )
    monkeypatch.setattr(run_obj, "update_statusdb", mock_update_statusdb)

    run_obj.start_transfer(final=final)

    if rsync_running:
        assert not hasattr(mock_submit_background_process, "called")
    else:
        assert hasattr(mock_submit_background_process, "called")
        assert "rsync" in mock_submit_background_process.command_str
        assert hasattr(mock_update_statusdb, "called")
        if final:
            assert mock_update_statusdb.status == "final_transfer_started"
        else:
            assert mock_update_statusdb.status == "transfer_started"


@pytest.mark.parametrize(
    "run_fixture, sync_successful",
    [
        ("novaseqxplus_testobj", True),
        ("novaseqxplus_testobj", False),
        ("nextseq_testobj", True),
        ("nextseq_testobj", False),
        ("miseqseq_testobj", True),
        ("miseqseq_testobj", False),
        ("miseqseqi100_testobj", True),
        ("miseqseqi100_testobj", False),
    ],
)
def test_final_sync_successful(run_fixture, sync_successful, request):
    run_obj = request.getfixturevalue(run_fixture)
    if sync_successful:
        # Create the final rsync exit code file with a success code
        with open(run_obj.final_rsync_exitcode_file, "w") as f:
            f.write("0")
    else:
        # Create the final rsync exit code file with a failure code
        with open(run_obj.final_rsync_exitcode_file, "w") as f:
            f.write("1")
    assert run_obj.final_sync_successful == sync_successful


@pytest.mark.parametrize(
    "run_fixture, status_to_check, expected_result",
    [
        ("novaseqxplus_testobj", "sequencing_started", False),
        ("novaseqxplus_testobj", "sequencing_started", True),
        ("novaseqxplus_testobj", "sequencing_finished", False),
        ("novaseqxplus_testobj", "sequencing_finished", True),
        ("nextseq_testobj", "sequencing_started", False),
        ("nextseq_testobj", "sequencing_started", True),
        ("nextseq_testobj", "sequencing_finished", False),
        ("nextseq_testobj", "sequencing_finished", True),
        ("miseqseq_testobj", "sequencing_started", False),
        ("miseqseq_testobj", "sequencing_started", True),
        ("miseqseq_testobj", "sequencing_finished", False),
        ("miseqseq_testobj", "sequencing_finished", True),
        ("miseqseqi100_testobj", "sequencing_started", False),
        ("miseqseqi100_testobj", "sequencing_started", True),
        ("miseqseqi100_testobj", "sequencing_finished", False),
        ("miseqseqi100_testobj", "sequencing_finished", True),
    ],
)
def test_has_status(run_fixture, status_to_check, expected_result, request):
    run_obj = request.getfixturevalue(run_fixture)

    class MockDB:
        def get_events(self, run_id):
            if expected_result:
                return {"rows": [{"value": {status_to_check: True}}]}
            else:
                return {"rows": [{"value": {}}]}

    run_obj.db = MockDB()
    assert run_obj.has_status(status_to_check) == expected_result


@pytest.mark.parametrize(
    "run_fixture, existing_statuses, status_to_update",
    [
        ("novaseqxplus_testobj", [], "sequencing_started"),
        (
            "novaseqxplus_testobj",
            [{"event_type": "sequencing_started"}],
            "transfer_started",
        ),
        (
            "nextseq_testobj",
            [],
            "sequencing_started",
        ),
        (
            "nextseq_testobj",
            [{"event_type": "sequencing_started"}],
            "transfer_started",
        ),
        (
            "miseqseq_testobj",
            [],
            "sequencing_started",
        ),
        (
            "miseqseq_testobj",
            [{"event_type": "sequencing_started"}],
            "transfer_started",
        ),
        (
            "miseqseqi100_testobj",
            [],
            "sequencing_started",
        ),
        (
            "miseqseqi100_testobj",
            [{"event_type": "sequencing_started"}],
            "transfer_started",
        ),
    ],
)
def test_update_statusdb(
    run_fixture,
    existing_statuses,
    status_to_update,
    request,
):
    run_obj = request.getfixturevalue(run_fixture)

    class MockDB:
        def __init__(self):
            self.updated_doc = None

        def get_db_doc(self, ddoc, view, run_id):
            return {"events": existing_statuses, "files": {}}

        def update_db_doc(self, doc):
            self.updated_doc = doc

    import dataflow_transfer.utils.filesystem as fs

    def mock_locate_metadata(metadata_list, run_dir):
        return []

    def mock_parse_metadata_files(files):
        return {}

    fs.locate_metadata = mock_locate_metadata
    fs.parse_metadata_files = mock_parse_metadata_files
    mock_db = MockDB()
    run_obj.db = mock_db
    run_obj.update_statusdb(status=status_to_update)
    assert mock_db.updated_doc["events"][-1]["event_type"] == status_to_update
