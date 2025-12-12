import os
import pytest


from dataflow_transfer.run_classes import illumina_runs, generic_runs


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


# mock calls to dataflow_transfer.utils.statusdb.StatusdbSession to avoid actual DB connections
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


# use parameterization for the test fixtures to test confirm_run_type
@pytest.mark.parametrize(
    "run_fixture, expected_run_type",
    [
        ("nextseq_testobj", "NextSeq"),
        ("miseqseq_testobj", "MiSeq"),
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
        "nextseq_testobj",
        "miseqseq_testobj",
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
        ("nextseq_testobj", False),
        ("nextseq_testobj", True),
        ("miseqseq_testobj", False),
        ("miseqseq_testobj", True),
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


def test_initiate_background_transfer():
    pass  # Further tests can be implemented for initiate_background_transfer


def test_do_final_transfer():
    pass  # Further tests can be implemented for do_final_transfer


@pytest.mark.parametrize(
    "run_fixture, sync_successful",
    [
        ("nextseq_testobj", True),
        ("nextseq_testobj", False),
        ("miseqseq_testobj", True),
        ("miseqseq_testobj", False),
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


# use fixtures to test Run.has_status for differernt illumina_runs objects
@pytest.mark.parametrize(
    "run_fixture, status_to_check, expected_result",
    [
        ("nextseq_testobj", "sequencing_started", False),
        ("nextseq_testobj", "sequencing_started", True),
        ("nextseq_testobj", "sequencing_finished", False),
        ("nextseq_testobj", "sequencing_finished", True),
        ("miseqseq_testobj", "sequencing_started", False),
        ("miseqseq_testobj", "sequencing_started", True),
        ("miseqseq_testobj", "sequencing_finished", False),
        ("miseqseq_testobj", "sequencing_finished", True),
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
