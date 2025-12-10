from dataflow_transfer.run_classes import illumina_runs
from dataflow_transfer.run_classes.generic_runs import Run


def _patch_run_init(monkeypatch, run_id):
    def fake_init(self, run_dir, configuration):
        # minimal attributes the classes under test expect
        self.run_dir = run_dir
        self.configuration = configuration
        self.run_id = run_id

    monkeypatch.setattr(Run, "__init__", fake_init)


def test_novaseqxplus_has_expected_attributes_and_flowcell(monkeypatch):
    run_id = "20251010_LH00202_0284_B22CVHTLT1"
    _patch_run_init(monkeypatch, run_id)

    inst = illumina_runs.NovaSeqXPlusRun("/tmp/run", {"unused": True})

    assert isinstance(inst, illumina_runs.IlluminaRun)
    assert inst.final_file == "RTAComplete.txt"
    assert inst.flowcell_id == "B22CVHTLT1"
    assert inst.run_type == "NovaSeqXPlus"
    assert inst.run_id_format == r"^\d{8}_[A-Z0-9]+_\d{4}_[A-Z0-9]+$"


def test_nextseq_has_expected_attributes_and_flowcell(monkeypatch):
    run_id = "251015_VH00203_572_AAHFHCCM5"
    _patch_run_init(monkeypatch, run_id)

    inst = illumina_runs.NextSeqRun("/tmp/run", {"unused": True})

    assert isinstance(inst, illumina_runs.IlluminaRun)
    assert inst.final_file == "RTAComplete.txt"
    assert inst.flowcell_id == "AAHFHCCM5"
    assert inst.run_type == "NextSeq"
    assert inst.run_id_format == r"^\d{6}_[A-Z0-9]+_\d{3}_[A-Z0-9]+$"


def test_miseq_has_expected_attributes_and_flowcell_with_hyphen(monkeypatch):
    run_id = "251015_M01548_0646_000000000-M6D7K"
    _patch_run_init(monkeypatch, run_id)

    inst = illumina_runs.MiSeqRun("/tmp/run", {"unused": True})

    assert isinstance(inst, illumina_runs.IlluminaRun)
    assert inst.final_file == "RTAComplete.txt"
    assert inst.flowcell_id == "000000000-M6D7K"
    assert inst.run_type == "MiSeq"
    assert inst.run_id_format == r"^\d{6}_[A-Z0-9]+_\d{4}_[A-Z0-9\-]+$"
