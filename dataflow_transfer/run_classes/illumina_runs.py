from dataflow_transfer.run_classes.generic_runs import Run
from .registry import register_run_class


class IlluminaRun(Run):
    """Defines an Illumina sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.final_file = "RTAComplete.txt"
        self.flowcell_id = self.run_id.split("_")[-1]  # TODO: verify that this is true


@register_run_class
class NovaSeqXPlusRun(IlluminaRun):
    """Defines a NovaSeq X Plus sequencing run"""

    run_type = "NovaSeqXPlus"

    def __init__(self, run_dir, configuration):
        self.run_id_format = (
            "^\d{8}_[A-Z0-9]+_\d{4}_[A-Z0-9]+$"  # 20251010_LH00202_0284_B22CVHTLT1
        )
        super().__init__(run_dir, configuration)


@register_run_class
class NextSeqRun(IlluminaRun):
    """Defines a NextSeq sequencing run"""

    run_type = "NextSeq"

    def __init__(self, run_dir, configuration):
        self.run_id_format = (
            "^\d{6}_[A-Z0-9]+_\d{3}_[A-Z0-9]+$"  # 251015_VH00203_572_AAHFHCCM5
        )
        super().__init__(run_dir, configuration)


@register_run_class
class MiSeqRun(IlluminaRun):
    """Defines a MiSeq sequencing run"""

    run_type = "MiSeq"

    def __init__(self, run_dir, configuration):
        self.run_id_format = (
            "^\d{6}_[A-Z0-9]+_\d{4}_[A-Z0-9\-]+$"  # 251015_M01548_0646_000000000-M6D7K
        )
        super().__init__(run_dir, configuration)
