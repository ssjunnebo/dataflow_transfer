from dataflow_transfer.run_classes.generic_runs import Run

from .registry import register_run_class


class ONTRun(Run):
    """Defines a ONT sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.final_file = "final_summary.txt"
        self.flowcell_id = self.run_id.split("_")[-2]


@register_run_class
class PromethIONRun(ONTRun):
    """Defines a PromethION sequencing run"""

    run_type = "PromethION"

    def __init__(self, run_dir, configuration):
        self.run_id_format = r"^\d{8}_\d{4}_[A-Z0-9]{2}_P[A-Z0-9]+_[a-f0-9]{8}$"  # 20251015_1051_3B_PBG60686_0af3a2e0
        super().__init__(run_dir, configuration)


@register_run_class
class MinIONRun(ONTRun):
    """Defines a MinION sequencing run"""

    run_type = "MinION"

    def __init__(self, run_dir, configuration):
        self.run_id_format = r"^\d{8}_\d{4}_MN[A-Z0-9]+_[A-Z0-9]+_[a-f0-9]{8}$"  # 20240229_1404_MN19414_ASH657_7a74bf8f
        super().__init__(run_dir, configuration)
