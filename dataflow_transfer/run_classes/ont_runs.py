from dataflow_transfer.run_classes.generic_runs import Run


class NanoporeRun(Run):
    """Defines a Nanopore sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.final_file = "final_summary.txt"
        self.run_type = "Nanopore"


class PromethIONRun(NanoporeRun):
    """Defines a PromethION sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.run_type = "PromethION"
        self.miarka_destination = "/ont/promethion"


class MinIONRun(NanoporeRun):
    """Defines a MinION sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.run_type = "MinION"
        self.miarka_destination = "/ont/minion"
