from dataflow_transfer.run_classes.generic_runs import Run


class ONTRun(Run):
    """Defines a ONT sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.final_file = "final_summary.txt"


class PromethIONRun(ONTRun):
    """Defines a PromethION sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_type = "PromethION"
        super().__init__(run_dir, configuration)


class MinIONRun(ONTRun):
    """Defines a MinION sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_type = "MinION"
        super().__init__(run_dir, configuration)
