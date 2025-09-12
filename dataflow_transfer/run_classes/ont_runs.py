import os

from dataflow_transfer.run_classes.generic_runs import Run


class NanoporeRun(Run):
    """Defines a Nanopore sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.final_file = "final_summary.txt"
        self.run_type = "Nanopore"

    def check_sequencing_status(self):
        # TODO: Implement logic to check sequencing status for Nanopore runs
        # For example, check if the final_file exists in the run_dir
        final_file_path = os.path.join(self.run_dir, self.final_file)
        if os.path.exists(final_file_path):
            self.status["Sequenced"] = True
        else:
            self.status["Sequenced"] = False
        return self.status


class PromethIONRun(NanoporeRun):
    """Defines a PromethION sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.run_type = "PromethION"


class MinIONRun(NanoporeRun):
    """Defines a MinION sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.run_type = "MinION"
