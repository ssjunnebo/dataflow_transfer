import os
from dataflow_transfer.run_classes.generic_runs import Run


class IlluminaRun(Run):
    """Defines an Illumina sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.final_file = "RTAComplete.txt"
        self.run_type = "Illumina"

    def check_sequencing_status(self):
        # TODO: Implement logic to check sequencing status for Nanopore runs
        # For example, check if the final_file exists in the run_dir
        final_file_path = os.path.join(self.run_dir, self.final_file)
        if os.path.exists(final_file_path):
            self.status["Sequenced"] = True
        else:
            self.status["Sequenced"] = False
        return self.status


class NovaSeqXPlusRun(IlluminaRun):
    """Defines a NovaSeq X Plus sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.run_type = "NovaSeqXPlus"


class NextSeqRun(IlluminaRun):
    """Defines a NextSeq sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.run_type = "NextSeq"


class MiSeqRun(IlluminaRun):
    """Defines a MiniSeq sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.run_type = "MiniSeq"
