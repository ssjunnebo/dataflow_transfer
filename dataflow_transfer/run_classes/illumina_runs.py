from dataflow_transfer.run_classes.generic_runs import Run


class IlluminaRun(Run):
    """Defines an Illumina sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_type = "Illumina"
        super().__init__(run_dir, configuration)
        self.final_file = "RTAComplete.txt"


class NovaSeqXPlusRun(IlluminaRun):
    """Defines a NovaSeq X Plus sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_type = "NovaSeqXPlus"
        super().__init__(run_dir, configuration)


class NextSeqRun(IlluminaRun):
    """Defines a NextSeq sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_type = "NextSeq"
        super().__init__(run_dir, configuration)


class MiSeqRun(IlluminaRun):
    """Defines a MiniSeq sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_type = "MiniSeq"
        super().__init__(run_dir, configuration)
