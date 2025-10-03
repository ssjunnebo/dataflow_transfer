import os
from dataflow_transfer.run_classes.generic_runs import Run


class IlluminaRun(Run):
    """Defines an Illumina sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.final_file = "RTAComplete.txt"
        self.run_type = "Illumina"

    def upload_stats(self):
        # TODO: implement actual stats upload logic. Look at TACA aviti
        print(f"Uploading stats for {self.run_dir}")


class NovaSeqXPlusRun(IlluminaRun):
    """Defines a NovaSeq X Plus sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.run_type = "NovaSeqXPlus"
        self.miarka_destination = "/illumina/novaseqxplus"


class NextSeqRun(IlluminaRun):
    """Defines a NextSeq sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.run_type = "NextSeq"
        self.miarka_destination = "/illumina/nextseq"


class MiSeqRun(IlluminaRun):
    """Defines a MiniSeq sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.run_type = "MiniSeq"
        self.miarka_destination = "/illumina/miseq"
