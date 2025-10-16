from dataflow_transfer.run_classes.generic_runs import Run
from .registry import register_run_class


class IlluminaRun(Run):
    """Defines an Illumina sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.final_file = "RTAComplete.txt"


@register_run_class
class NovaSeqXPlusRun(IlluminaRun):
    """Defines a NovaSeq X Plus sequencing run"""

    run_type = "NextSeq"

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)


@register_run_class
class NextSeqRun(IlluminaRun):
    """Defines a NextSeq sequencing run"""

    run_type = "NextSeq"

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)


@register_run_class
class MiSeqRun(IlluminaRun):
    """Defines a MiSeq sequencing run"""

    run_type = "MiSeq"

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
