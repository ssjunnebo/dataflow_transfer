from dataflow_transfer.run_classes.generic_runs import Run
from .registry import register_run_class


class ONTRun(Run):
    """Defines a ONT sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.final_file = "final_summary.txt"


@register_run_class
class PromethIONRun(ONTRun):
    """Defines a PromethION sequencing run"""

    run_type = "PromethION"

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)


@register_run_class
class MinIONRun(ONTRun):
    """Defines a MinION sequencing run"""

    run_type = "MinION"

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
