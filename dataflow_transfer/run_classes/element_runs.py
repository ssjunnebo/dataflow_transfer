from dataflow_transfer.run_classes.generic_runs import Run
from .registry import register_run_class


class ElementRun(Run):
    """Defines an Element sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.final_file = "RunUploaded.json"


@register_run_class
class AVITIRun(ElementRun):
    """Defines an AVITI sequencing run"""

    run_type = "AVITI"

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
