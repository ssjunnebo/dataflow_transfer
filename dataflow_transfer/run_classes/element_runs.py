from dataflow_transfer.run_classes.generic_runs import Run

from .registry import register_run_class


class ElementRun(Run):
    """Defines an Element sequencing run"""

    def __init__(self, run_dir, configuration):
        super().__init__(run_dir, configuration)
        self.final_file = "RunUploaded.json"
        self.flowcell_id = self.run_id.split("_")[
            -1
        ]  # This is true for all except Teton runs


@register_run_class
class AVITIRun(ElementRun):
    """Defines an AVITI sequencing run"""

    run_type = "AVITI"

    def __init__(self, run_dir, configuration):
        self.run_id_format = (
            "^\d{8}_AV\d{6}_(A|BP)\d{10}$"  # 20251007_AV242106_A2507535225
        )
        super().__init__(run_dir, configuration)
