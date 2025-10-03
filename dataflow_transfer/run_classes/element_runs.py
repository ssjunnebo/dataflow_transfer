from dataflow_transfer.run_classes.generic_runs import Run


class ElementRun(Run):
    """Defines an Element sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_type = "Element"
        super().__init__(run_dir, configuration)
        self.final_file = "RunUploaded.json"


class AvitiRun(ElementRun):
    """Defines an Aviti sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_type = "Aviti"
        super().__init__(run_dir, configuration)
