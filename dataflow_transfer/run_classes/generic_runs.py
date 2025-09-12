class Run:
    """Defines a generic sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_dir = run_dir
        self.configuration = configuration
        self.status = {
            "Sequenced": False,
            "Transferred": False,
        }
    
    def check_sequencing_status(self):
        # Placeholder method to check sequencing status
        # This should be implemented in subclasses
        raise NotImplementedError("Subclasses should implement this method")