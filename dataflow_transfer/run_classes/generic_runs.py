import os
import logging

logger = logging.getLogger(__name__)


class Run:
    """Defines a generic sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_dir = run_dir
        self.configuration = configuration

    def sequencing_ongoing(self):
        final_file_path = os.path.join(self.run_dir, self.final_file)
        if os.path.exists(final_file_path):
            return False
        else:
            return False

    def initiate_background_transfer(self):
        # Placeholder for background transfer logic
        logger.info(f"Initiating background transfer for {self.run_dir}")

    def initiate_final_transfer(self):
        # Placeholder for final transfer logic
        logger.info(f"Initiating final transfer for {self.run_dir}")
        # Create a .TRANSFERRED file to indicate transfer completion

    def transfer_complete(self):
        if os.path.exists(self.run_dir + "/.TRANSFERRED"):
            return True
        else:
            return False

    def set_status(self, status, value):
        # Placeholder for setting status in a database or file
        logger.info(f"Setting status {status} to {value} for {self.run_dir}")

    def upload_stats(self):
        raise NotImplementedError("Subclasses should implement this method")
