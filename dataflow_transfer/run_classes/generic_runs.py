import os
import logging

from dataflow_transfer.utils.transfer import sync_to_hpc

logger = logging.getLogger(__name__)


class Run:
    """Defines a generic sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_dir = run_dir
        self.run_id = os.path.basename(run_dir)
        self.configuration = configuration
        self.final_file = ""
        self.rsync_log = os.path.join(
            self.run_dir, "rsync.log"
        )  # TODO: add timestamp to log filename
        self.miarka_destination = self.configuration.get("miarka_destination").get(
            getattr(self, "run_type", None)
        )

    def confirm_run_type(self):
        # compare run ID with expected format for NovaSeq X Plus
        # TODO: check that run_id_format is correct regex
        pass

    def sequencing_ongoing(self):
        final_file_path = os.path.join(self.run_dir, self.final_file)
        if os.path.exists(final_file_path):
            return False
        else:
            return True

    def initiate_background_transfer(self):
        logger.info(f"Initiating background transfer for {self.run_dir}")
        sync_to_hpc(
            run_path=self.run_dir,
            destination=self.miarka_destination,
            rsync_log=self.rsync_log,
            background=True,
            transfer_details=self.configuration.get("transfer_details", {}),
        )

    def do_final_transfer(self):
        logger.info(f"Initiating final transfer for {self.run_dir}")
        sync_to_hpc(
            run_path=self.run_dir,
            destination=self.miarka_destination,
            rsync_log=self.rsync_log,
            background=False,
            transfer_details=self.configuration.get("transfer_details", {}),
        )

    def sync_metadata(self):
        # TODO: implement actual metadata sync logic. Look at TACA
        pass

    def transfer_complete(self):
        # TODO: check the status in statusdb
        pass

    def set_status(self, status, value):
        # TODO: implement actual status update logic. Look at TACA aviti
        logger.info(f"Setting status {status} to {value} for {self.run_dir}")

    def upload_stats(self):
        # TODO: implement actual stats upload logic. Each subclasss can have a "gather_info" method that is called here
        pass
