import os
import logging
import re

from dataflow_transfer.utils.transfer import sync_to_hpc

logger = logging.getLogger(__name__)


class Run:
    """Defines a generic sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_dir = run_dir
        self.run_id = os.path.basename(run_dir)
        self.configuration = configuration
        self.final_file = ""
        self.rsync_log = os.path.join(self.run_dir, "rsync.log")
        self.final_rsync_exitcode_file = os.path.join(
            self.run_dir, "final_rsync_exitcode"
        )
        self.miarka_destination = self.configuration.get("miarka_destination").get(
            getattr(self, "run_type", None)
        )

    def confirm_run_type(self):
        # Compare run ID with expected format for the run type
        if not re.match(self.run_id_format, self.run_id):
            raise ValueError(
                f"Run ID {self.run_id} does not match expected format for {getattr(self, 'run_type', 'Unknown')} runs."
            )

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
            transfer_details=self.configuration.get("transfer_details", {}),
        )

    def do_final_transfer(self):
        logger.info(f"Initiating final transfer for {self.run_dir}")
        sync_to_hpc(
            run_path=self.run_dir,
            destination=self.miarka_destination,
            rsync_log=self.rsync_log,
            transfer_details=self.configuration.get("transfer_details", {}),
            rsync_exit_code_file=self.final_rsync_exitcode_file,
        )

    def final_sync_successful(self):
        if os.path.exists(self.final_rsync_exitcode_file):
            with open(self.final_rsync_exitcode_file, "r") as f:
                exit_code = f.read().strip()
                if exit_code == "0":
                    return True
        return False

    def sync_metadata(self):
        # TODO: implement actual metadata sync logic. Look at TACA
        pass

    def transfer_complete(self):
        return os.path.exists(self.final_rsync_exitcode_file)

    def get_status(self, status_name):
        # TODO: get statuses from view in statusdb and return true or false depending on if the status is set
        pass

    def update_statusdb(self, status):
        # TODO: implement actual status update logic. Look at TACA aviti. Always fetch the latest doc from statusdb and use this for updating
        logger.info(f"Setting status {status} for {self.run_dir}")
