import logging
import os
import re
from datetime import datetime

import dataflow_transfer.utils.filesystem as fs
from dataflow_transfer.utils.statusdb import StatusdbSession

logger = logging.getLogger(__name__)


class Run:
    """Defines a generic sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_dir = run_dir
        self.run_id = os.path.basename(run_dir)
        self.configuration = configuration
        self.sequencer_config = self.configuration.get("sequencers").get(
            getattr(self, "run_type", None)
        )
        self.final_file = ""
        self.transfer_details = self.configuration.get("transfer_details", {})
        self.metadata_rsync_exitcode_file = os.path.join(
            self.run_dir, ".metadata_rsync_exitcode"
        )
        self.metadata_destination = os.path.join(
            self.configuration.get("metadata_archive"),
            getattr(self, "run_type", None),
            self.run_id,
        )
        self.final_rsync_exitcode_file = os.path.join(
            self.run_dir, ".final_rsync_exitcode"
        )
        self.remote_destination = self.sequencer_config.get("remote_destination")
        self.db = StatusdbSession(self.configuration.get("statusdb"))

    def confirm_run_type(self):
        """Compare run ID with expected format for the run type."""
        if not re.match(self.run_id_format, self.run_id):
            raise ValueError(
                f"Run ID {self.run_id} does not match expected format for {getattr(self, 'run_type', 'Unknown')} runs."
            )

    @property
    def sequencing_ongoing(self):
        """Check if sequencing is still ongoing by looking for the absence of the final file."""
        final_file_path = os.path.join(self.run_dir, self.final_file)
        if os.path.exists(final_file_path):
            return False
        return True

    @property
    def metadata_synced(self):
        """Check if the metadata rsync was successful by reading the exit code file."""
        return fs.check_exit_status(self.metadata_rsync_exitcode_file)

    def sync_metadata(self):
        """Start background rsync transfer for metadata files."""
        metadata_rsync_command = self.generate_rsync_command(
            remote=False, with_exit_code_file=True
        )

        if fs.rsync_is_running(src=self.run_dir, dst=self.metadata_destination):
            logger.info(
                f"Metadata rsync is already running for {self.run_dir} to destination {self.metadata_destination}. Skipping background metadata sync initiation."
            )
            return
        try:
            fs.submit_background_process(metadata_rsync_command)
            logger.info(
                f"{self.run_id}: Started metadata rsync to {self.metadata_destination}"
                + f" with the following command: '{metadata_rsync_command}'"
            )
        except Exception as e:
            logger.error(f"Failed to start metadata rsync for {self.run_id}: {e}")
            raise e

    def generate_rsync_command(self, remote=False, with_exit_code_file=False):
        """Generate an rsync command string."""
        if remote:
            source = self.run_dir
            destination = (
                self.transfer_details.get("user")
                + "@"
                + self.transfer_details.get("host")
                + ":"
                + self.remote_destination
            )
            log_file_option = "--log-file=" + os.path.join(
                self.run_dir, "rsync_remote_log.txt"
            )
            rsync_options = self.sequencer_config.get("remote_rsync_options", [])
            exit_code_file = self.final_rsync_exitcode_file
        else:
            source = self.run_dir + "/"
            destination = self.metadata_destination + "/"
            log_file_option = "--log-file=" + os.path.join(
                self.run_dir, "rsync_metadata_log.txt"
            )
            rsync_options = self.sequencer_config.get("metadata_rsync_options", [])
            exit_code_file = self.metadata_rsync_exitcode_file
        run_one_bin = self.configuration.get("run_one_path", "run-one")
        command = [
            run_one_bin,
            "rsync",
            "-au",
            log_file_option,
            *(rsync_options),
            "--exclude='*'" if not remote else "",
            source,
            destination,
        ]
        command_str = " ".join(command)
        if with_exit_code_file:
            command_str += f"; echo $? > {exit_code_file}"
        return command_str

    def start_transfer(self, final=False):
        """Start background rsync transfer to storage."""
        transfer_command = self.generate_rsync_command(
            remote=True, with_exit_code_file=final
        )
        if fs.rsync_is_running(src=self.run_dir, dst=self.remote_destination):
            logger.info(
                f"Rsync is already running for {self.run_dir} to destination {self.remote_destination}. Skipping background transfer initiation."
            )
            return
        try:
            fs.submit_background_process(transfer_command)
            logger.info(
                f"{self.run_id}: Started rsync to {self.remote_destination}"
                + f" with the following command: '{transfer_command}'"
            )
        except Exception as e:
            logger.error(f"Failed to start rsync for {self.run_id}: {e}")
            raise e
        rsync_info = {
            "command": transfer_command,
            "destination_path": self.remote_destination,
        }
        if final:
            self.update_statusdb(
                status="final_transfer_started", additional_info=rsync_info
            )
        else:
            self.update_statusdb(status="transfer_started", additional_info=rsync_info)

    @property
    def final_sync_successful(self):
        """Check if the final rsync transfer was successful by reading the exit code file."""
        return fs.check_exit_status(self.final_rsync_exitcode_file)

    def has_status(self, status_name):
        """Check if a specific status exists in the statusdb events for this run."""
        events = self.db.get_events(self.run_id)["rows"]
        current_statuses = events[0].get("value", {}) if events else {}
        return True if current_statuses.get(status_name) else False

    def update_statusdb(self, status, additional_info=None):
        """Update the statusdb document for this run with the given status
        and associated metadata files."""
        db_doc = (
            self.db.get_db_doc(ddoc="lookup", view="runfolder_id", run_id=self.run_id)
            or {}
        )

        statuses_to_only_update_once = [
            "sequencing_started",
            "sequencing_finished",
        ]
        if status in statuses_to_only_update_once:
            for event in db_doc.get("events", []):
                if event["event_type"] == status:
                    return

        if not db_doc:
            db_doc = {
                "runfolder_id": self.run_id,
                "flowcell_id": self.flowcell_id,
                "events": [],
                "files": {},
            }
        files_to_include = fs.locate_metadata(
            self.sequencer_config.get("metadata_for_statusdb", []),
            self.run_dir,
        )
        parsed_files = fs.parse_metadata_files(files_to_include)
        db_doc["files"].update(parsed_files)
        db_doc["events"].append(
            {
                "event_type": status,
                "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "data": additional_info or {},
            }
        )
        logger.info(f"Setting status {status} for {self.run_dir}")
        self.db.update_db_doc(db_doc)
