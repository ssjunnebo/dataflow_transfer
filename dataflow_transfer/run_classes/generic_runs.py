import os
import logging
import re
from datetime import datetime

from dataflow_transfer.utils.transfer import (
    rsync_is_running,
    transfer,
    make_rsync_include_options,
)
from dataflow_transfer.utils.statusdb import StatusdbSession
from dataflow_transfer.utils.filesystem import (
    parse_metadata_files,
    check_exit_status,
    locate_metadata,
)

logger = logging.getLogger(__name__)


class Run:
    """Defines a generic sequencing run"""

    def __init__(self, run_dir, configuration):
        self.run_dir = run_dir
        self.run_id = os.path.basename(run_dir)
        self.configuration = configuration
        self.sequencer_config = self.configuration.get(getattr(self, "run_type", None))
        self.final_file = ""
        self.transfer_details = self.configuration.get("transfer_details", {})
        self.final_rsync_exitcode_file = os.path.join(
            self.run_dir, "final_rsync_exitcode"
        )
        self.metadata_sync_exitcode_file = os.path.join(
            self.run_dir, "metadata_sync_exitcode"
        )
        self.nasns_destination = self.sequencer_config.get("nasns_destination")
        self.miarka_destination = self.sequencer_config.get("miarka_destination")
        self.db = StatusdbSession(self.configuration.get("statusdb"))

    def confirm_run_type(self):
        """Compare run ID with expected format for the run type."""
        if not re.match(self.run_id_format, self.run_id):
            raise ValueError(
                f"Run ID {self.run_id} does not match expected format for {getattr(self, 'run_type', 'Unknown')} runs."
            )

    def sequencing_ongoing(self):
        """Check if sequencing is still ongoing by looking for the absence of the final file."""
        final_file_path = os.path.join(self.run_dir, self.final_file)
        if os.path.exists(final_file_path):
            return False
        else:
            return True

    def generate_rsync_command(self, exit_code_file=None, remote=False):
        """Generate an rsync command string."""
        if remote:
            log_file = os.path.join(self.run_dir, "rsync_remote_log.txt")
            additional_options = [
                "--chown=" + self.transfer_details.get("owner"),
                "--chmod=" + self.transfer_details.get("permissions"),
            ]
            destination = (
                self.transfer_details.get("user")
                + "@"
                + self.transfer_details.get("host")
                + ":"
                + self.miarka_destination
            )
        else:
            log_file = os.path.join(self.run_dir, "rsync_nasns_log.txt")
            include_patterns = self.sequencer_config.get("metadata_for_nasns", [])
            include = make_rsync_include_options(include_patterns, self.run_dir)
            additional_options = [include, "--exclude='*'"]  # exclude everything else
            destination = self.nasns_destination

        command = [
            "run-one",
            "rsync",
            "-au",
            "--log-file=" + log_file,
        ]
        command.extend(additional_options)
        command.extend[
            self.run_dir,
            destination,
        ]

        if exit_code_file:
            command.extend(
                [
                    f"; echo $? > {exit_code_file}",
                ]
            )
        command_str = " ".join(command)
        return command_str

    def initiate_background_transfer(self):
        """Start background rsync transfer to storage."""
        logger.info(f"Initiating background transfer for {self.run_dir}")
        background_transfer_command = self.generate_rsync_command(
            exit_code_file=self.final_rsync_exitcode_file, remote=True
        )
        if rsync_is_running(src=self.run_dir):
            logger.info(
                f"Rsync is already running for {self.run_dir}. Skipping background transfer initiation."
            )
            return
        transfer(background_transfer_command)
        logger.info(
            f"{self.run_id}: Started background rsync to {self.miarka_destination}"
            + f" with the following command: '{background_transfer_command}'"
        )
        rsync_info = {
            "command": background_transfer_command,
            "destination_path": self.miarka_destination,
        }
        self.update_statusdb(status="transfer_started", additional_info=rsync_info)

    def do_final_transfer(self):
        """Start final rsync transfer to storage."""
        logger.info(f"Initiating final transfer for {self.run_dir}")

        final_transfer_command = self.generate_rsync_command(remote=True)
        if rsync_is_running(src=self.run_dir):
            logger.info(
                f"Rsync is already running for {self.run_dir}. Skipping final transfer initiation."
            )
            return

        transfer(final_transfer_command)
        logger.info(
            f"{self.run_id}: Started FINAL rsync to {self.miarka_destination}"
            + f" with the following command: '{final_transfer_command}'"
        )
        rsync_info = {
            "command": final_transfer_command,
            "destination_path": self.miarka_destination,
        }
        self.update_statusdb(
            status="final_transfer_started", additional_info=rsync_info
        )

    def final_sync_successful(self):
        """Check if the final rsync transfer was successful by reading the exit code file."""
        return check_exit_status(self.final_rsync_exitcode_file)

    def metadata_synced(self):
        """Check if metadata has been synced by looking for an indicator file."""
        return check_exit_status(self.metadata_sync_exitcode_file)

    def sync_metadata(self):
        # Look for the indicator file and exit code. If the exit code is 0, update statusdb. Otherwise retry.
        logger.info(
            f"Initiating background transfer of metadata to ngi-nas-ns for {self.run_dir}"
        )
        metadata_rsync_command = self.generate_rsync_command(
            exit_code_file=self.metadata_sync_exitcode_file, remote=False
        )
        if rsync_is_running(src=self.run_dir):  # TODO: check destination as well
            logger.info(
                f"rsync to ngi-nas-ns is already running for {self.run_dir}. Skipping background transfer initiation."
            )
            return
        transfer(metadata_rsync_command)
        logger.info(
            f"{self.run_id}: Started background rsync to {self.nasns_destination}"
            + f" with the following command: '{metadata_rsync_command}'"
        )
        rsync_info = {
            "command": metadata_rsync_command,
            "destination_path": self.nasns_destination,
        }
        self.update_statusdb(status="metadata_sync_started", additional_info=rsync_info)

    def transfer_complete(self):
        """Check if the final rsync exit code file exists, indicating transfer completion."""
        return os.path.exists(self.final_rsync_exitcode_file)

    def get_status(self, status_name):
        """Check if a specific status exists in the statusdb events for this run."""
        events = self.db.get_events(self.run_id)["rows"]
        current_statuses = {}
        if events:
            current_statuses = events[0]["value"]
        if current_statuses.get(status_name):
            return True
        else:
            return False

    def update_statusdb(self, status, additional_info=None):
        """Update the statusdb document for this run with the given status and associated metadata files."""
        logger.info(f"Setting status {status} for {self.run_dir}")
        db_doc = self.db.get_db_doc(
            ddoc="lookup", view="runfolder_id", run_id=self.run_id
        )

        statuses_to_only_update_once = [  # some statuses should only be updated once
            "sequencing_started",
            "sequencing_finished",
            "metadata_synced",
        ]
        if status in statuses_to_only_update_once:
            for event in db_doc.get("events", []):
                if event["status"] == status:
                    return

        if not db_doc:
            db_doc = {
                "runfolder_id": self.run_id,
                "flowcell_id": self.flowcell_id,
                "events": [],
            }

        files_to_include = locate_metadata(
            self.sequencer_config.get("metadata_for_statusdb", [])
        )
        if db_doc.get("files", {}):
            for f in files_to_include:
                if os.path.basename(f) in db_doc["files"]:
                    files_to_include.remove(f)
                    # TODO: This excludes files that are already uploaded, but could there be incomplete documents that should be updated? Is it better to always re-parse and update?
        else:
            # Initialize files dict if not present. This happens if the sample sheet daemon created the initial document without files.
            db_doc["files"] = {}
        parsed_files = parse_metadata_files(files_to_include)
        db_doc["files"].update(parsed_files)
        db_doc["events"].append(
            {
                "status": status,
                "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "data": additional_info or {},
            }
        )
        self.db.update_db_doc(db_doc)
