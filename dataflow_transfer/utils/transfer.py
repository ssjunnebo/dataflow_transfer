import subprocess
import logging
import os

logger = logging.getLogger(__name__)


def rsync_is_running(src, dst):
    """Check if rsync is already running for given src and dst."""
    pattern = f"rsync.*{src}.*{dst}"
    try:
        subprocess.check_output(["pgrep", "-f", pattern])
        return True
    except subprocess.CalledProcessError:
        return False


def sync_to_hpc(
    run_path: str,
    destination: str,
    rsync_log: str,
    background: bool,
    transfer_details: dict = {},
):
    """Sync the run to storage using rsync.
    Skip if rsync is already running on the run."""
    remote_destination = (
        transfer_details.get("user")
        + "@"
        + transfer_details.get("host")
        + ":"
        + destination
    )
    command = [
        "run-one",
        "rsync",
        "-au",
        "--log-file=" + rsync_log,
        "--chown=" + transfer_details.get("owner"),
        "--chmod=" + transfer_details.get("permissions"),
        remote_destination,
        run_path,
        destination,
    ]

    if rsync_is_running(
        src=run_path, dst=remote_destination
    ):  # TODO: check if this works as intended
        logger.info(
            f"{os.path.basename(run_path)}: Rsync to {remote_destination} is already running, skipping."
        )
        return False
    else:
        if background:
            p_background = subprocess.Popen(command)
            logger.info(
                f"{os.path.basename(run_path)}: Started background rsync to {remote_destination}"
                + f" with PID {p_background.pid} and the following command: '{' '.join(command)}'"
            )
        else:
            logger.info(
                f"{os.path.basename(run_path)}: Starting foreground rsync to {remote_destination}"
                + f" with the following command: '{' '.join(command)}'"
            )
            p_foreground = subprocess.run(command)
            if p_foreground.returncode == 0:
                logger.info(
                    f"{os.path.basename(run_path)}: Rsync to {remote_destination} finished successfully."
                )
                return True
            else:
                logger.error(
                    f"{os.path.basename(run_path)}: Rsync to {remote_destination} failed with error code {p_foreground.returncode}."
                )
                return False
