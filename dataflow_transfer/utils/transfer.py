import subprocess
import logging

logger = logging.getLogger(__name__)


def rsync_is_running(src):
    """Check if rsync is already running for given src and dst."""
    pattern = f"rsync.*{src}"
    try:
        subprocess.check_output(["pgrep", "-f", pattern])
        return True
    except subprocess.CalledProcessError:
        return False


def sync_to_hpc(command_str: str):
    """Sync the run to storage using rsync."""

    background_process = subprocess.Popen(
        command_str, stdout=subprocess.PIPE, shell=True
    )
