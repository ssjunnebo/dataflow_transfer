import subprocess
import logging
import os

logger = logging.getLogger(__name__)


def rsync_is_running(src):
    """Check if rsync is already running for given src."""
    pattern = f"rsync.*{src}"
    try:
        subprocess.check_output(["pgrep", "-f", pattern])
        return True
    except subprocess.CalledProcessError:
        return False


def transfer(command_str: str):
    """Sync the run to storage using rsync."""

    background_process = subprocess.Popen(
        command_str, stdout=subprocess.PIPE, shell=True
    )


def make_rsync_include_options(patterns, run_dir):
    """Create rsync include options from a list of patterns."""
    include_options = []
    for pattern in patterns:
        if os.path.isfile(os.path.join(run_dir, pattern)):
            include_options.append(f"--include='{pattern}'")
        elif os.path.isdir(os.path.join(run_dir, pattern)):
            include_options.append(
                f"--include='{pattern}/***'"
            )  # include dir and all its contents
    return " ".join(include_options)
