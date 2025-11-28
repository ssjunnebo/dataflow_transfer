import json
import logging
import os
import xmltodict
import subprocess

logger = logging.getLogger(__name__)


def get_run_dir(run):
    if os.path.isabs(run) and os.path.isdir(run):
        return run
    elif os.path.isdir(run):
        return os.path.abspath(run)
    else:
        raise ValueError(f"Provided run path is not a valid directory: {run}")


def find_runs(base_dir, ignore_folders=[]):
    """Find run directories in the given base directory, ignoring specified folders."""
    runs = []
    for entry in os.listdir(base_dir):
        entry_path = os.path.join(base_dir, entry)
        if os.path.isdir(entry_path) and entry not in ignore_folders:
            runs.append(entry_path)
    return runs


def rsync_is_running(src):
    """Check if rsync is already running for given src."""
    pattern = f"rsync.*{src}"
    try:
        subprocess.check_output(["pgrep", "-f", pattern])
        return True
    except subprocess.CalledProcessError:
        return False


def submit_background_process(command_str: str):
    """Submit a command string as a background process."""

    background_process = subprocess.Popen(
        command_str, stdout=subprocess.PIPE, shell=True
    )


def parse_metadata_files(files):
    """Given a list of files, read the content into a dict.
    Handle .json and .xml files differently."""
    metadata = {}
    for file_path in files:
        try:
            if file_path.endswith(".json"):
                with open(file_path, "r") as f:
                    metadata[os.path.basename(file_path)] = json.load(f)
            elif file_path.endswith(".xml"):
                with open(file_path, "r") as f:
                    xml_content = xmltodict.parse(
                        f.read(), attr_prefix="", cdata_key="text"
                    )
                    metadata[os.path.basename(file_path)] = xml_content
            else:
                logger.warning(
                    f"Unsupported metadata file type for {file_path}. Only .json and .xml are supported."
                )
                continue
        except Exception as e:
            logger.error(f"Error reading metadata file {file_path}: {e}")
    return metadata


def check_exit_status(file_path):
    """Check the exit status from a given file. 
    Return True if exit code is 0, else False."""
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            exit_code = f.read().strip()
            if exit_code == "0":
                return True
    return False


def locate_metadata(metadata_list, run_dir):
    """Locate metadata in the given run directory."""
    located_paths = []
    for pattern in metadata_list:
        metadata_path = os.path.join(run_dir, pattern)
        if os.path.exists(metadata_path):
            located_paths.append(metadata_path)
    return located_paths
