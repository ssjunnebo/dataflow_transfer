import click
import os
import logging
import yaml

from dataflow_transfer.dataflow_transfer import transfer_runs
from dataflow_transfer import log

logger = logging.getLogger(__name__)

def load_config(config_file_path):
    with open(config_file_path, "r") as file:
        config = yaml.safe_load(file)
    return config

@click.command()
@click.option(
    "-c",
    "--config-file",
    default=os.path.join(os.environ["HOME"], ".df_transfer/df_transfer.yaml"),
    envvar="TRANSFER_CONFIG",
    type=click.File("r"),
    help="Path to dataflow_transfer configuration file",
)
@click.option(
    "-r",
    "--run",
    required=False,
    type=str,
    default=None,
    help="Only transfer a specific run, e.g., 20250528_LH00217_0219_A22TT52LT4.",
)
def cli(config_file, run):
    """
    Command line interface for dataflow_transfer.

    This CLI allows users to specify a run identifier to transfer.

    Args:
        config_file (file): Path to the configuration file.
        run (str): A identifier, e.g., '20250528_LH00217_0219_A22TT52LT4'.
    """
    config = load_config(config_file.name)
    log_file = config.get("log", {}).get("file", None)
    if log_file:
        level = config.get("log").get("log_level", "INFO")
        log.init_logger_file(log_file, level)
    transfer_runs(config, run)
