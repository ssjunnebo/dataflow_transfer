import click
import os
import logging
import yaml

from dataflow_transfer.dataflow_transfer import transfer_runs
from dataflow_transfer import log
from dataflow_transfer.run_classes.registry import RUN_CLASS_REGISTRY

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
@click.option(
    "-s",
    "--sequencer",
    required=False,
    type=click.Choice(list(RUN_CLASS_REGISTRY.keys())),
    default=None,
    help="Sequencer type of the run, e.g., NovaSeqXPlus, MiSeq, AVITI. Only valid if --run is specified.",
)
def cli(config_file, run, sequencer):
    """
    Command line interface for dataflow_transfer.

    This CLI allows users to specify a run identifier to transfer.

    Args:
        config_file (file): Path to the configuration file.
        run (str): A identifier, e.g., '20250528_LH00217_0219_A22TT52LT4'.
        sequencer (str): Sequencer type, e.g., 'NovaSeqXPlus', 'MiSeq', 'AVITI'.
    """
    if sequencer and not run:
        raise click.UsageError(
            "--sequencer/-s can only be used together with --run/-r."
        )
    if run and not sequencer:
        raise click.UsageError(
            "--run/-r requires --sequencer/-s to be specified."
        )
    config = load_config(config_file.name)
    log_file = config.get("log", {}).get("file", None)
    if log_file:
        level = config.get("log").get("log_level", "INFO")
        log.init_logger_file(log_file, level)
    transfer_runs(config, run, sequencer)
