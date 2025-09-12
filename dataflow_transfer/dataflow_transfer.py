import logging
import os

from dataflow_transfer.run_classes.illumina_runs import (
    NovaSeqXPlusRun,
    NextSeqRun,
    MiSeqRun,
)
from dataflow_transfer.run_classes.ont_runs import PromethIONRun, MinIONRun
from dataflow_transfer.run_classes.element_runs import AvitiRun

logger = logging.getLogger(__name__)


def process_run(run_dir, sequencer, config):
    # Initiate a Run object based on sequencer type
    # e.g., run = IlluminaRun(run_dir, config)
    if sequencer == "NovaSeqXPlus":
        run = NovaSeqXPlusRun(run_dir, config)
    elif sequencer == "NextSeq":
        run = NextSeqRun(run_dir, config)
    elif sequencer == "MiSeq":
        run = MiSeqRun(run_dir, config)
    elif sequencer == "PromethION":
        run = PromethIONRun(run_dir, config)
    elif sequencer == "MinION":
        run = MinIONRun(run_dir, config)
    elif sequencer == "Aviti":
        run = AvitiRun(run_dir, config)
    else:
        logger.warning(f"Unknown sequencer type: {sequencer}. Skipping run: {run_dir}")
        return
    # check the status of the run based on the current state of the run dir
    sequencing_status = run.check_sequencing_status()
    # If the run is ongoing (i.e. final file does not exist):
    # start an rsync process in the background to transfer the data to Miarka (with run-one to avoid multiple concurrent transfers)
    # If the run is complete (i.e. final file exists):
    # update the "Sequenced" satus to True
    # initiate a final transfer
    # update the "transferred" status to True
    pass


def transfer_runs(conf, run=None):
    if run:
        logger.info(f"Transferring specific run: {run}")
        sequencer = "unknown"
        process_run(run, sequencer, conf)
    else:
        logger.info("Transferring all runs as per configuration")
        sequencing_dirs = conf.get("sequencing_dirs", {})
        for sequencer in sequencing_dirs.keys():
            logger.info(f"Processing data from: {sequencer}")
            sequencing_dir = sequencing_dirs.get(sequencer)
            for run_dir in os.listdir(sequencing_dir):
                run_dir_path = os.path.join(sequencing_dir, run_dir)
                if os.path.isdir(run_dir_path):
                    logger.info(f"Processing directory: {run_dir_path}")
                    try:
                        process_run(run_dir, sequencer, conf)
                    except Exception as e:
                        logger.error(f"Error processing run {run_dir}: {e}")
                        continue  # Continue with the next run
