import yaml
import logging
import os

logger = logging.getLogger(__name__)


def process_run(run_dir, sequencer, config):
    # For each run dir:
    # check the status of the run based on the current state of the run dir
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
                    process_run(run_dir, sequencer, conf)
