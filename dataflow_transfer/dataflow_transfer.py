import logging
import os

from dataflow_transfer.run_classes.illumina_runs import (
    NovaSeqXPlusRun,
    NextSeqRun,
    MiSeqRun,
)
from dataflow_transfer.run_classes.ont_runs import PromethIONRun, MinIONRun
from dataflow_transfer.run_classes.element_runs import AVITIRun

logger = logging.getLogger(__name__)


def get_run_object(run_dir, sequencer, config):
    if sequencer == "NovaSeqXPlus":
        return NovaSeqXPlusRun(run_dir, config)
    elif sequencer == "NextSeq":
        return NextSeqRun(run_dir, config)
    elif sequencer == "MiSeq":
        return MiSeqRun(run_dir, config)
    elif sequencer == "PromethION":
        return PromethIONRun(run_dir, config)
    elif sequencer == "MinION":
        return MinIONRun(run_dir, config)
    elif sequencer == "Aviti":
        return AVITIRun(run_dir, config)
    else:
        return None


def process_run(run_dir, sequencer, config):
    run = get_run_object(run_dir, sequencer, config)
    if not run:
        logger.warning(f"Unknown sequencer type: {sequencer}. Skipping run: {run_dir}")
        return
    if run.sequencing_ongoing():
        logger.info(
            f"Sequencing is ongoing for {run_dir}. Starting background transfer."
        )
        run.initiate_background_transfer()
        run.upload_stats()
        return
    if not run.transfer_complete():
        run.set_status("Sequenced", True)
        run.upload_stats()
        run.sync_metadata()
        logger.info(f"Sequencing is complete for {run_dir}. Starting final transfer.")
        run.do_final_transfer()
        run.set_status("Transferred", True)
        return
    else:
        logger.info(f"Transfer already completed for {run_dir}. No action needed.")
        return


def get_run_info(run):
    if os.path.isabs(run) and os.path.isdir(run):
        run_dir = run
    elif os.path.isdir(run):
        run_dir = os.path.abspath(run)
    else:
        raise ValueError(f"Provided run path is not a valid directory: {run}")
    sequencer = os.path.basename(
        os.path.dirname(run_dir)
    )  # TODO: fix this for NextSeq and Aviti
    return run_dir, sequencer


def transfer_runs(conf, run=None):
    if run:
        logger.info(f"Transferring specific run: {run}")
        run_dir, sequencer = get_run_info(run)
        process_run(run_dir, sequencer, conf)
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
                        process_run(run_dir_path, sequencer, conf)
                    except Exception as e:
                        logger.error(f"Error processing run {run_dir}: {e}")
                        continue  # Continue with the next run
