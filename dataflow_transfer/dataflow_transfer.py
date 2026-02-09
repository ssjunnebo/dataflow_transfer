import logging
import time

from dataflow_transfer.run_classes.registry import RUN_CLASS_REGISTRY
from dataflow_transfer.utils.filesystem import find_runs, get_run_dir

logger = logging.getLogger(__name__)


def get_run_object(run_dir, sequencer, config):
    run_class = RUN_CLASS_REGISTRY.get(sequencer)
    if run_class:
        return run_class(run_dir, config)
    else:
        raise ValueError(
            f"Unknown sequencer type: {sequencer}. Skipping run: {run_dir}"
        )


def process_run(run_dir, sequencer, config):
    run = get_run_object(run_dir, sequencer, config)
    run.confirm_run_type()

    ## Transfer already completed. Do nothing.
    if run.final_sync_successful and run.has_status("transferred_to_hpc"):
        # Check transfer success both in statusdb and via exit code file
        # To restart transfer, remove the exit code file
        logger.info(f"Transfer of {run_dir} is finished. No action needed.")
        return

    ## Sequencing ongoing. Start background transfer if not already running.
    if run.sequencing_ongoing:
        run.update_statusdb(status="sequencing_started")
        run.start_transfer(final=False)
        return

    ## Sequencing finished. Copy metadata in the background if not already done.
    if run.has_status("sequencing_finished"):
        if not run.metadata_synced:
            run.sync_metadata()

    ## Sequencing finished but transfer not complete. Start final transfer.
    if not run.final_sync_successful:
        if run.has_status("sequencing_finished"):
            logger.info(
                f"Run {run_dir} is already marked as sequenced, but transfer not complete. "
                "Will attempt final transfer again."
            )
        run.update_statusdb(status="sequencing_finished")
        run.start_transfer(final=True)
        return

    ## Final transfer completed successfully. Update statusdb.
    if run.final_sync_successful:
        logger.info(f"Final transfer completed successfully for {run_dir}.")
        run.update_statusdb(status="transferred_to_hpc")
        return

    ## Unknown status of run. Log error and raise exception.
    else:
        logger.error(f"Unknown status for {run_dir}. Please check logs.")
        raise RuntimeError(f"Unknown status for {run_dir}.")


def transfer_runs(conf, run=None, sequencer=None):
    start_time = time.time()
    if run:
        logger.info(f"Transferring specific run: {run}")
        run_dir = get_run_dir(run)
        process_run(run_dir, sequencer, conf)
        end_time = time.time()
    else:
        logger.info("Transferring all runs as per configuration")
        sequencers = conf.get("sequencers", {})
        for sequencer in sequencers.keys():
            logger.info(f"Processing data from: {sequencer}")
            sequencing_dir = sequencers.get(sequencer).get("sequencing_path")
            for run_dir in find_runs(
                sequencing_dir, sequencers.get(sequencer).get("ignore_folders", [])
            ):
                logger.info(f"Processing directory: {run_dir}")
                try:
                    process_run(run_dir, sequencer, conf)
                except Exception as e:
                    logger.error(f"Error processing run {run_dir}: {e}")
                    continue  # Continue with the next run
        end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"Data transfer process completed in {elapsed_time:.2f} seconds.")
