import logging
import os
from dataflow_transfer.run_classes.registry import RUN_CLASS_REGISTRY

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
    if run.get_status("final_transfer_completed"):
        logger.info(f"Transfer already completed for {run_dir}. No action needed.")
        return
    if run.sequencing_ongoing():
        run.update_statusdb(status="sequencing_started")
        logger.info(
            f"Sequencing is ongoing for {run_dir}. Starting background transfer."
        )
        run.initiate_background_transfer()
        run.update_statusdb(status="transfer_started")
        return
    if not run.transfer_complete():
        if run.get_status("sequencing_completed"):
            logger.info(
                f"Run {run_dir} is already marked as sequenced, but transfer not complete. Will attempt final transfer again."
            )
        run.update_statusdb(status="sequencing_completed")
        run.sync_metadata()
        logger.info(f"Sequencing is complete for {run_dir}. Starting final transfer.")
        run.do_final_transfer()
        run.update_statusdb(status="final_transfer_started")
        return
    if run.final_sync_successful():
        logger.info(f"Final transfer completed successfully for {run_dir}.")
        run.update_statusdb(status="final_transfer_completed")
        return
    else:
        logger.error(f"Final transfer failed for {run_dir}. Please check rsync logs.")
        raise RuntimeError(
            f"Final transfer failed for {run_dir}."
        )  # TODO: we could retry? e.g log nr of retries in the DB and retry N times before sending aout an email warning?
        # Removing the final transfer indicator should let the run retry next iteration


def get_run_dir(run):
    if os.path.isabs(run) and os.path.isdir(run):
        return run
    elif os.path.isdir(run):
        return os.path.abspath(run)
    else:
        raise ValueError(f"Provided run path is not a valid directory: {run}")


def transfer_runs(conf, run=None, sequencer=None):
    if run:
        logger.info(f"Transferring specific run: {run}")
        run_dir = get_run_dir(run)
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
