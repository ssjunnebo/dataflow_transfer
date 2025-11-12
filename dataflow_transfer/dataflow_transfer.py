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
    ## Case 1: Transfer already completed. Do nothing. Removing the final transfer indicator lets the run retry transfer the next iteration.
    if run.get_status("transferred_to_hpc") and run.final_sync_successful():
        logger.info(f"Transfer already completed for {run_dir}. No action needed.")
        return
    ## Case 2: Sequencing ongoing. Start background transfer if not already running.
    if run.sequencing_ongoing():
        run.update_statusdb(status="sequencing_started")
        logger.info(
            f"Sequencing is ongoing for {run_dir}. Starting background transfer."
        )
        run.initiate_background_transfer()
        return
    ## Case 3: Sequencing finished but transfer not complete. Sync metadata to ngi-nas-ns and start final transfer.
    if not run.transfer_complete():
        if run.get_status("sequencing_finished"):
            logger.info(
                f"Run {run_dir} is already marked as sequenced, but transfer not complete. Will attempt final transfer again."
            )
        run.update_statusdb(status="sequencing_finished")
        if run.metadata_synced():
            logger.info(f"Metadata already synced for {run_dir}.")
            run.update_statusdb(status="metadata_synced")
        else:
            run.sync_metadata()
        logger.info(f"Sequencing is complete for {run_dir}. Starting final transfer.")
        run.do_final_transfer()
        return
    ## Case 4: Final transfer completed successfully. Update statusdb.
    if run.final_sync_successful():
        logger.info(f"Final transfer completed successfully for {run_dir}.")
        run.update_statusdb(status="final_transfer_completed")
        return
    ## Case 5: Final transfer attempted but failed. Log error and raise exception.
    else:
        logger.error(f"Final transfer failed for {run_dir}. Please check rsync logs.")
        raise RuntimeError(
            f"Final transfer failed for {run_dir}."
        )  # TODO: we could retry? e.g log nr of retries in the DB and retry N times before sending aout an email warning?


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
        sequencers = conf.get("sequencers", {})
        for sequencer in sequencers.keys():
            logger.info(f"Processing data from: {sequencer}")
            sequencing_dir = sequencer.get("sequencing_path")
            for run_dir in os.listdir(sequencing_dir):
                run_dir_path = os.path.join(sequencing_dir, run_dir)
                if os.path.isdir(run_dir_path):
                    logger.info(f"Processing directory: {run_dir_path}")
                    try:
                        process_run(run_dir_path, sequencer, conf)
                    except Exception as e:
                        logger.error(f"Error processing run {run_dir}: {e}")
                        continue  # Continue with the next run
