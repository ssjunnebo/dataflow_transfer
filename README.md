# dataflow_transfer
A tool for transferring sequencing data from NAS to HPC

## Status Files
The logic of the script relies on the following status files
 - run.final_file
    - The final file written by each sequencing machine. Used to indicate when the sequencing has completed.
 - final_rsync_exitcode
    - Used to indicate when the final rsync is done, so that the final rsync can be run in the background. This is especially useful for restarts after long pauses of the cronjob.
"""