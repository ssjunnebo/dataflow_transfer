# Read a list of directories from a config file
# For each directory: 
    # locate all subdirectories and filter out the ones that are run dirs
# For each run dir:
    # check the status of the run based on the current state of the run dir
    # If the run is ongoing (i.e. final file does not exist):
    # update the "Sequncing started" status to True
    # start an rsync process in the background to transfer the data to Miarka (with run-one to avoid multiple concurrent transfers)
    # update the "transfer started" status to True
# If the run is complete (i.e. final file exists):
    # update the "Sequenicing finished" satus to True
    # initiate a final transfer
    # update the "transfer finished" status to True

