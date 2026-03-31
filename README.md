# Dataflow Transfer

A Python application for automating the transfer of sequencing data.

## Overview

Dataflow Transfer monitors sequencing run directories and orchestrates the transfer of sequencing data via rsync. It supports multiple sequencer types (Illumina, Oxford Nanopore and Element), tracks transfer progress in a CouchDB-based status database, and handles both continuous and final transfer phases during and after sequencing completion.

## Supported Sequencers

- **Illumina**: NextSeq, MiSeqi100, NovaSeqXPlus (WIP), MiSeq (WIP)
- **Oxford Nanopore (ONT)**: PromethION (WIP), MinION (WIP)
- **Element**: AVITI (WIP)

## Installation

### Requirements

- Python 3.11+
- Dependencies listed in [requirements.txt](requirements.txt):
  - PyYAML
  - click
  - ibmcloudant
- [run-one](https://launchpad.net/ubuntu/+source/run-one)

### Setup suggestions

1. Clone the repository:

```bash
git clone <repository-url>
cd dataflow_transfer
```

2. Install the package:

```bash
pip install -e .
```

Or with development dependencies:

```bash
pip install -e ".[dev]"
```

## Usage

### Command Line Interface

```bash
dataflow_transfer [OPTIONS]
```

#### Options

- `-c, --config-file PATH`: Path to configuration YAML file. Defaults to `~/.df_transfer/df_transfer.yaml`. Can also be set via `TRANSFER_CONFIG` environment variable.
- `-r, --run RUN_ID`: Transfer a specific run (e.g., `20250528_LH00217_0219_A22TT52LT4`). Requires `--sequencer`.
- `-s, --sequencer TYPE`: Sequencer type of the run (e.g., `NovaSeqXPlus`, `MiSeq`, `AVITI`). Required with `--run`.
- `--version`: Show version and exit.

#### Examples

```bash
# Transfer all runs (uses configuration for sequencing directories)
dataflow_transfer

# Transfer a specific run
dataflow_transfer --run 20250528_LH00217_0219_A22TT52LT4 --sequencer NovaSeqXPlus

# Use a custom config file
dataflow_transfer --config-file /path/to/config.yaml
```

## Configuration

Create a YAML configuration file with the following structure:

```yaml
log:
  file: /path/to/dataflow_transfer.log

run_one_path: /usr/bin/run-one

transfer_details:
  user: username
  host: remote.host.com

statusdb:
  username: couchdb_user
  password: couchdb_password
  url: couchdb.host.com
  database: flowcell_status

sequencers:
  NovaSeqXPlus:
    sequencing_path: /sequencing/NovaSeqXPlus
    remote_destination: /Illumina/NovaSeqXPlus
    metadata_archive: /path/to/metadata/archive/NovaSeqXPlus_data
    ignore_folders:
      - nosync
    remote_rsync_options:
      - --chmod=Dg+s,g+rw
    metadata_rsync_options:
      - "--include=InterOp"
  # ... additional sequencer configurations
```

## How It Works

1. **Discovery**: Scans configured sequencing directories for run folders
2. **Validation**: Confirms run ID matches expected format for the sequencer type
3. **Transfer Phases**:
   - **Sequencing Phase**: Starts continuous background rsync transfer while sequencing is ongoing (when the final sequencing file doesn't exist). Uploads status to database.
   - **Final Transfer**: After sequencing completes (final sequencing file appears), syncs specified metadata files to archive location, initiates final rsync transfer and captures exit codes.
   - **Completion**: Updates database when transfer was successful.

### Status Tracking

Run status is tracked in CouchDB with events including:

| Status                   | Meaning                             | Occurs when                                                                                                                                                      |
| ------------------------ | ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sequencing_started`     | Sequencing is ongoing               | A run folder exists but the final sequencing file has not been created yet                                                                                       |
| `transfer_started`       | Intermediate transfer was initiated | Sequencing is ongoing and an rsync has been started                                                                                                              |
| `sequencing_finished`    | Sequencing has completed            | A run folder exists and the final sequencing file has been created                                                                                               |
| `final_transfer_started` | Final sync has started              | A run folder exists and the final sequencing file has been created, but the final rsync exit code file has not yet been created or contains a non-zero exit code |
| `transferred_to_hpc`     | Transfer completed successfully     | A run folder exists, the final sequencing file has been created, and the final rsync exit code file contains a 0 exit code                                       |

### Flow chart

![Flow chart for Dataflow transfer](/docs/Dataflow_Transfer_flowchart.svg)

### Key Components

- **[`dataflow_transfer/run_classes/`](dataflow_transfer/run_classes/)**: Sequencer-specific run classes
- **[`dataflow_transfer/utils/filesystem.py`](dataflow_transfer/utils/filesystem.py)**: File operations and rsync handling
- **[`dataflow_transfer/utils/statusdb.py`](dataflow_transfer/utils/statusdb.py)**: CouchDB session management with retry logic
- **[`dataflow_transfer/cli.py`](dataflow_transfer/cli.py)**: Command-line interface

## Assumptions

- Run directories are named according to sequencer-specific ID formats (defined in run classes)
- Final completion is indicated by the presence of a sequencer-specific final file (e.g., `CopyComplete.txt` for Illumina)
- Remote storage is accessible via rsync over SSH
- CouchDB is accessible and the database specified in the config exists and has a ddoc called `events` with a view called `current_status_per_runfolder` that emits a dictionary of all the statuses and their current state (true/false)
- CouchDB is accessible and the database `gs_configs` contains a document called `regex_patterns` containing the regexes used to identify different run types.
- The flowcell ID is set to correspond to the ID that is scanned with a barcode scanner during sequencing setup in the lab

### Status Files

The logic of the script relies on the following status files:

- `run.final_file` - The final file written by each sequencing machine. Used to indicate when the sequencing has completed.
- `.final_rsync_exitcode` - Used to indicate when the final rsync is done, so that the final rsync can be run in the background. This is especially useful for restarts after long pauses of the cronjob.
- `.metadata_rsync_exitcode` - Used to indicate when rsync of metadata to the metadata archive is done, so that the rsync can be run in the background. This is useful when there are I/O issue with the disks.

## Development

### Running Tests

```bash
pytest
```

With coverage:

```bash
pytest --cov --cov-branch
```

### Code Quality

Run linting and formatting checks:

```bash
ruff check .
ruff format --check .
```

### Project Structure

```
dataflow_transfer/
├── cli.py                 # Command-line interface
├── dataflow_transfer.py   # Main transfer orchestration
├── log/                   # Logging utilities
├── run_classes/           # Sequencer-specific run classes
├── utils/                 # Utility modules (filesystem, statusdb)
└── tests/                 # Unit tests
```

### Adding a new sequencer

To add support for a new sequencer, add the following to dataflow_transfer:

1. Add a new class for the sequencer in one of the run classes files below. Make sure it inherits from the manufacturer class (IlluminaRun, ElementRun, ONTRun)
   - `dataflow_transfer/run_classes/illumina_runs.py`
   - `dataflow_transfer/run_classes/element_runs.py`
   - `dataflow_transfer/run_classes/ont_runs.py`
2. Import the new class in `dataflow_transfer/run_classes/__init__.py`
3. Add a test fixture for the new run in `dataflow_transfer/tests/test_run_classes.py` and include it in the relevant tests
4. Add a section for the sequencer in the config file
5. Add a regular expression matching the run folder name of the sequencer in the CouchDB database `gs_configs`, in the document called `regex_patterns`.
