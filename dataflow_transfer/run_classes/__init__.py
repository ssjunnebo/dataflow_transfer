# This adds the run classes to the registry. Do not remove.

from dataflow_transfer.run_classes.element_runs import AVITIRun  # noqa: F401, I001
from dataflow_transfer.run_classes.illumina_runs import (
    MiSeqRun,  # noqa: F401
    NextSeqRun,  # noqa: F401
    NovaSeqXPlusRun,  # noqa: F401
)
from dataflow_transfer.run_classes.ont_runs import MinIONRun, PromethIONRun  # noqa: F401

from .registry import RUN_CLASS_REGISTRY  # noqa: F401
