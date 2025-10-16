from .registry import RUN_CLASS_REGISTRY


from dataflow_transfer.run_classes.illumina_runs import (
    NovaSeqXPlusRun,
    NextSeqRun,
    MiSeqRun,
)
from dataflow_transfer.run_classes.ont_runs import PromethIONRun, MinIONRun
from dataflow_transfer.run_classes.element_runs import AVITIRun
