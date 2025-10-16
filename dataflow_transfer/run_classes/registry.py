RUN_CLASS_REGISTRY = {}


def register_run_class(cls):
    run_type = getattr(cls, "run_type", None)
    if run_type:
        RUN_CLASS_REGISTRY[run_type] = cls
    return cls
