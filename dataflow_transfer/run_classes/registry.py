from types import MappingProxyType


class RunClassRegistry:
    _registry = {}

    @classmethod
    def register(cls, run_cls):
        run_type = getattr(run_cls, "run_type", None)
        if run_type:
            cls._registry[run_type] = run_cls
        return run_cls

    @classmethod
    def get(cls, run_type):
        return cls._registry.get(run_type)

    @classmethod
    def view(cls):
        return MappingProxyType(cls._registry)


# Decorator for registering run classes
def register_run_class(run_cls):
    return RunClassRegistry.register(run_cls)


# Immutable view for external use
RUN_CLASS_REGISTRY = RunClassRegistry.view()
