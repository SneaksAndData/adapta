""" Module for common decorator methods. """
from functools import wraps

from adapta.logs import SemanticLogger
from adapta.logs.models import LogLevel
from adapta.metrics import MetricsProvider
from adapta.utils._common import operation_time


def run_time_metrics(metric_name: str, tag_function_name: bool = False, reporting_level: LogLevel = LogLevel.DEBUG):
    """
    Decorator that records runtime of decorated method to logging source and metrics_provider.

    :param metric_name: Description of method type that can be used to capture logging in metric sink.
    :param tag_function_name: Boolean flag to indicate if function name should be added as tag to metric. Default False.
    :param reporting_level: Defines on which level the logger should send reports. Default debug.
    """

    def outer_runtime_decorator(func):
        @wraps(func)
        def inner_runtime_decorator(*args, **kwargs):
            logger: SemanticLogger = kwargs.get("logger", None)
            metrics_provider: MetricsProvider = kwargs.get("metrics_provider", None)
            if logger is None or metrics_provider is None:
                raise AttributeError(
                    f"Decorated wrapped function: {func.__name__} missing required objects logger, metrics_provider"
                )
            log_method = getattr(logger, reporting_level.value.lower())
            if log_method is None:
                raise AttributeError(f"Logger {logger.__class__} does not send logs on level: {reporting_level}")

            metric_tags = kwargs.pop("metric_tags", {})
            metric_tags |= {"function_name": str(func.__name__)} if tag_function_name else {}

            log_method("running {run_type} on method {method_name}", run_type=metric_name, method_name=func.__name__)
            with operation_time() as ot:
                result = func(*args, **kwargs)
            metrics_provider.gauge(
                metric_name=metric_name,
                metric_value=round(ot.elapsed / 1e9, 2),
                tags=metric_tags,
            )
            log_method(
                "{method_name} finished in {elapsed:.2f}s seconds",
                method_name=func.__name__,
                elapsed=(ot.elapsed / 1e9),
            )
            return result

        return inner_runtime_decorator

    return outer_runtime_decorator
