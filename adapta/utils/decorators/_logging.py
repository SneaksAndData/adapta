""" Module for common decorator methods. """
from functools import wraps

from adapta.logs import SemanticLogger
from adapta.metrics import MetricsProvider
from adapta.utils import operation_time


def run_time_metrics(method_type: str, pass_logger: bool = False, pass_metrics_provider: bool = False):
    """
    Decorator which can be used to log automatically runtime of a method.

    :param method_type: Description of method type that can be used to capture logging in metric sink.
    :param pass_logger: Boolean flag that denotes if logger should be passed onwards to decorated method.
    :param pass_metrics_provider: Boolean flag that denotes if metric_provider should be pasesd to decorated method.
    """

    def outer_runtime_decorator(func):
        @wraps(func)
        def inner_runtime_decorator(*args, **kwargs):
            logger: SemanticLogger = kwargs.get("logger", None) if pass_logger else kwargs.pop("logger", None)
            metrics_provider: MetricsProvider = (
                kwargs.get("metrics_provider", None) if pass_metrics_provider else kwargs.pop("metrics_provider", None)
            )
            extra_entities = kwargs.pop("extra_metric_entities", {})

            if logger is not None:
                logger.info(
                    "running {run_type} on method {method_name}", run_type=method_type, method_name=func.__name__
                )

            with operation_time() as ot:
                result = func(*args, **kwargs)

            if metrics_provider is not None:
                metrics_provider.gauge(
                    metric_name=f"{method_type}",
                    metric_value=round(ot.elapsed / 1e9, 2),
                    tags={"entity_name": str(func.__name__)} | extra_entities,
                )
            if logger is not None:
                logger.debug(
                    "{method_name} finished in {elapsed:.2f}s seconds",
                    method_name=func.__name__,
                    elapsed=(ot.elapsed / 1e9),
                )
            return result

        return inner_runtime_decorator

    return outer_runtime_decorator
