""" Module for common decorator methods. """
from functools import wraps
from typing import Optional, Dict, Any

from adapta.logs import SemanticLogger
from adapta.logs._async_logger import _AsyncLogger
from adapta.logs.models import LogLevel
from adapta.metrics import MetricsProvider
from adapta.utils._common import operation_time


def run_time_metrics(metric_name: str, tag_function_name: bool = False, log_level: LogLevel = LogLevel.DEBUG):
    """
    Decorator that records runtime of decorated method to logging source and metrics_provider.

    :param metric_name: Description of method type that can be used to capture logging in metric sink.
    :param tag_function_name: Boolean flag to indicate if function name should be added as tag to metric. Default False.
    :param log_level: Defines on which level the logger should send reports. Default debug.
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
            log_method = getattr(logger, log_level.value.lower())
            if log_method is None:
                raise AttributeError(f"Logger {logger.__class__} does not send logs on level: {log_level}")

            metric_tags = kwargs.pop("metric_tags", {}) | (
                {"function_name": str(func.__name__)} if tag_function_name else {}
            )

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


def run_time_metrics_async(
    metric_name: str,
    tag_function_name: bool = False,
    on_finish_message_template="Method {method_name} finished in {elapsed:.2f}s seconds",
    template_args: Optional[dict[str, Any]] = None,
):
    """
    Decorator that records runtime of decorated method to logging source and metrics_provider.

    :param metric_name: Description of method type that can be used to capture logging in metric sink.
    :param tag_function_name: Boolean flag to indicate if function name should be added as tag to metric. Default False.
    :param on_finish_message_template: Message to log on INFO level when run is finished. Must contain `{elapsed}` in order to log the time correctly.
    :param template_args: Template arguments, without {elapsed}.
    """

    def outer(func):
        @wraps(func)
        async def inner_runtime_decorator(
            metrics_provider: MetricsProvider,
            logger: _AsyncLogger,
            metric_tags: Optional[Dict[str, str]] = None,
            **kwargs,
        ):
            metric_tags = (metric_tags or {}) | ({"function_name": str(func.__name__)} if tag_function_name else {})

            logger.debug("Running {method_name}", method_name=func.__name__)

            with operation_time() as ot:
                result = await func(metrics_provider=metrics_provider, logger=logger, metric_tags=metric_tags, **kwargs)

            metrics_provider.gauge(
                metric_name=metric_name,
                metric_value=round(ot.elapsed / 1e9, 2),
                tags=metric_tags,
            )
            nonlocal template_args
            template_args = (template_args or {"method_name": func.__name__}) | {"elapsed": ot.elapsed / 1e9}

            logger.info(
                on_finish_message_template,
                **template_args,
            )
            return result

        return inner_runtime_decorator

    return outer
