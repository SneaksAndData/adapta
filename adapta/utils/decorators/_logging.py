""" Module for common decorator methods. """

#  Copyright (c) 2023. ECCO Sneaks & Data
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from functools import wraps

from adapta.logs import SemanticLogger
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
