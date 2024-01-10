"""
 Adapta Logging Interface.
"""
#  Copyright (c) 2023-2024. ECCO Sneaks & Data
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

import logging
import ctypes
import os.path
import sys
import tempfile

from contextlib import contextmanager

from logging import Handler, StreamHandler
from typing import List, Optional, Dict, final

from adapta.logs._internal_logger import _InternalLogger
from adapta.logs.models import LogLevel
from adapta.logs._internal import MetadataLogger, from_log_level


@final
class SemanticLogger(_InternalLogger):
    """
    Proxy for a collection of python loggers that use the same formatting interface.
    """

    def __init__(self, fixed_template: Optional[Dict[str, Dict[str, str]]] = None, fixed_template_delimiter=", "):
        """
          Creates a new instance of a SemanticLogger

        :param fixed_template: Additional template to append to message templates provided via logging methods.
        :param fixed_template_delimiter: Optional delimiter to use when appending fixed templates.
        """
        super().__init__(fixed_template, fixed_template_delimiter)
        self._loggers: Dict[str, logging.Logger] = {}
        self._default_log_source = None
        self._fixed_template = fixed_template
        self._fixed_template_delimiter = fixed_template_delimiter
        logging.setLoggerClass(MetadataLogger)

    def add_log_source(
        self,
        *,
        log_source_name: str,
        min_log_level: LogLevel,
        log_handlers: Optional[List[Handler]] = None,
        is_default=False,
    ) -> "SemanticLogger":
        """
          Adds a new log source.

        :param log_source_name: Name of a source.
        :param min_log_level: Minimal log level for this source.
        :param log_handlers: Attached log handlers. StreamHandler is used if not provided.
        :param is_default: Whether this log source should be used in a `log` method when no log source is explicitly provided.
        :return:
        """
        new_logger = logging.getLogger(log_source_name)
        new_logger.setLevel(min_log_level.value)

        if not log_handlers or len(log_handlers) == 0:
            log_handlers = [StreamHandler()]

        self._loggers.setdefault(log_source_name, new_logger)

        if is_default:
            self._default_log_source = log_source_name

        for log_handler in log_handlers:
            new_logger.addHandler(log_handler)

        return self

    def __getattr__(self, log_source) -> Optional[logging.Logger]:
        if log_source in self._loggers:
            return self._loggers[log_source]

        return None

    def _get_logger(self, log_source_name: Optional[str] = None) -> MetadataLogger:
        """
          Retrieves a logger by log source name, or a default logger is log source name is not provided.

        :param log_source_name: Optional name of a log source.
        :return:
        """
        assert (
            log_source_name or self._default_log_source
        ), "Argument `log_source` must be provided when no default log source is added. You can add a log source as default by calling `add_log_source(.., is_default=True)`"

        if log_source_name:
            assert (
                log_source_name in self._loggers
            ), f"{log_source_name} does not have an associated logger. Use add_log_source() to associate a logger with this log source."

        return self._loggers[log_source_name or self._default_log_source]

    def info(
        self,
        template: str,
        tags: Optional[Dict[str, str]] = None,
        log_source_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
          Sends an INFO level message to configured log sources.

        :param template: Message template.
        :param tags: Optional message tags.
        :param log_source_name: Optional name of a log source, if not using a default.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        logger = self._get_logger(log_source_name)
        self._meta_info(template=template, logger=logger, tags=tags, **kwargs)

    def warning(
        self,
        template: str,
        exception: Optional[BaseException] = None,
        tags: Optional[Dict[str, str]] = None,
        log_source_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
          Sends a WARNING level message to configured log sources.

        :param template: Message template.
        :param exception: Exception associated with this warning.
        :param tags: Optional message tags.
        :param log_source_name: Optional name of a log source, if not using a default.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        logger = self._get_logger(log_source_name)
        self._meta_warning(template=template, logger=logger, tags=tags, exception=exception, **kwargs)

    def error(
        self,
        template: str,
        exception: Optional[BaseException] = None,
        tags: Optional[Dict[str, str]] = None,
        log_source_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
          Sends an ERROR level message to configured log sources.

        :param template: Message template.
        :param exception: Exception associated with this error.
        :param tags: Optional message tags.
        :param log_source_name: Optional name of a log source, if not using a default.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        logger = self._get_logger(log_source_name)
        self._meta_error(template=template, logger=logger, tags=tags, exception=exception, **kwargs)

    def debug(
        self,
        template: str,
        exception: Optional[BaseException] = None,
        diagnostics: Optional[str] = None,  # pylint: disable=R0913
        tags: Optional[Dict[str, str]] = None,
        log_source_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
          Sends a DEBUG level message to configured log sources.

        :param template: Message template.
        :param exception: Exception associated with this error.
        :param diagnostics: Optional additional diagnostics info.
        :param tags: Optional message tags.
        :param log_source_name: Optional name of a log source, if not using a default.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        logger = self._get_logger(log_source_name)
        self._meta_debug(
            template=template, logger=logger, tags=tags, exception=exception, diagnostics=diagnostics, **kwargs
        )

    def _print_redirect_state(self, logger, log_level, state, tags):
        template = self._get_template(">> Redirected output {state} <<")
        msg = template.format(**self._get_fixed_args(), state=state)
        logger.log_with_metadata(
            from_log_level(log_level),
            msg=msg,
            tags=tags,
            diagnostics=None,
            stack_info=None,
            exception=None,
            metadata_fields=self._get_metadata_fields({}),
            template=template,
        )

    def _print_redirect_message(self, logger, log_level, message, tags):
        template = self._get_template("Redirected output: {message}")
        msg = template.format(**self._get_fixed_args(), message=message)
        logger.log_with_metadata(
            from_log_level(log_level),
            msg=msg,
            tags=tags,
            diagnostics=None,
            stack_info=None,
            exception=None,
            metadata_fields=self._get_metadata_fields({}),
            template=template,
        )

    @contextmanager
    def redirect(
        self,
        tags: Optional[Dict[str, str]] = None,
        log_source_name: Optional[str] = None,
        log_level=LogLevel.INFO,
    ):
        """
         Redirects stdout to a temporary file and dumps its contents as INFO messages
         once the wrapped code block finishes execution. Stdout is restored after the block completes execution.
         Note that timestamps appended by the logger will not correlate with the actual timestamp
         of the reported message, if one is present in the output. This method works for the whole process,
         including external libraries (C/C++ etc). Example usage:

         with composite_logger.redirect():
             # from here, output will be redirected and collected separately
             call_my_function()
             call_my_other_function()

         # once `with` block ends, vanilla logging behaviour is restored.
         call_my_other_other_function()

         NB: This method only works on Linux. Invoking it on Windows will have no effect.

        :param tags: Optional message tags.
        :param log_source_name: Optional name of a log source, if not using a default.
        :param log_level: Optional logging level for a redirected log source. Defaults to INFO.
        :return:
        """

        if sys.platform == "win32":
            self.info(
                self._get_template(">> Output redirection not supported on this platform: {platform} <<"),
                platform=sys.platform,
                tags=tags,
                log_source_name=log_source_name,
            )
            try:
                yield None
                return
            finally:
                pass

        libc = ctypes.CDLL(None)
        saved_stdout = libc.dup(1)
        tmp_file = os.path.join(tempfile.gettempdir(), tempfile.mktemp()).encode("utf-8")
        try:
            redirected_fd = libc.creat(tmp_file)
            libc.dup2(redirected_fd, 1)
            libc.close(redirected_fd)
            yield None
        finally:
            sys.stdout.flush()
            libc.dup2(saved_stdout, 1)
            os.chmod(tmp_file, 420)

            logger = self._get_logger(log_source_name)

            self._print_redirect_state(logger, log_level, "BEGIN", tags)
            with open(tmp_file, encoding="utf-8") as output:
                for line in output.readlines():
                    self._print_redirect_message(logger, log_level, line, tags)
            self._print_redirect_state(logger, log_level, "END", tags)
