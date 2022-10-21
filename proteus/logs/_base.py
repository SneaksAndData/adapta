"""
 Proteus Logging Interface.
"""
import logging
import ctypes
import os.path
import sys
import tempfile
import threading

from contextlib import contextmanager
from functools import partial

from logging import Handler, StreamHandler
from typing import List, Optional, Dict

from proteus.logs.handlers.datadog_api_handler import DataDogApiHandler

from proteus.logs.models import LogLevel
from proteus.logs.models import ProteusLogMetadata


class MetadataLogger(logging.Logger):
    """
    Wrapper for standard python logger that enriches messages with proteus metadata
    """

    def __init__(self, name: str, level=logging.NOTSET):
        super().__init__(name, level)

    def log_with_metadata(self, log_level, msg, template, tags, diagnostics, stack_info, metadata_fields):
        """
        Log with metadata
        """
        log_metadata = ProteusLogMetadata(
            template=template,
            diagnostics=diagnostics,
            tags=tags,
            fields=metadata_fields,
            exc_info=sys.exc_info())
        self._log(log_level,
                  msg=msg,
                  args=None,
                  extra={ProteusLogMetadata.__name__: log_metadata},
                  stack_info=stack_info)


class DatadogTemplatedLogger(MetadataLogger):
    """
    Inserts metadata to log entry for datadog
    """

    def __init__(self, name: str, level=logging.NOTSET):
        super().__init__(name, level)


class ProteusLogger:
    """
     Proteus Proxy for Python logging library.
    """

    def __init__(
            self,
            fixed_template: Optional[Dict[str, Dict[str, str]]] = None,
            fixed_template_delimiter=', '
    ):
        """
          Creates a new instance of a ProteusLogger

        :param fixed_template: Additional template to append to message templates provided via logging methods.
        :param fixed_template_delimiter: Optional delimiter to use when appending fixed templates.
        """
        self._loggers = {}
        self._default_log_source = None
        self._fixed_template = fixed_template
        self._fixed_template_delimiter = fixed_template_delimiter
        logging.setLoggerClass(MetadataLogger)

    def add_log_source(self, *, log_source_name: str, min_log_level: LogLevel,
                       log_handlers: Optional[List[Handler]] = None,
                       is_default=False) -> 'ProteusLogger':
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

        for log_handler in log_handlers:
            new_logger.addHandler(log_handler)

        self._loggers.setdefault(log_source_name, new_logger)

        if is_default:
            self._default_log_source = log_source_name

        return self

    def __getattr__(self, log_source) -> Optional[logging.Logger]:
        if log_source in self._loggers:
            return self._loggers[log_source]

        return None

    @staticmethod
    def _prepare_message(template: str, **kwargs) -> str:
        return template.format(**kwargs)

    def _get_logger(self, log_source_name: Optional[str] = None) -> DatadogTemplatedLogger:
        """
          Retrieves a logger by log source name, or a default logger is log source name is not provided.

        :param log_source_name: Optional name of a log source.
        :return:
        """
        assert log_source_name or self._default_log_source, 'Argument `log_source` must be provided when no default log source is added. You can add a log source as default by calling `add_log_source(.., is_default=True)`'

        if log_source_name:
            assert log_source_name in self._loggers, f"{log_source_name} does not have an associated logger. Use add_log_source() to associate a logger with this log source."

        return self._loggers[log_source_name or self._default_log_source]

    def _get_fixed_args(self) -> Dict:
        fixed_args = {}
        if self._fixed_template:
            for fixed_value in self._fixed_template.values():
                fixed_args = {**fixed_args, **fixed_value}

        return fixed_args

    def _get_template(self, template) -> str:
        return self._fixed_template_delimiter.join(
            [template, ', '.join(self._fixed_template.keys())]) if self._fixed_template else template

    def info(self,
             template: str,
             tags: Optional[Dict[str, str]] = None,
             log_source_name: Optional[str] = None,
             **kwargs) -> None:
        """
          Sends an INFO level message to configured log sources.

        :param template: Message template.
        :param tags: Optional message tags.
        :param log_source_name: Optional name of a log source, if not using a default.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        logger = self._get_logger(log_source_name)
        msg = self._get_template(template).format(**self._get_fixed_args(), **kwargs)
        logger.log_with_metadata(
            logging.INFO,
            msg=msg,
            template=self._get_template(template),
            tags=tags,
            diagnostics=None,
            stack_info=False,
            metadata_fields=kwargs)

    def warning(self,
                template: str,
                exception: Optional[BaseException] = None,
                tags: Optional[Dict[str, str]] = None,
                log_source_name: Optional[str] = None,
                **kwargs) -> None:
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
        msg = self._get_template(template).format(**self._get_fixed_args(), **kwargs)
        logger.log_with_metadata(logging.WARN,
                                 msg=msg,
                                 tags=tags,
                                 template=template,
                                 diagnostics=None,
                                 stack_info=False,
                                 metadata_fields=kwargs)

    def error(self,
              template: str,
              exception: Optional[BaseException] = None,
              tags: Optional[Dict[str, str]] = None,
              log_source_name: Optional[str] = None,
              **kwargs) -> None:
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
        msg = self._get_template(template).format(**self._get_fixed_args(), **kwargs)
        logger.log_with_metadata(logging.ERROR,
                                 msg=msg,
                                 template=template,
                                 tags=tags,
                                 diagnostics=None,
                                 stack_info=False,
                                 metadata_fields=kwargs)

    def debug(self,
              template: str,
              exception: Optional[BaseException] = None,
              diagnostics: Optional[str] = None,  # pylint: disable=R0913
              tags: Optional[Dict[str, str]] = None,
              log_source_name: Optional[str] = None, **kwargs) -> None:
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
        msg = self._get_template(template).format(**self._get_fixed_args(), **kwargs)
        logger.log_with_metadata(logging.DEBUG,
                                 msg=msg,
                                 template=template,
                                 tags=tags,
                                 diagnostics=diagnostics,
                                 stack_info=True,
                                 metadata_fields=kwargs)

    @contextmanager
    def redirect(self,
                 tags: Optional[Dict[str, str]] = None,
                 log_source_name: Optional[str] = None,
                 log_level=LogLevel.INFO):
        """
         Redirects stdout to a temporary file and dumps its contents as INFO messages
         once the wrapped code block finishes execution. Stdout is restored after the block completes execution.
         Note that timestamps appended by the logger will not correlate with the actual timestamp
         of the reported message, if one is present in the output. This method works for the whole process,
         including external libraries (C/C++ etc). Example usage:

         with proteus_logger.redirect():
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
                self._get_template('>> Output redirection not supported on this platform: {platform} <<'),
                platform=sys.platform,
                tags=tags,
                log_source_name=log_source_name
            )
            try:
                yield None
                return
            finally:
                pass

        libc = ctypes.CDLL(None)
        saved_stdout = libc.dup(1)
        tmp_file = os.path.join(tempfile.gettempdir(), tempfile.mktemp()).encode('utf-8')
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
            log_header = partial(self._prepare_message,
                                 template=self._get_template('>> Redirected output {state} <<'),
                                 tags=tags,
                                 **self._get_fixed_args(),
                                 diagnostics=None)
            log_message = partial(self._prepare_message,
                                  template=self._get_template('Redirected output: {message}'),
                                  tags=tags,
                                  **self._get_fixed_args(),
                                  diagnostics=None)
            log_method = {
                LogLevel.INFO: logger.info,
                LogLevel.WARN: logger.warning,
                LogLevel.ERROR: logger.error,
                LogLevel.DEBUG: logger.debug
            }

            log_method[log_level](log_header(state='BEGIN'))

            with open(tmp_file, encoding='utf-8') as output:
                for line in output.readlines():
                    log_method[log_level](log_message(message=line))

            log_method[log_level](log_header(state='END'))
