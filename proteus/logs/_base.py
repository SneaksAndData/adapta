"""
 Proteus Logging Interface.
"""
import json
import logging
import ctypes
import os.path
import sys
import tempfile

from contextlib import contextmanager

from logging import Handler, StreamHandler
from typing import List, Optional

import json_log_formatter

from proteus.logs.models import LogLevel


class ProteusLogger:
    """
     Proteus Proxy for Python logging library.
    """

    def __init__(self):
        """
          Creates a new instance of a ProteusLogger
        """
        self._loggers = {}
        self._default_log_source = None

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
            log_handler.setFormatter(json_log_formatter.JSONFormatter())
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
    def _prepare_message(template: str, tags: Optional[str] = None, diagnostics: Optional[str] = None,
                         **kwargs) -> str:
        """
         Returns message dictionary to be used by handler formatter.
         :param exclude_fields: Fields to exclude from export

        :return:
        """
        base_object = {
            'template': template,
            'text': template.format(**kwargs)
        }
        if tags:
            base_object.setdefault('tags', tags)
        if diagnostics:
            base_object.setdefault('diagnostics', diagnostics)

        base_object.update(**kwargs)

        return json.dumps(base_object)

    def _get_logger(self, log_source_name: Optional[str] = None) -> logging.Logger:
        """
          Retrieves a logger by log source name, or a default logger is log source name is not provided.

        :param log_source_name: Optional name of a log source.
        :return:
        """
        assert log_source_name or self._default_log_source, 'Argument `log_source` must be provided when no default log source is added. You can add a log source as default by calling `add_log_source(.., is_default=True)`'

        if log_source_name:
            assert log_source_name in self._loggers, f"{log_source_name} does not have an associated logger. Use add_log_source() to associate a logger with this log source."

        return self._loggers[log_source_name or self._default_log_source]

    def info(self, template: str, tags: Optional[str] = None, log_source_name: Optional[str] = None, **kwargs) -> None:
        """
          Sends an INFO level message to configured log sources.

        :param template: Message template.
        :param tags: Optional message tags.
        :param log_source_name: Optional name of a log source, if not using a default.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        logger = self._get_logger(log_source_name)
        logger.info(msg=self._prepare_message(template=template, tags=tags, diagnostics=None, **kwargs))

    def warning(self, template: str, exception: BaseException, tags: Optional[str] = None,
                log_source_name: Optional[str] = None, **kwargs) -> None:
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
        logger.warning(msg=self._prepare_message(template=template, tags=tags, diagnostics=None, **kwargs),
                       exc_info=exception, stack_info=True)

    def error(self, template: str, exception: BaseException, tags: Optional[str] = None,
              log_source_name: Optional[str] = None, **kwargs) -> None:
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
        logger.error(msg=self._prepare_message(template=template, tags=tags, diagnostics=None, **kwargs),
                     exc_info=exception, stack_info=True)

    def debug(self, template: str, exception: BaseException, diagnostics: Optional[str] = None,  # pylint: disable=R0913
              tags: Optional[str] = None,
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
        logger.error(msg=self._prepare_message(template=template, tags=tags, diagnostics=diagnostics, **kwargs),
                     exc_info=exception, stack_info=True)

    @contextmanager
    def redirect(self,
                 tags: Optional[str] = None,
                 log_source_name: Optional[str] = None):
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
        :return:
        """

        if sys.platform == "win32":
            self.info(
                '>> Output redirection not supported on this platform: {platform} <<',
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
            libc.dup2(saved_stdout, 1)
            os.chmod(tmp_file, 420)
            self.info('>> Redirected output {state} <<', state='BEGIN', tags=tags, log_source_name=log_source_name)
            with open(tmp_file, encoding='utf-8') as output:
                for line in output.readlines():
                    self.info('Redirected output: {msg}', msg=line, tags=tags, log_source_name=log_source_name)
            self.info('>> Redirected output {state} <<', state='END', tags=tags, log_source_name=log_source_name)
