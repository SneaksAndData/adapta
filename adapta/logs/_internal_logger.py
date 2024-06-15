"""
 Shared functionality for the MetadataLogger enricher implementations.
"""
import ctypes

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
import os
import sys
import tempfile
import uuid
from abc import ABC
from contextlib import contextmanager
from threading import Thread
from time import sleep
from typing import Optional, Dict, Any

from adapta.logs._internal import MetadataLogger, from_log_level
from adapta.logs._logger_interface import LoggerInterface
from adapta.logs.models import LogLevel


class _InternalLogger(LoggerInterface, ABC):
    def __init__(
        self,
        fixed_template: Optional[Dict[str, Dict[str, str]]] = None,
        fixed_template_delimiter=", ",
    ):
        """
          Creates a new instance of a InternalLogger

        :param fixed_template: Additional template to append to message templates provided via logging methods.
        :param fixed_template_delimiter: Optional delimiter to use when appending fixed templates.
        """
        self._fixed_template = fixed_template
        self._fixed_template_delimiter = fixed_template_delimiter

    def _get_metadata_fields(self, kwargs):
        fields = kwargs
        fields.update(self._get_fixed_args())
        return fields

    def _get_fixed_args(self) -> Dict:
        fixed_args = {}
        if self._fixed_template:
            for fixed_value in self._fixed_template.values():
                fixed_args = {**fixed_args, **fixed_value}

        return fixed_args

    def _get_template(self, template) -> str:
        return (
            self._fixed_template_delimiter.join([template, ", ".join(self._fixed_template.keys())])
            if self._fixed_template
            else template
        )

    def _meta_info(
        self,
        template: str,
        logger: MetadataLogger,
        tags: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> None:
        """
          Sends an INFO level message to configured log sources.

        :param template: Message template.
        :param tags: Optional message tags.
        :param logger: Logger to use.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        msg = self._get_template(template).format(**self._get_fixed_args(), **kwargs)
        logger.log_with_metadata(
            logging.INFO,
            msg=msg,
            template=self._get_template(template),
            tags=tags,
            diagnostics=None,
            stack_info=False,
            exception=None,
            metadata_fields=self._get_metadata_fields(kwargs),
        )

    def _meta_warning(
        self,
        template: str,
        logger: MetadataLogger,
        exception: Optional[BaseException] = None,
        tags: Optional[Dict[str, str]] = None,
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
        msg = self._get_template(template).format(**self._get_fixed_args(), **kwargs)
        logger.log_with_metadata(
            logging.WARN,
            msg=msg,
            tags=tags,
            template=template,
            diagnostics=None,
            stack_info=False,
            exception=exception,
            metadata_fields=self._get_metadata_fields(kwargs),
        )

    def _meta_error(
        self,
        template: str,
        logger: MetadataLogger,
        exception: Optional[BaseException] = None,
        tags: Optional[Dict[str, str]] = None,
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
        msg = self._get_template(template).format(**self._get_fixed_args(), **kwargs)
        logger.log_with_metadata(
            logging.ERROR,
            msg=msg,
            template=template,
            tags=tags,
            diagnostics=None,
            stack_info=False,
            exception=exception,
            metadata_fields=self._get_metadata_fields(kwargs),
        )

    def _meta_debug(
        self,
        template: str,
        logger: MetadataLogger,
        exception: Optional[BaseException] = None,
        diagnostics: Optional[str] = None,  # pylint: disable=R0913
        tags: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> None:
        """
          Sends a DEBUG level message to configured log sources.

        :param template: Message template.
        :param exception: Exception associated with this error.
        :param diagnostics: Optional additional diagnostics info.
        :param tags: Optional message tags.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        msg = self._get_template(template).format(**self._get_fixed_args(), **kwargs)
        logger.log_with_metadata(
            logging.DEBUG,
            msg=msg,
            template=template,
            tags=tags,
            diagnostics=diagnostics,
            stack_info=False,
            exception=exception,
            metadata_fields=self._get_metadata_fields(kwargs),
        )

    def _log_redirect_message(
        self,
        logger: MetadataLogger,
        base_template: str,
        message: str,
        tags: Optional[Dict[str, str]] = None,
        log_level: Optional[LogLevel] = None,
    ):
        template = self._get_template(base_template)
        msg = template.format(**self._get_fixed_args(), message=message)
        logger.log_with_metadata(
            from_log_level(log_level) or logger.level,
            msg=msg,
            tags=tags,
            diagnostics=None,
            stack_info=None,
            exception=None,
            metadata_fields=self._get_metadata_fields({}),
            template=template,
        )

    def _prepare_redirect(self) -> tuple[ctypes.CDLL, Any, Any, bytes, bytes]:
        """
        Prepares objects needed for output redirection
        """
        libc = ctypes.CDLL(None)
        saved_stdout = libc.dup(1)
        saved_stderr = libc.dup(2)
        tmp_file_stdout = os.path.join(tempfile.gettempdir(), f"{tempfile.mktemp()}-out").encode("utf-8")
        tmp_file_stderr = os.path.join(tempfile.gettempdir(), f"{tempfile.mktemp()}-out").encode("utf-8")

        return libc, saved_stdout, saved_stderr, tmp_file_stdout, tmp_file_stderr

    def _activate_redirect(
        self, libc: ctypes.CDLL, tmp_file_stdout: bytes, tmp_file_stderr: bytes
    ) -> tuple[bytes, bytes]:
        redirected_fd_stdout = libc.creat(tmp_file_stdout)
        redirected_fd_stderr = libc.creat(tmp_file_stderr)

        libc.dup2(redirected_fd_stdout, 1)
        libc.dup2(redirected_fd_stderr, 2)

        libc.close(redirected_fd_stdout)
        libc.close(redirected_fd_stderr)

        tmp_symlink_stdout = os.path.join(tempfile.gettempdir(), f"{str(uuid.uuid4())}-out").encode("utf-8")
        tmp_symlink_stderr = os.path.join(tempfile.gettempdir(), f"{str(uuid.uuid4())}-err").encode("utf-8")

        os.symlink(tmp_file_stdout, tmp_symlink_stdout)
        os.symlink(tmp_file_stderr, tmp_symlink_stderr)

        os.chmod(tmp_symlink_stdout, 420)
        os.chmod(tmp_symlink_stderr, 420)
        return tmp_symlink_stdout, tmp_symlink_stderr

    def _close_redirect(self, libc: ctypes.CDLL, saved_stdout: Any, saved_stderr: Any):
        libc.dup2(saved_stdout, 1)
        libc.dup2(saved_stderr, 2)

    def _flush_and_log(
        self,
        pos: int,
        tmp_symlink: bytes,
        logger: MetadataLogger,
        tags: Optional[Dict[str, str]] = None,
        log_level: Optional[LogLevel] = None,
    ) -> int:
        sys.stdout.flush()
        with open(tmp_symlink, "r", encoding="utf-8") as output:
            output.seek(pos)
            for line in output.readlines():
                self._log_redirect_message(
                    logger,
                    base_template="Redirected output: {message}",
                    message=line,
                    tags=tags,
                    log_level=log_level,
                )
            return output.tell()

    def _handle_unsupported_redirect(
        self,
        tags: Optional[Dict[str, str]] = None,
    ):
        if sys.platform == "win32":
            self.info(
                self._get_template(">> Output redirection not supported on this platform: {platform} <<"),
                platform=sys.platform,
                tags=tags,
            )
            try:
                yield None
                return
            finally:
                pass

    @contextmanager
    def _redirect(self, logger: MetadataLogger, tags: Optional[Dict[str, str]] = None, log_level=LogLevel.INFO, **_):
        is_active = False
        tmp_symlink_out = b""
        tmp_symlink_err = b""

        def log_redirected() -> tuple[int, int]:
            start_position_out = 0
            start_position_err = 0

            # externally control flush activation
            while tmp_symlink_out == b"" and tmp_symlink_err == b"":
                sleep(0.1)

            # externally control the flushing process
            while is_active:
                start_position_out = self._flush_and_log(
                    pos=start_position_out, tmp_symlink=tmp_symlink_out, logger=logger, tags=tags, log_level=log_level
                )
                start_position_err = self._flush_and_log(
                    pos=start_position_err, tmp_symlink=tmp_symlink_err, logger=logger, tags=tags, log_level=log_level
                )
                sleep(0.1)

            return self._flush_and_log(
                pos=start_position_out, tmp_symlink=tmp_symlink_out, logger=logger, tags=tags, log_level=log_level
            ), self._flush_and_log(
                pos=start_position_err, tmp_symlink=tmp_symlink_err, logger=logger, tags=tags, log_level=log_level
            )

        self._handle_unsupported_redirect(tags)
        libc, saved_stdout, saved_stderr, tmp_file_out, tmp_file_err = self._prepare_redirect()
        log_thread = Thread(target=log_redirected)
        log_thread.start()
        try:
            tmp_symlink_out, tmp_symlink_err = self._activate_redirect(libc, tmp_file_out, tmp_file_err)
            is_active = True
            yield None
        finally:
            is_active = False
            self._close_redirect(libc, saved_stdout, saved_stderr)
