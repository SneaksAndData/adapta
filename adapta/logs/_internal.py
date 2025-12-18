"""Classes for internal use by `adapta.logs` module. Should not be imported outside this module"""
#  Copyright (c) 2023-2026. ECCO Data & AI and other project contributors.
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

from adapta.logs.models import CompositeLogMetadata, LogLevel


class MetadataLogger(logging.Logger):
    """
    Wrapper for standard python logger that enriches messages with additional metadata
    """

    def __init__(self, name: str, level=logging.NOTSET):
        """
        Creates instance, see logging.Logger.__init__

        :param name: Logger name
        :param level: Log level
        """
        super().__init__(name, level)

    def log_with_metadata(
        self,
        log_level: int,
        msg: str,
        template: str,
        tags: dict[str, str] | None,
        diagnostics: str | None,
        stack_info: bool,
        metadata_fields: dict[str, str] | None,
        exception: BaseException | None,
    ):
        """
        Creates log entry with metadata from Composite Logger

        :param log_level: Level defined in logging module.
        :param msg: Log message after templating.
        :param template: Raw message template.
        :param tags: Optional message tags.
        :param exception: Exception associated with this error.
        :param diagnostics: Optional additional diagnostics info.
        :param stack_info: True if message should contain stack trace information
        :param metadata_fields: Templated arguments (key=value).
        :param exception: Optional exception for warning and error levels
        """

        if not self.isEnabledFor(level=log_level):
            return
        log_metadata = CompositeLogMetadata(
            template=template,
            diagnostics=diagnostics,
            tags=tags,
            fields=metadata_fields,
        )
        self._log(
            log_level,
            msg=msg,
            args=None,
            extra={CompositeLogMetadata.__name__: log_metadata},
            exc_info=exception,
            stack_info=stack_info,
        )


def from_log_level(log_level: LogLevel | None) -> int | None:
    """
    Converts adapta log level to logging log level
    """
    if log_level is None:
        return None
    log_method = {
        LogLevel.INFO: logging.INFO,
        LogLevel.WARN: logging.WARN,
        LogLevel.ERROR: logging.ERROR,
        LogLevel.DEBUG: logging.DEBUG,
    }
    return log_method[log_level]
