"""
 Shared functionality for the MetadataLogger enricher implementations.
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
from typing import Optional, Dict

from adapta.logs._internal import MetadataLogger


class _InternalLogger:
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
