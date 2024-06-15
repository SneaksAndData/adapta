"""
 Marker interface for logging API
"""
from abc import ABC, abstractmethod
from contextlib import contextmanager, asynccontextmanager
from typing import Optional, Dict


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


class LoggerInterface(ABC):
    """
    Abstract logger interface, enables interchangeability between sync/async loggers
    """

    @abstractmethod
    def info(self, template: str, tags: Optional[Dict[str, str]] = None, **kwargs):
        """
        Logs a message on INFO level
        """

    @abstractmethod
    def warning(
        self, template: str, exception: Optional[BaseException] = None, tags: Optional[Dict[str, str]] = None, **kwargs
    ):
        """
        Logs a message on WARN level
        """

    @abstractmethod
    def error(
        self, template: str, exception: Optional[BaseException] = None, tags: Optional[Dict[str, str]] = None, **kwargs
    ):
        """
        Logs a message on ERROR level
        """

    @abstractmethod
    def debug(
        self,
        template: str,
        exception: Optional[BaseException] = None,
        diagnostics: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs
    ):
        """
        Logs a message on DEBUG level
        """

    @abstractmethod
    def start(self) -> None:
        """
        Optional method to start the logger, if required.
        """

    @abstractmethod
    def stop(self) -> None:
        """
        Optional method to stop and flush the logger, if required.
        """

    @contextmanager
    @abstractmethod
    def redirect(self, tags: Optional[Dict[str, str]] = None, **kwargs):
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
        :param log_level: Optional logging level for a redirected log source. Defaults to INFO.
        :return:
        """

    @asynccontextmanager
    @abstractmethod
    async def redirect_async(self, tags: Optional[Dict[str, str]] = None, **kwargs):
        """
        Async version of a redirect. Not supported in sync client
        """
