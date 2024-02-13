"""
 Marker interface for logging API
"""
from abc import ABC, abstractmethod
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
