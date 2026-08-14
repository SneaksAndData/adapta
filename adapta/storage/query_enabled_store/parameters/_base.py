"""
 Query Enabled Store Parameters.
"""

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

from abc import ABC, abstractmethod
from typing import Any


class QueryEnabledStoreOperationParameter(ABC):
    """
    Base parameter class for query enabled store operation parameters.
    """

    def __init__(self, value: Any):
        self._value = value

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Name of this parameter.
        """

    @property
    @abstractmethod
    def value(self) -> Any:
        """
        Value of this parameter.
        """
        return self._value


class QueryEnabledStoreReadParameter(QueryEnabledStoreOperationParameter, ABC):
    """
    Base parameter class for query enabled store read operation parameters.
    """


class QueryEnabledStoreWriteParameter(QueryEnabledStoreOperationParameter, ABC):
    """
    Base parameter class for query enabled store write operation parameters.
    """
