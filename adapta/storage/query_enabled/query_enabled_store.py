"""
 Abstraction for storage operations.
"""
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

from abc import ABC, abstractmethod
from typing import Union, Iterator

import pandas

from adapta.storage.models.filter_expression import Expression
from adapta.storage.query_enabled._models import QueryEnabledStoreConnection


class QueryEnabledStore(ABC):
    def __init__(self, connection_string: str):
        self._connection = QueryEnabledStoreConnection.from_string(connection_string)

    @abstractmethod
    def apply_filter(self, filter_expression: Expression) -> Union[pandas.DataFrame, Iterator[pandas.DataFrame]]:
        """
        Applies the provided filter expression to this Store and returns the result in a pandas DataFrame
        """

    @abstractmethod
    def apply_query(self, query: str) -> Union[pandas.DataFrame, Iterator[pandas.DataFrame]]:
        """
        Applies a plaintext query to this Store and returns the result in a pandas DataFrame
        """
