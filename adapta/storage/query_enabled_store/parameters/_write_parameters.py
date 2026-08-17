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

from collections.abc import Iterator
from typing import final

from adapta.storage.query_enabled_store.parameters._base import QueryEnabledStoreWriteParameter
from adapta.utils.metaframe import MetaFrame


@final
class QueryEnabledStoreDataParameter(QueryEnabledStoreWriteParameter[MetaFrame | Iterator[MetaFrame] | None]):
    """
    Data to write.
    """

    @property
    def name(self) -> str:
        """See base class."""
        return "data"


@final
class QueryEnabledStoreOverwriteParameter(QueryEnabledStoreWriteParameter[bool]):
    """
    Overwrite target or not.
    """

    @property
    def name(self) -> str:
        """See base class."""
        return "overwrite"


@final
class QueryEnabledStoreBlockSizeParameter(QueryEnabledStoreWriteParameter[int]):
    """
    Block size for streaming writer.
    """

    @property
    def name(self) -> str:
        """See base class."""
        return "block_size"
