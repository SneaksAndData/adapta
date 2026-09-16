"""
Models used by Astra DB when working with storage.
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

from dataclasses import dataclass

from adapta.storage.models.base import DataProtocols
from adapta.storage.models.cassandra import CassandraPath


@dataclass
class AstraPath(CassandraPath):
    """
    Path wrapper for Astra DB.
    """

    protocol: str = DataProtocols.ASTRA.value

    @classmethod
    def _path_regex(cls) -> str:
        return r"^(astra)(?:\+(.+))?:\/\/(.+)@(.+)$"
