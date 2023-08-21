"""
 Models used by Astra DB when working with storage.
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

from dataclasses import dataclass
from typing import Union

from adapta.storage.models.base import DataPath, DataProtocols


@dataclass
class AstraPath(DataPath):
    """
    Path wrapper for Astra DB.
    """

    astra_id: str
    astra_region: str

    def base_uri(self) -> str:
        return f"https://{self.astra_id}-{self.astra_region}.apps.astra.datastax.com/api/rest/"

    @classmethod
    def from_uri(cls, url: str) -> "DataPath":
        assert url.startswith("https://") and (
                ".apps.astra.datastax.com/api" in url
        ), ("Invalid URL supplied. Please use the following format: "
            "https://<astra-id>-<astra-region>.apps.astra.datastax.com/api/rest/")

        return cls(
            astra_id=url.split('/')[2].split('-')[0],
            astra_region=url.split('/')[2].split('-')[1]
        )

    def to_uri(self) -> str:
        return f"https://{self.astra_id}-{self.astra_region}.apps.astra.datastax.com/api/rest/"

    def _check_path(self):
        assert not self.path.startswith("/"), "Path should not start with /"

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "DataPath":
        pass

    def to_hdfs_path(self) -> str:
        pass

    def to_delta_rs_path(self) -> str:
        pass
