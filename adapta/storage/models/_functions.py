"""
 Models used by Astra DB when working with storage.
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

from typing import Iterable, Optional

from adapta.storage.models.astra import AstraPath
from adapta.storage.models.aws import S3Path
from adapta.storage.models.azure import AdlsGen2Path, WasbPath
from adapta.storage.models.base import DataPath
from adapta.storage.models.local import LocalPath


def parse_data_path(
    path: str, candidates: Iterable[DataPath] = (AdlsGen2Path, LocalPath, WasbPath, AstraPath, S3Path)
) -> Optional[DataPath]:
    """
      Attempts to convert a string path to one of the known DataPath types.

    :param path: A path to convert
    :param candidates: Conversion candidate classes for `DataPath`. Default to all currently supported `DataPath` implementations.
      If a user has their own `DataPath` implementations, those can be supplied instead for convenience.

    :return:
    """
    for candidate in candidates:
        try:
            return candidate.from_hdfs_path(path)
        except:  # pylint: disable=W0702
            continue

    return None
