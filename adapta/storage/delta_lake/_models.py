"""
 Models used by delta lake functions.
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

from dataclasses import dataclass
from enum import Enum
from typing import Dict


class DeltaOperation(Enum):
    """
    Possible Delta table operations.
    """

    DELETE = "DELETE"
    UPDATE = "UPDATE"
    WRITE = "WRITE"
    MERGE = "MERGE"
    CREATE_TABLE = "CREATE TABLE"
    CREATE_TABLE_AS_SELECT = "CREATE TABLE AS SELECT"
    CREATE_OR_REPLACE_TABLE_AS_SELECT = "CREATE OR REPLACE TABLE AS SELECT"
    CHANGE_COLUMN = "CHANGE COLUMN"
    VACUUM_START = "VACUUM START"
    VACUUM_END = "VACUUM END"
    UNDEFINED = "UNDEFINED"


@dataclass
class DeltaTransaction:
    """
    A subset of Delta table transaction entry properties.
    """

    version: int
    timestamp: int
    operation: DeltaOperation
    operation_parameters: Dict
    read_version: int
    is_blind_append: bool

    @classmethod
    def from_dict(cls, value: Dict) -> "DeltaTransaction":
        """
          Converts delta transaction log entry to DeltaTransaction.
        :param value: single entry from `describe history ...`
        :return:
        """
        delta_op = value.get("operation", DeltaOperation.UNDEFINED.value)
        supported_ops = set(item.value for item in DeltaOperation)

        return cls(
            version=value.get("version", -1),
            timestamp=value["timestamp"],
            operation=DeltaOperation(delta_op) if delta_op in supported_ops else DeltaOperation.UNDEFINED,
            operation_parameters=value.get("operationParameters", {}),
            read_version=value.get("readVersion", -1),
            is_blind_append=value.get("isBlindAppend", False),
        )
