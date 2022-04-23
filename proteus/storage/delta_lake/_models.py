"""
 Models used by delta lake functions.
"""
from dataclasses import dataclass
from enum import Enum
from typing import Dict


class DeltaOperation(Enum):
    """
     Possible Delta table operations.
    """
    DELETE = 'DELETE'
    UPDATE = 'UPDATE'
    WRITE = 'WRITE'
    MERGE = 'MERGE'
    CREATE_TABLE_AS_SELECT = 'CREATE TABLE AS SELECT'


@dataclass
class DeltaTransaction:
    """
     A subset of Delta table transaction entry properties.
    """
    timestamp: int
    operation: DeltaOperation
    operation_parameters: Dict
    read_version: int
    is_blind_append: bool

    @classmethod
    def from_dict(cls, value: Dict) -> 'DeltaTransaction':
        """
          Converts delta transaction log entry to DeltaTransaction.
        :param value: single entry from `describe history ...`
        :return:
        """
        return DeltaTransaction(
            timestamp=value['timestamp'],
            operation=DeltaOperation(value['operation']),
            operation_parameters=value['operation_parameters'],
            read_version=value['readVersion'],
            is_blind_append=value['isBlindAppend']
        )
