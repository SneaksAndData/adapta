"""
  Models for Crystal connector
"""
from enum import Enum
from dataclasses import dataclass
from typing import Optional


class StatusState(Enum):
    """
     Crystal status states.
    """
    NEW = 'NEW'
    BUFFERED = 'BUFFERED'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    SCHEDULING_TIMEOUT = 'SCHEDULING_TIMEOUT'
    DEADLINE_EXCEEDED = 'DEADLINE_EXCEEDED'
    THROTTLED = 'THROTTLED'


@dataclass
class CrystalResult:
    run_id: str
    status: StatusState
    result_uri: Optional[str] = None
    run_error_message: Optional[str] = None

    @classmethod
    def from_dict(cls, dict_: dict):
        return CrystalResult(
            run_id=dict_['requestId'],
            status=StatusState(dict_['status']),
            result_uri=dict_['resultUri'],
            run_error_message=dict_['runErrorMessage'],
        )


@dataclass
class CrystalAlgorithmResponse:
    run_id: str
    cause: Optional[str] = None
    message: Optional[str] = None
    sas_uri: Optional[str] = None
