"""
  Models for Crystal connector
"""
from enum import Enum
from dataclasses import dataclass
from typing import Optional


class RequestLifeCycleStage(Enum):
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
class RequestResult:
    """
    The Crystal result when retrieving an existing run.
    """
    run_id: str
    status: RequestLifeCycleStage
    result_uri: Optional[str] = None
    run_error_message: Optional[str] = None

    @classmethod
    def from_dict(cls, dict_: dict):
        """
        Constructs a CrystalResult object from a dictionary containing the
        keys from the /result HTTP GET request.

        :param dict_: The (JSON) dict from the HTTP request.
        :return: The corresponding CrystalResult object.
        """
        return RequestResult(
            run_id=dict_['requestId'],
            status=RequestLifeCycleStage(dict_['status']),
            result_uri=dict_['resultUri'],
            run_error_message=dict_['runErrorMessage'],
        )


@dataclass
class AlgorithmRunResult:
    """
    The result of an algorithm to be submitted to Crystal.
    """
    run_id: str
    cause: Optional[str] = None
    message: Optional[str] = None
    sas_uri: Optional[str] = None
