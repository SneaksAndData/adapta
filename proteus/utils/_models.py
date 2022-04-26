"""
Contains models used in utils module.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class CrystalArguments:
    """
    Holds Crystal arguments parsed from command line.
    """
    sas_uri: str
    request_id: str
    results_receiver: str
    results_receiver_user: Optional[str]
    results_receiver_password: Optional[str]
    sign_result: Optional[bool]
