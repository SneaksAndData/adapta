"""
Contains models used in utils module.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class CrystalEntrypointArguments:
    """
    Holds Crystal arguments parsed from command line.
    """
    sas_uri: str
    request_id: str
    results_receiver: str
    sign_result: Optional[bool] = None
