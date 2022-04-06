"""
  Connector for Crystal Job Runtime (AKS)
"""
import os
from typing import Dict, Optional

from requests.auth import HTTPBasicAuth

from proteus.utils import session_with_retries

from proteus.connectors.crystal._models import RequestResult, AlgorithmRunResult


class CrystalConnector:
    """
      Crystal API connector
    """

    def __init__(self, *, base_url: str, user: Optional[str] = None, password: Optional[str] = None):
        self.base_url = base_url
        self.http = session_with_retries()
        user = user if user is not None else os.environ.get('CRYSTAL_USER')
        password = password if password is not None else os.environ.get('CRYSTAL_PASSWORD')
        self.http.auth = HTTPBasicAuth(user, password)

    def create_run(self, algorithm: str, payload: Dict, api_version="v1.1") -> str:
        """
          Creates a Crystal job run against the latest API version.

        :param algorithm: Name of a connected algorithm.
        :param payload: Algorithm payload.
        :param api_version: Crystal API version.
        :return: Request identifier assigned to the job by Crystal.
        """
        run_body = {
            "AlgorithmName": algorithm,
            "AlgorithmParameters": payload
        }

        print(f"Sending the following configuration for algorithm {algorithm}: {run_body}")

        run_response = self.http.post(f"{self.base_url}/algorithm/{api_version}/run", json=run_body)

        # raise if not successful
        run_response.raise_for_status()

        run_id = run_response.json()['requestId']

        print(
            f"Algorithm run initiated: {run_id}. Check status at {self.base_url}/algorithm/{api_version}/run/{run_id}/result")

        return run_id

    def retrieve_run(self, run_id: str, api_version="v1.1") -> RequestResult:
        """
        Retrieves a submitted Crystal job.

        :param run_id: Request identifier assigned to the job by Crystal.
        :param api_version: Crystal API version.
        """
        url = f'{self.base_url}/algorithm/{api_version}/run/{run_id}/result'

        response = self.http.get(url=url)

        # raise if not successful
        response.raise_for_status()

        crystal_result = RequestResult.from_dict(response.json())

        return crystal_result

    def submit_result(self, result: AlgorithmRunResult) -> None:
        """
        Submit a result of an algorithm back to Crystal.

        :param result: The result of the algorithm.
        """
        payload = {
            'cause': result.cause,
            'message': result.message,
            'requestId': result.run_id,
            'sasUri': result.sas_uri,
        }

        run_response = self.http.post(
            url=self.base_url,
            json=payload
        )

        # raise if not successful
        run_response.raise_for_status()
