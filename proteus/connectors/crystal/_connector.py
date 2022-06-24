"""
  Connector for Crystal Job Runtime (AKS)
"""
import os
from typing import Dict, Optional, Type, TypeVar

from requests.auth import HTTPBasicAuth

from proteus.utils import session_with_retries, CrystalEntrypointArguments
from proteus.connectors.crystal._models import RequestResult, AlgorithmRunResult
from proteus.storage.models.format import SerializationFormat

T = TypeVar('T')  # pylint: disable=C0103


class CrystalConnector:
    """
      Crystal API connector
    """

    def __init__(self, *, base_url: str, user: Optional[str] = None, password: Optional[str] = None):
        self.base_url = base_url
        self.http = session_with_retries()
        if user is not None and password is not None:
            self.http.auth = HTTPBasicAuth(user, password)

    @staticmethod
    def create_authenticated(*, base_url: str, user: Optional[str] = None, password: Optional[str] = None):
        """Creates Crystal connector with basic authentication.
        For connecting to Crystal outside the Crystal kubernetes cluster, e.g.
        from other cluster or Airflow environment.
        """
        return CrystalConnector(base_url=base_url,
                                user=user or os.environ.get('CRYSTAL_USER'),
                                password=password or os.environ.get('CRYSTAL_PASSWORD'))

    @staticmethod
    def create_anonymous(*, base_url: str):
        """Creates Crystal connector with no authentication.
         This should be use for accessing Crystal from inside a hosting cluster."""
        return CrystalConnector(base_url=base_url, user=None, password=None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()

    def create_run(self, algorithm: str, payload: Dict, api_version: str = "v1.1") -> str:
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

    def retrieve_run(self, run_id: str, api_version: str = "v1.1") -> RequestResult:
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

    def submit_result(self, result: AlgorithmRunResult, url: str) -> None:
        """
        Submit a result of an algorithm back to Crystal.
        Notice, this method is only intended to be used within Crystal.

        :param result: The result of the algorithm.
        :param url: URL of the results receiver.
        """
        payload = {
            'cause': result.cause,
            'message': result.message,
            'requestId': result.run_id,
            'sasUri': result.sas_uri,
        }

        run_response = self.http.post(
            url=url,
            json=payload
        )

        # raise if not successful
        run_response.raise_for_status()

    @staticmethod
    def read_input(*, crystal_arguments: CrystalEntrypointArguments,
                   serialization_format: Type[SerializationFormat[T]]) -> T:
        """
        Read Crystal input given in the SAS URI provided in the CrystalEntrypointArguments
        :param crystal_arguments: The arguments given to the Crystal job.
        :param serialization_format: The format used to deserialize the contents of the SAS URI.
        :return: The deserialized input data.
        """
        http_session = session_with_retries()
        http_response = http_session.get(url=crystal_arguments.sas_uri)
        http_response.raise_for_status()
        http_session.close()
        return serialization_format().deserialize(http_response.content)

    def dispose(self) -> None:
        """
        Gracefully dispose object.
        """
        self.http.close()
