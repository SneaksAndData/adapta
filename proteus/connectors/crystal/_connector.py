"""
  Connector for Crystal Job Runtime (AKS)
"""
import os
from typing import Dict

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3 import Retry


class CrystalConnector:
    """
      Crystal API connector
    """

    def __init__(self, *, base_url):
        self.base_url = base_url
        retry_strategy = Retry(
            total=4,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS", "TRACE"],
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.http = requests.Session()
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)
        self.http.auth = HTTPBasicAuth(os.environ.get('CRYSTAL_USER'), os.environ.get('CRYSTAL_PASSWORD'))

    def create_run(self, algorithm: str, payload: Dict, api_version="v1.1") -> str:
        """
          Creates a Crystal job run against latest API version.

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
