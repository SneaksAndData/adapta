import os
from dataclasses import dataclass
from http.client import HTTPException
from typing import Optional

from requests.auth import HTTPBasicAuth

from proteus.connectors.arcane import SqlServerStreamConfiguration, StreamInfo
from utils import session_with_retries


class ArcaneConnector:
    """
      Arcane Streaming API connector
    """

    def __init__(self, *, base_url):
        """
          Creates Arcane Streaming connector, capable of managing Akka streams launched via Arcane.

        :param base_url: Base URL for Arcane Streaming endpoint.
        """
        self.base_url = base_url
        self.http = session_with_retries()
        self.http.auth = HTTPBasicAuth(os.environ.get('ARCANE_USER'), os.environ.get('ARCANE_PASSWORD'))

    def _existing_submission(self, submitted_tag: str, stream_source: str) -> Optional[str]:
        print(f"Looking for existing streams with {submitted_tag}")

        existing_streams = self.http.get(f"{self.base_url}/stream/{stream_source}/tags/{submitted_tag}").json()

        if len(existing_streams) == 0:
            print(f"No active streams found for {submitted_tag}")
            return None

        active_streams = [active_stream_info.id for active_stream_info in existing_streams if
                          not active_stream_info.stoppedAt]

        if len(active_streams) == 0:
            print("None of found streams are active")
            return None

        if len(active_streams) == 1:
            return active_streams[0].id

        raise Exception(
            f"Fatal: more than one active stream of {submitted_tag} is running: {active_streams}. Please review their status and restart/terminate the task accordingly")

    def start_sql_server_ct_stream(self, conf: SqlServerStreamConfiguration):
        """
         Starts a new stream again Sql Server table with change tracking enabled.

        :param conf: Stream configuration
        :return:
        """
        request_json = conf.to_dict()
        submission_result = self.http.post(f"{self.base_url}/stream/{conf.url_path}", json=request_json)
        submission_json = submission_result.json()

        if submission_result.status_code == 200 and submission_json:
            print(
                f"Stream activated: {submission_json['id']}")
        else:
            raise HTTPException(
                f"Error {submission_result.status_code} when submitting a request: {submission_result.text}")

    def get_stream(self, source: str, stream_id: str) -> Optional[StreamInfo]:
        """
          Reads information about the specified stream and source.

        :param source: Stream source.
        :param stream_id: Stream identifier.
        :return:
        """
        info = self.http.get(f"{self.base_url}/stream/{source}/{stream_id}")
        info.raise_for_status()

        return StreamInfo.from_dict(info.json())
