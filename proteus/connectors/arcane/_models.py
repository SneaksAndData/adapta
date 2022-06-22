"""
 Models for Arcane
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional


class StreamConfiguration(ABC):
    """
     Base configuration for all streams.
    """

    @abstractmethod
    def to_dict(self):
        """
          Converts this to the payload accepted by streaming start endpoint.
        :return:
        """

    @property
    @abstractmethod
    def url_path(self):
        """
         Url path for streams matching this configuration.
        :return:
        """


@dataclass
class SqlServerStreamConfiguration(StreamConfiguration):
    """
     Stream configuration for Sql Server Change Tracking Source.
    """
    connection_string: str
    schema: str
    table: str
    rows_per_group: int
    grouping_interval: str
    groups_per_file: int
    sink_location: str
    sink_filename: str
    full_load_on_start: bool
    client_tag: str
    lookback_interval: int = 86400
    change_capture_interval: str = "0.00:00:15"
    command_timeout: int = 3600

    @property
    def url_path(self) -> str:
        return "start/sqlserverct"

    def to_dict(self) -> Dict:
        return {
            "ConnectionString": self.connection_string,
            "Schema": self.schema,
            "Table": self.table,
            "RowsPerGroup": self.rows_per_group,
            "GroupingInterval": self.grouping_interval,
            "GroupsPerFile": self.groups_per_file,
            "SinkLocation": self.sink_location,
            "SinkFileName": self.sink_filename,
            "FullLoadOnStart": self.full_load_on_start,
            "ClientTag": self.client_tag,
            "LookbackInterval": self.lookback_interval,
            "ChangeCaptureInterval": self.change_capture_interval,
            "CommandTimeout": self.command_timeout
        }


@dataclass
class CdmChangeFeedStreamConfiguration(StreamConfiguration):
    """
     Stream configuration for Sql Server Change Tracking Source.
    """
    storage_account_connection_string: str
    base_location: str
    entity_name: str
    rows_per_group: int
    grouping_interval: str
    groups_per_file: int
    sink_location: str
    sink_filename: str
    full_load_on_start: bool
    client_tag: str
    http_client_max_retries: int = 3
    http_client_retry_delay: str = "0.00:00:01"
    change_capture_interval: str = "0.00:00:15"

    @property
    def url_path(self) -> str:
        return "start/microsoft_cdm"

    def to_dict(self) -> Dict:
        return {
            "StorageAccountConnectionString": self.storage_account_connection_string,
            "HttpClientMaxRetries": self.http_client_max_retries,
            "HttpClientRetryDelay": self.http_client_retry_delay,
            "BaseLocation": self.base_location,
            "EntityName": self.entity_name,
            "FullLoadOnStart": self.full_load_on_start,
            "ChangeCaptureInterval": self.change_capture_interval,
            "RowsPerGroup": self.rows_per_group,
            "GroupingInterval": self.grouping_interval,
            "GroupsPerFile": self.groups_per_file,
            "SinkLocation": self.sink_location,
            "SinkFileName": self.sink_filename,
            "ClientTag": self.client_tag
        }


@dataclass
class StreamInfo:
    """
      Arcane stream information.
    """
    id: str  # pylint: disable=C0103
    stream_source: str
    started_at: str
    stopped_at: Optional[str]
    owner: str
    tag: str
    stream_configuration: str
    stream_metadata: str
    stream_state: str

    @classmethod
    def from_dict(cls, json_data: Dict):
        """
          Converts json returned by stream info endpoint to this dataclass.

        :param json_data: JSON response from Arcane stream info.
        :return:
        """
        return StreamInfo(
            id=json_data['id'],
            stream_source=json_data['streamSource'],
            started_at=json_data['startedAt'],
            stopped_at=json_data['stoppedAt'],
            owner=json_data['owner'],
            tag=json_data['tag'],
            stream_configuration=json_data['streamConfiguration'],
            stream_metadata=json_data['streamMetadata'],
            stream_state=json_data['streamState']
        )


class StreamState(Enum):
    """
     Stream states in Arcane.
    """
    RUNNING = 'RUNNING'
    STOPPED = 'STOPPED'
    TERMINATING = 'TERMINATING'
    RESTARTING = 'RESTARTING'
    FAILED = 'FAILED'
