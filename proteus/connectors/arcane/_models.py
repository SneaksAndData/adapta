"""
 Models for Arcane
"""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Optional
from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin, dataclass_json, LetterCase


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
class BigQueryStreamConfiguration(StreamConfiguration):
    """
     Stream configuration for Sql Server Change Tracking Source.
    """
    project: str
    dataset: str
    table: str
    entity_name: str
    secret: str
    partition_column_name: str
    change_capture_interval: str
    lookback_interval: str
    full_load_on_start: str
    sink_location: str
    partition_column_name_format: str
    client_tag: str

    @property
    def url_path(self) -> str:
        return "start/bigquery"

    def to_dict(self) -> Dict:
        return {
            "Project": self.project,
            "Dataset": self.dataset,
            "Table": self.table,
            "EntityName": self.entity_name,
            "Secret": self.secret,
            "PartitionColumnName": self.partition_column_name,
            "ChangeCaptureInterval": self.change_capture_interval,
            "LookbackInterval": self.lookback_interval,
            "FullLoadOnStart": self.full_load_on_start,
            "SinkLocation": self.sink_location,
            "PartitionColumnNameFormat": self.partition_column_name_format,
            "ClientTag": self.client_tag
        }


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class StreamError(DataClassJsonMixin):
    """
     Arcane stream failure information.
    """
    error_type: str
    error_message: str
    error_stack: str


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class StreamInfo(DataClassJsonMixin):
    """
      Arcane stream information.
    """
    id: str  # pylint: disable=C0103
    stream_source: str
    started_at: str
    owner: str
    tag: str
    stream_configuration: str
    stream_metadata: str
    stream_state: str
    error: StreamError
    stopped_at: Optional[str] = None


class StreamState(Enum):
    """
     Stream states in Arcane.
    """
    RUNNING = 'RUNNING'
    STOPPED = 'STOPPED'
    TERMINATING = 'TERMINATING'
    RESTARTING = 'RESTARTING'
    FAILED = 'FAILED'
