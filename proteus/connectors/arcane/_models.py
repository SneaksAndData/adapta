from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class SqlServerStreamConfiguration:
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
    url_path: str = "start/sqlserverct"

    def to_json(self) -> Dict:
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
            "ClientTag": self.client_tag
        }


@dataclass
class StreamInfo:
    id: str
    stream_source: str
    started_at: str
    stopped_at: Optional[str]
    owner: str
    tag: str
    stream_configuration: str
    stream_metadata: str

    @classmethod
    def from_json(cls, json_data: Dict):
        return StreamInfo(
            id=json_data['id'],
            stream_source=json_data['streamSource'],
            started_at=json_data['startedAt'],
            stopped_at=json_data['stoppedAt'],
            owner=json_data['owner'],
            tag=json_data['tag'],
            stream_configuration=json_data['streamConfiguration'],
            stream_metadata=json_data['streamMetadata']
        )
