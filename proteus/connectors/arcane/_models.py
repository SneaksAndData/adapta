from dataclasses import dataclass
from typing import Dict


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
            "connectionString": self.connection_string,
            "schema": self.schema,
            "table": self.table,
            "rowsPerGroup": self.rows_per_group,
            "groupingInterval": self.grouping_interval,
            "groupsPerFile": self.groups_per_file,
            "sinkLocation": self.sink_location,
            "sinkFileName": self.sink_filename,
            "fullLoadOnStart": self.full_load_on_start,
            "clientTag": self.client_tag
        }
