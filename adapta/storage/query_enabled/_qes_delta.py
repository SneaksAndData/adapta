import re
from dataclasses import dataclass
from pydoc import locate
from typing import final, Union, Iterator

import pandas
from dataclasses_json import DataClassJsonMixin

from adapta.storage.delta_lake import load
from adapta.storage.models.base import DataPath
from adapta.storage.models.filter_expression import Expression
from adapta.storage.query_enabled._models import QueryEnabledStore, CONNECTION_STRING_REGEX


@dataclass
class DeltaCredential(DataClassJsonMixin):
    auth_client_class: str

    def __post_init__(self):
        if not self.auth_client_class:
            raise ValueError("Authentication plugin class name not provided but is required")


@dataclass
class DeltaSettings(DataClassJsonMixin):
    """
    Delta QES has no additional settings.
    """


@final
class DeltaQes(QueryEnabledStore[DeltaCredential, DeltaSettings]):
    @classmethod
    def _from_connection_string(cls, connection_string: str) -> "QueryEnabledStore[DeltaCredential, DeltaSettings]":
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(credentials=DeltaCredential.from_json(credentials), settings=DeltaSettings.from_json(settings))

    def _apply_filter(
        self, path: DataPath, filter_expression: Expression, columns: list[str]
    ) -> Union[pandas.DataFrame, Iterator[pandas.DataFrame]]:
        return load(
            auth_client=locate(self.credentials.auth_client_class)(),
            path=path,
            row_filter=filter_expression,
            columns=columns,
        )

    def _apply_query(self, query: str) -> Union[pandas.DataFrame, Iterator[pandas.DataFrame]]:
        raise NotImplementedError("Text queries are not supported by Delta QES")
