"""
 QES implementations for delta-rs.
"""
import re
from dataclasses import dataclass
from pydoc import locate
from typing import final, Union, Iterator

from pandas import DataFrame
from dataclasses_json import DataClassJsonMixin

from adapta.storage.delta_lake import load
from adapta.storage.models.base import DataPath
from adapta.storage.models.filter_expression import Expression
from adapta.storage.query_enabled_store._models import QueryEnabledStore, CONNECTION_STRING_REGEX


@dataclass
class DeltaCredential(DataClassJsonMixin):
    """
    Delta-rs credential helper for QES.
    """

    auth_client_class: str

    def __post_init__(self):
        if not self.auth_client_class:
            raise ValueError("Authentication plugin class name not provided but is required")

        if locate(self.auth_client_class) is None:
            raise ModuleNotFoundError(
                "Authentication plugin class name cannot be loaded. Please check the spelling and make sure your application can resolve the import"
            )


@dataclass
class DeltaSettings(DataClassJsonMixin):
    """
    Delta QES has no additional settings.
    """


@final
class DeltaQueryEnabledStore(QueryEnabledStore[DeltaCredential, DeltaSettings]):
    """
    QES Client for Delta Lake reads using delta-rs.
    """

    def close(self) -> None:
        pass

    @classmethod
    def _from_connection_string(
        cls, connection_string: str, lazy_init: bool = False
    ) -> "QueryEnabledStore[DeltaCredential, DeltaSettings]":
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(credentials=DeltaCredential.from_json(credentials), settings=DeltaSettings.from_json(settings))

    def _apply_filter(
        self, path: DataPath, filter_expression: Expression, columns: list[str]
    ) -> Union[DataFrame, Iterator[DataFrame]]:
        return load(
            auth_client=locate(self.credentials.auth_client_class)(),
            path=path,
            row_filter=filter_expression,
            columns=columns,
        )

    def _apply_query(self, query: str) -> Union[DataFrame, Iterator[DataFrame]]:
        raise NotImplementedError("Text queries are not supported by Delta QES")
