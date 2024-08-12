"""
 QES implementations for delta-rs.
"""
import re
from dataclasses import dataclass
from pydoc import locate
from typing import final, Union, Iterator, Optional

from dataclasses_json import DataClassJsonMixin

from adapta.storage.delta_lake.v3 import load
from adapta.storage.models.base import DataPath
from adapta.storage.models.filter_expression import Expression
from adapta.storage.query_enabled_store._models import QueryEnabledStore, CONNECTION_STRING_REGEX
from adapta.utils.metaframe import MetaFrame


@dataclass
class DeltaCredential(DataClassJsonMixin):
    """
    Delta-rs credential helper for QES.
    """

    auth_client_class: str

    # AWS Related Credentials (Optional)
    auth_client_credentials_class: Optional[str] = None
    access_key: Optional[str] = None
    access_key_id: Optional[str] = None
    region: Optional[str] = None
    endpoint: Optional[str] = None
    session_token: Optional[str] = None

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
    ) -> Union[MetaFrame, Iterator[MetaFrame]]:
        return load(
            auth_client=locate(self.credentials.auth_client_class)(**self.credentials.to_dict()),
            path=path,
            row_filter=filter_expression,
            columns=columns,
        )

    def _apply_query(self, query: str) -> Union[MetaFrame, Iterator[MetaFrame]]:
        raise NotImplementedError("Text queries are not supported by Delta QES")
