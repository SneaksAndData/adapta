"""
 QES implementations for PyIceberg.
"""
import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import final, Self

from dataclasses_json import DataClassJsonMixin
from pyiceberg.catalog import Catalog

from adapta.storage.iceberg.v1 import load_using_catalog, IcebergRestCatalogConfig, get_catalog
from adapta.storage.models.enum import QueryEnabledStoreOptions
from adapta.storage.models.expression_dsl.filter_expression import Expression
from adapta.storage.models.iceberg import IcebergPath
from adapta.storage.query_enabled_store._models import (
    QueryEnabledStore,
    CONNECTION_STRING_REGEX,
)
from adapta.utils.metaframe import MetaFrame


@dataclass
class IcebergCredential(DataClassJsonMixin):
    """
    Credential helper for Iceberg QES.
    """

    oauth_enabled: bool

    def __post_init__(self):
        self._catalog_config = IcebergRestCatalogConfig.from_environment(oauth2_enabled=self.oauth_enabled)

    @property
    def catalog_config(self) -> IcebergRestCatalogConfig:
        """
        Catalog configuration for Iceberg QES.
        """
        return self._catalog_config


@dataclass
class IcebergSettings(DataClassJsonMixin):
    """
    Iceberg QES has no additional settings.
    """

    lazy_read: bool
    catalog_name: str = "default"


@final
class IcebergQueryEnabledStore(QueryEnabledStore[IcebergCredential, IcebergSettings]):
    """
    QES Client for Iceberg tables managed by REST catalog, using PyIceberg.
    """

    def __init__(self, credentials: IcebergCredential, settings: IcebergSettings):
        super().__init__(credentials, settings)
        self._catalog: Catalog | None = None

    def _init_catalog(self) -> Self:
        self._catalog = get_catalog(self.settings.catalog_name, self.credentials.catalog_config)
        return self

    def close(self) -> None:
        pass

    @classmethod
    def _from_connection_string(
        cls, connection_string: str, lazy_init: bool = False
    ) -> "QueryEnabledStore[IcebergCredential, IcebergSettings]":
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(
            credentials=IcebergCredential.from_json(credentials), settings=IcebergSettings.from_json(settings)
        )._init_catalog()

    def _apply_filter(
        self,
        path: IcebergPath,
        filter_expression: Expression,
        columns: list[str],
        options: dict[QueryEnabledStoreOptions, any] | None = None,
        limit: int | None = None,
    ) -> MetaFrame | Iterator[MetaFrame]:
        return load_using_catalog(
            schema=path.schema,
            table_name=path.table,
            columns=columns if columns else None,
            limit=limit,
            version_id=options.get(QueryEnabledStoreOptions.VERSION_ID, None) if options else None,
            lazy_read=self.settings.lazy_read,
            catalog=self._catalog,
        )

    def _apply_query(self, query: str) -> MetaFrame | Iterator[MetaFrame]:
        raise NotImplementedError("Text queries are not supported by Iceberg QES")
