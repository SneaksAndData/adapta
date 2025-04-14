"""
 QES implementations for parquet.
"""
import re
from dataclasses import dataclass
from pydoc import locate
from typing import final, Union, Iterator, Optional, Type

import polars as pl
from dataclasses_json import DataClassJsonMixin

from adapta.security.clients import AwsClient, AzureClient
from adapta.storage.blob.base import StorageClient
from adapta.storage.blob.s3_storage_client import S3StorageClient
from adapta.storage.models.base import DataPath
from adapta.storage.models.filter_expression import Expression
from adapta.storage.models.format import PolarsDataFrameParquetSerializationFormat
from adapta.storage.query_enabled_store._models import (
    QueryEnabledStore,
    CONNECTION_STRING_REGEX,
)
from adapta.storage.models.enum import QueryEnabledStoreOptions
from adapta.utils.metaframe import MetaFrame


@dataclass
class ParquetCredential(DataClassJsonMixin):
    """
    Parquet credential helper for QES.
    """

    auth_client_class: str
    auth_client_credentials_class: Optional[str] = None
    auth_client_credentials: Optional[Type] = None

    storage_client: Optional[StorageClient] = None

    def __post_init__(self):
        if not self.auth_client_class:
            raise ValueError("Authentication plugin class name not provided but is required")

        if self.auth_client_credentials_class:
            self.auth_client_credentials = locate(self.auth_client_credentials_class)()

        auth_client = locate(self.auth_client_class)(credentials=self.auth_client_credentials)

        self.storage_client = None

        if isinstance(auth_client, AwsClient):
            self.storage_client = S3StorageClient.create(auth=auth_client)
        if isinstance(auth_client, AzureClient):
            raise NotImplementedError("Azure authentication is not yet supported for parquet QES")

        if self.storage_client is None:
            raise ModuleNotFoundError(
                "Authentication plugin class name cannot be loaded. Please check the spelling and make sure your application can resolve the import"
            )


@dataclass
class ParquetSettings(DataClassJsonMixin):
    """
    Parquet QES has no additional settings.
    """


@final
class ParquetQueryEnabledStore(QueryEnabledStore[ParquetCredential, ParquetSettings]):
    """
    QES Client for Delta Lake reads using parquet.
    """

    def close(self) -> None:
        pass

    @classmethod
    def _from_connection_string(
        cls, connection_string: str, lazy_init: bool = False
    ) -> "QueryEnabledStore[ParquetCredential, ParquetSettings]":
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(credentials=ParquetCredential.from_json(credentials), settings=ParquetSettings.from_json(settings))

    def _apply_filter(
        self,
        path: DataPath,
        filter_expression: Expression,
        columns: list[str],
        options: dict[QueryEnabledStoreOptions, any] | None = None,
        limit: Optional[int] = None,
    ) -> Union[MetaFrame, Iterator[MetaFrame]]:
        polars_table = pl.concat(
            self.credentials.storage_client.read_blobs(
                blob_path=path,
                serialization_format=PolarsDataFrameParquetSerializationFormat,
                filter_predicate=lambda b: b.key.endswith(".parquet"),
            )
        )

        return MetaFrame.from_polars(
            data=polars_table,
        )

    def _apply_query(self, query: str) -> Union[MetaFrame, Iterator[MetaFrame]]:
        raise NotImplementedError("Text queries are not supported by Parquet QES")
