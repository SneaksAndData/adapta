"""Local Query Enabled Store (QES) for reading local files."""
import re
from dataclasses import dataclass
from typing import final, Iterator

from dataclasses_json import DataClassJsonMixin
from pyarrow.parquet import read_table

from adapta.storage.models import DataPath
from adapta.storage.models.filter_expression import Expression, compile_expression, ArrowFilterExpression
from adapta.storage.query_enabled_store._models import QueryEnabledStore, CONNECTION_STRING_REGEX
from adapta.utils.metaframe import MetaFrame


@dataclass
class LocalCredential(DataClassJsonMixin):
    """
    Local credential helper for QES.
    No authentication is required for local files.
    """


@dataclass
class LocalSettings(DataClassJsonMixin):
    """
    Settings for local QES
    """


@final
class LocalQueryEnabledStore(QueryEnabledStore[LocalCredential, LocalSettings]):
    """
    QES Client for local file reads (e.g., Parquet) using PyArrow.
    """

    def close(self) -> None:
        pass

    @classmethod
    def _from_connection_string(
        cls, connection_string: str, lazy_init: bool = False
    ) -> "QueryEnabledStore[LocalCredential, LocalSettings]":
        """
        Parses a connection string for local files.
        """
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(credentials=LocalCredential.from_json(credentials), settings=LocalSettings.from_json(settings))

    def _apply_filter(
        self,
        path: DataPath,
        filter_expression: Expression,
        columns: list[str],
    ) -> MetaFrame | Iterator[MetaFrame]:
        """
        Applies a filter to a local file
        """
        row_filter = compile_expression(filter_expression, ArrowFilterExpression) if filter_expression else None

        pyarrow_table = read_table(
            path.path,
            columns=columns if columns else None,
            filters=row_filter,
        )

        return MetaFrame.from_arrow(
            data=pyarrow_table,
        )

    def _apply_query(self, query: str) -> MetaFrame | Iterator[MetaFrame]:
        """
        Local QES does not natively support SQL-like queries.
        """
        raise NotImplementedError("Text queries are currently not supported by Local QES")
