from typing import final, Any

from adapta.storage.models.enum import QueryEnabledStoreOptions
from adapta.storage.models.expression_dsl.filter_expression import FilterExpression
from adapta.storage.query_enabled_store.parameters._base import QueryEnabledStoreReadParameter


@final
class QueryEnabledStoreFilterParameter(QueryEnabledStoreReadParameter):
    """
    Data filter.
    """

    @property
    def name(self) -> str:
        return "filter_expression"

    @property
    def value(self) -> FilterExpression | None:
        return super().value


@final
class QueryEnabledStoreSelectParameter(QueryEnabledStoreReadParameter):
    """
    Column selector.
    """

    @property
    def name(self) -> str:
        return "columns"

    @property
    def value(self) -> list[str]:
        return super().value


@final
class QueryEnabledStoreReadOptionsParameter(QueryEnabledStoreReadParameter):
    """
    Read options.
    """

    @property
    def name(self) -> str:
        return "options"

    @property
    def value(self) -> dict[QueryEnabledStoreOptions, Any]:
        return super().value


@final
class QueryEnabledStoreLimitParameter(QueryEnabledStoreReadParameter):
    """
    Read limit (rows).
    """

    @property
    def name(self) -> str:
        return "limit"

    @property
    def value(self) -> int | None:
        return super().value
