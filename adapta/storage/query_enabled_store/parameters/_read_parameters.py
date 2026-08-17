from typing import final, Any

from adapta.storage.models.enum import QueryEnabledStoreOptions
from adapta.storage.models.expression_dsl.filter_expression import FilterExpression
from adapta.storage.query_enabled_store.parameters._base import QueryEnabledStoreReadParameter


@final
class QueryEnabledStoreFilterParameter(QueryEnabledStoreReadParameter[FilterExpression | None]):
    """
    Data filter.
    """

    @property
    def name(self) -> str:
        """See base class."""
        return "filter_expression"


@final
class QueryEnabledStoreSelectParameter(QueryEnabledStoreReadParameter[list[str]]):
    """
    Column selector.
    """

    @property
    def name(self) -> str:
        """See base class."""
        return "columns"


@final
class QueryEnabledStoreReadOptionsParameter(QueryEnabledStoreReadParameter[dict[QueryEnabledStoreOptions, Any]]):
    """
    Read options.
    """

    @property
    def name(self) -> str:
        """See base class."""
        return "options"


@final
class QueryEnabledStoreLimitParameter(QueryEnabledStoreReadParameter[int | None]):
    """
    Read limit (rows).
    """

    @property
    def name(self) -> str:
        """See base class."""
        return "limit"
