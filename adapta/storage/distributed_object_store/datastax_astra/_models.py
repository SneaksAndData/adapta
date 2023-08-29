"""
 Auxiliary models for Astra client operations.
"""
from functools import reduce
from typing import final, List, Dict, Generic, TypeVar, Any, Union

TField = TypeVar("TField")  # pylint: disable=invalid-name


@final
class AstraFilterExpression:
    """
    Filter expression from combining two or more Astra fields
    """

    def __init__(self, expr: List[Dict[str, Any]]):
        """
        Creates an AstraFilterExpression
        """
        self._expr = expr

    @property
    def expression(self) -> List[Dict[str, Any]]:
        """
        Generated expression.
        """
        return self._expr

    def __and__(self, other: Union["AstraFilterExpression", "AstraField"]) -> "AstraFilterExpression":
        added_expr = [other.expression] if isinstance(other, AstraField) else other.expression
        self._expr = reduce(lambda e1, e2: e1 | e2, self._expr + added_expr)
        return self

    def __or__(self, other: Union["AstraFilterExpression", "AstraField"]) -> "AstraFilterExpression":
        added_expr = [other.expression] if isinstance(other, AstraField) else other.expression
        self._expr = self._expr + added_expr
        return self


@final
class AstraField(Generic[TField]):
    """
    Represents a field in the Astra table. Semantics mimic pyarrow.
    Based on https://docs.datastax.com/en/developer/python-driver/3.24/cqlengine/queryset/
    """

    def __init__(self, field_name: str):
        """
        Creates an instance of AstraField with empty filters applied.
        """
        self._field_name = field_name
        self._field_filters: Dict[str, TField] = {}

    @property
    def field_name(self):
        """
        Name of the wrapped field.
        """
        return self._field_name

    def with_filters(self, field_filters: Dict[str, TField]) -> "AstraField[TField]":
        """
        Merge current filters with provided ones.
        """
        self._field_filters.update(field_filters)
        return self

    def isin(self, values: List[TField]) -> "AstraField[TField]":
        """
        Generates a filter condition checking that field value is one of the values provided.
        """
        self._field_filters.update({f"{self._field_name}__in": values})

        return self

    def __gt__(self, value: TField) -> "AstraField[TField]":
        """
        Generates a filter condition checking that field is greater than value.
        """
        self._field_filters.update({f"{self._field_name}__gt": value})

        return self

    def __ge__(self, value: TField) -> "AstraField[TField]":
        """
        Generates a filter condition checking that field is greater or equal to value.
        """
        self._field_filters.update({f"{self._field_name}__gte": value})

        return self

    def __lt__(self, value: TField) -> "AstraField[TField]":
        """
        Generates a filter condition checking that field is less than a value.
        """
        self._field_filters.update({f"{self._field_name}__lt": value})

        return self

    def __le__(self, value: TField) -> "AstraField[TField]":
        """
        Generates a filter condition checking that field is less than or equal to a value.
        """
        self._field_filters.update({f"{self._field_name}__lte": value})

        return self

    def __eq__(self, value: TField) -> "AstraField":  # type: ignore[override]
        """
        Generates a filter condition checking that field is equal to a value.
        """
        self._field_filters.update({self._field_name: value})

        return self

    @property
    def expression(self) -> Dict[str, Any]:
        """
        Return current filter set state.
        """
        return self._field_filters

    def __and__(self, other: "AstraField[TField]") -> AstraFilterExpression:
        """
        Combine two fields with AND.
        """
        assert isinstance(other, AstraField), f"Cannot concatenate this AstraField with object of type {type(other)}"

        return AstraFilterExpression([self.expression | other.expression])

    def __or__(self, other: "AstraField[TField]") -> AstraFilterExpression:
        """
        Combine two fields with OR.
        """
        assert isinstance(other, AstraField), f"Cannot concatenate this AstraField with object of type {type(other)}"

        return AstraFilterExpression([self.expression, other.expression])
