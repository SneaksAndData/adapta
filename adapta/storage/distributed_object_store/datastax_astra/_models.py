"""
 Auxiliary models for Astra client operations.
"""

# TODO: delete
from typing import final, List, Dict, Generic, TypeVar, Any

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

    def __and__(self, other: "AstraFilterExpression") -> "AstraFilterExpression":
        self._expr = [other_expr | expr for other_expr in other.expression for expr in self._expr]
        return self

    def __or__(self, other: "AstraFilterExpression") -> "AstraFilterExpression":
        self._expr = self._expr + other.expression
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

    @property
    def field_name(self):
        """
        Name of the wrapped field.
        """
        return self._field_name

    def isin(self, values: List[TField]) -> "AstraFilterExpression":
        """
        Generates a filter condition checking that field value is one of the values provided.
        """
        return AstraFilterExpression([{f"{self._field_name}__in": values}])

    def __gt__(self, value: TField) -> "AstraFilterExpression":
        """
        Generates a filter condition checking that field is greater than value.
        """
        return AstraFilterExpression([{f"{self._field_name}__gt": value}])

    def __ge__(self, value: TField) -> "AstraFilterExpression":
        """
        Generates a filter condition checking that field is greater or equal to value.
        """
        return AstraFilterExpression([{f"{self._field_name}__gte": value}])

    def __lt__(self, value: TField) -> "AstraFilterExpression":
        """
        Generates a filter condition checking that field is less than a value.
        """
        return AstraFilterExpression([{f"{self._field_name}__lt": value}])

    def __le__(self, value: TField) -> "AstraFilterExpression":
        """
        Generates a filter condition checking that field is less than or equal to a value.
        """
        return AstraFilterExpression([{f"{self._field_name}__lte": value}])

    def __eq__(self, value: TField) -> "AstraFilterExpression":
        """
        Generates a filter condition checking that field is equal to a value.
        """
        return AstraFilterExpression([{self._field_name: value}])
