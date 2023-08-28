"""
 Auxiliary models for Astra client operations.
"""
from functools import reduce
from typing import final, List, Dict, Generic, TypeVar, Union, Any

TField = TypeVar('TField')


class AstraFilterExpression:
    def __init__(self, expr: List[Dict[str, Any]]):
        self._expr = expr

    @property
    def expression(self) -> List[Dict[str, Any]]:
        return self._expr

    def __and__(self, other: 'AstraFilterExpression') -> 'AstraFilterExpression':
        self._expr = reduce(lambda e1, e2: e1 | e2, self._expr + other.expression)
        return self

    def __or__(self, other: 'AstraFilterExpression') -> 'AstraFilterExpression':
        self._expr = self._expr + other.expression
        return self


@final
class AstraField(Generic[TField]):
    """
     Represents a field in the Astra table. Semantics mimic pyarrow.
     Based on https://docs.datastax.com/en/developer/python-driver/3.24/cqlengine/queryset/
    """
    def __init__(self, field_name: str):
        self._field_name = field_name
        self._field_filters: Dict[str, TField] = dict()

    @property
    def field_name(self):
        return self._field_name

    def with_filters(self, field_filters: Dict[str, TField]) -> 'AstraField[TField]':
        self._field_filters.update(field_filters)
        return self

    def isin(self, values: List[TField]) -> 'AstraField[TField]':
        self._field_filters.update({
            f"{self._field_name}__in": values
        })

        return self

    def gt(self, value: TField) -> 'AstraField[TField]':
        self._field_filters.update({
            f"{self._field_name}__gt": value
        })

        return self

    def gte(self, value: TField) -> 'AstraField[TField]':
        self._field_filters.update({
            f"{self._field_name}__gte": value
        })

        return self

    def lt(self, value: TField) -> 'AstraField[TField]':
        self._field_filters.update({
            f"{self._field_name}__lt": value
        })

        return self

    def lte(self, value: TField) -> 'AstraField[TField]':
        self._field_filters.update({
            f"{self._field_name}__lte": value
        })

        return self

    def eq(self, value: TField) -> 'AstraField[TField]':
        self._field_filters.update({
            self._field_name: value
        })

        return self

    @property
    def field_filters(self) -> Dict[str, Any]:
        return self._field_filters

    def __and__(self, other: 'AstraField[TField]') -> AstraFilterExpression:
        assert isinstance(other, AstraField), f'Cannot concatenate this AstraField with object of type {type(other)}'

        return AstraFilterExpression([self.field_filters | other.field_filters])

    def __or__(self, other: 'AstraField[TField]') -> AstraFilterExpression:
        assert isinstance(other, AstraField), f'Cannot concatenate this AstraField with object of type {type(other)}'

        return AstraFilterExpression([self.field_filters, other.field_filters])
