from abc import abstractmethod, ABC
from enum import Enum
from typing import final, List, Dict, Generic, TypeVar, Any, Union, Type

import operator
import pyarrow.compute as pc
from pyarrow.dataset import field as pyarrow_field

TField = TypeVar("TField")  # pylint: disable=invalid-name
TCompileTarget = TypeVar("TCompileTarget")


# TODO: USAGE
# f1 = FilterField[str]("col_a")
# f2 = FilterField[int]("col_b")
# expr = (f1 == "abc") & (f2 == 123)
# astra_expr = AstraFilterExpressionCompiler().compile(expr)
# pyarrow_expr = ArrowExpressionCompiler().compile(expr)

class FilterExpressionOperation(Enum):
    AND = "&"
    OR = "|"
    GT = ">"  # __gt (astra)
    GE = ">="  # __gte (astra)
    LT = "<"  # __lt (astra)
    LE = "<="  # __lte (astra)
    EQ = "=="
    IN = "isin"  # __in (ASTRA) # TODO: not sure about this --> is_in is only for pyarrow and for Astra is __in


@final
class FilterField(Generic[TField]):
    def __init__(self, field_name):
        """
        Creates an instance of FilterField
        """
        self._field_name = field_name

    @property
    def field_name(self):
        """
        Name of the wrapped field.
        """
        return self._field_name

    def isin(self, values: List[TField]) -> "FilterExpression":
        """
        Generates a filter condition checking that field value is one of the values provided.
        """
        return FilterExpression(left=self, right=values, op=FilterExpressionOperation.IN)

    def __gt__(self, values: List[TField]) -> "FilterExpression":
        """
        Generates a filter condition checking that field is greater than value.
        """
        return FilterExpression(left=self, right=values, op=FilterExpressionOperation.GT)

    def __ge__(self, values: List[TField]) -> "FilterExpression":
        """
        Generates a filter condition checking that field is greater or equal to value.
        """
        return FilterExpression(left=self, right=values, op=FilterExpressionOperation.GE)

    def __lt__(self, values: List[TField]) -> "FilterExpression":
        """
        Generates a filter condition checking that field is less than a value.
        """
        return FilterExpression(left=self, right=values, op=FilterExpressionOperation.LT)

    def __le__(self, values: List[TField]) -> "FilterExpression":
        """
        Generates a filter condition checking that field is less than or equal to a value.
        """
        return FilterExpression(left=self, right=values, op=FilterExpressionOperation.LE)

    def __eq__(self, values: List[TField]) -> "FilterExpression":
        """
        Generates a filter condition checking that field is equal to a value.
        """
        return FilterExpression(left=self, right=values, op=FilterExpressionOperation.EQ)


@final
class FilterExpression:
    """
    Filter expression
    """

    # TODO: Add docstring
    def __init__(self, left: Union['FilterExpression', FilterField],
                 right: Union['FilterExpression', TField, List[TField]],
                 op: FilterExpressionOperation):
        self.left = left
        self.right = right
        self.op = op

    def __and__(self, other: "FilterExpression") -> "FilterExpression":
        return FilterExpression(left=self, right=other, op=FilterExpressionOperation.AND)

    def __or__(self, other: "FilterExpression") -> "FilterExpression":
        return FilterExpression(left=self, right=other, op=FilterExpressionOperation.OR)


class FilterExpressionCompiler(Generic[TCompileTarget], ABC):
    @abstractmethod
    def compile(self, expr: FilterExpression) -> TCompileTarget:
        pass


@final
class AstraFilterExpressionCompiler(FilterExpressionCompiler[List[Dict[str, Any]]]):
    def compile(self, expression: FilterExpression) -> List[Dict[str, Any]]:
        # TODO: Add support for compound expressions
        left = expression.left
        right = expression.right
        op = expression.op
        match op:
            case FilterExpressionOperation.EQ:
                return [{f"{left.field_name}": right[0]}]
            case FilterExpressionOperation.IN:
                return [{f"{left.field_name}__in": right}]
            case FilterExpressionOperation.LE:
                return [{f"{left.field_name}__lte": right[0]}]
            case FilterExpressionOperation.GE:
                return [{f"{left.field_name}__gte": right[0]}]
            case _:
                return [{f"{left.field_name}__{op.name.lower()}": right[0]}]


@final
class ArrowExpressionCompiler(FilterExpressionCompiler[pc.Expression]):

    def compile(self, expression: FilterExpression) -> pc.Expression:
        match expression.op:
            case FilterExpressionOperation.IN:
                return pyarrow_field(expression.left.field_name).isin(expression.right)
            case FilterExpressionOperation.AND | FilterExpressionOperation.OR:
                op_func = getattr(operator, expression.op.name.lower() + "_")
                return op_func(ArrowExpressionCompiler().compile(expression.left),
                               ArrowExpressionCompiler().compile(expression.right))
            case _:
                op_func = getattr(operator, expression.op.name.lower())
                if type(expression.right) == list:
                    return op_func(pyarrow_field(expression.left.field_name), expression.right[0])
                # This is needed for compiling compound expressions
                return op_func(pyarrow_field(expression.left.field_name), expression.right)
