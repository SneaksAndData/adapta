import operator
from enum import Enum
from abc import abstractmethod, ABC
from typing import final, List, Dict, Generic, TypeVar, Any, Union, Type

import pyarrow.compute as pc
from pyarrow.dataset import field as pyarrow_field

TField = TypeVar("TField")  # pylint: disable=invalid-name
TCompileTarget = TypeVar("TCompileTarget")


class FilterExpressionOperation(Enum):
    AND = "&"
    OR = "|"
    GT = ">"
    GE = ">="
    LT = "<"
    LE = "<="
    EQ = "=="
    IN = "isin"


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
    A filter expression that represents a comparison or combination of field values.

    :param left: The left operand of the expression, either another FilterExpression or a FilterField.
    :type left: Union['FilterExpression', FilterField]
    :param right: The right operand of the expression, either another FilterExpression, a TField, or a list of TFields.
    :type right: Union['FilterExpression', TField, List[TField]]
    :param op: The operation to apply to the left and right operands.
    :type op: FilterExpressionOperation
    """

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
    """
     A base class for translating FilterExpressions into a specific target language or library.

     :param TCompileTarget: The type of the compiled expression.
     :type TCompileTarget: Generic
     """
    @abstractmethod
    def compile(self, expr: FilterExpression) -> TCompileTarget:
        pass


@final
class AstraFilterExpressionCompiler(FilterExpressionCompiler[List[Dict[str, Any]]]):
    """
        Translates a FilterExpression into Astra expression.
        :param FilterExpressionCompiler: The base class for the compiler.
        :type FilterExpressionCompiler: TypeVar
    """
    def compile(self, expression: FilterExpression) -> List[Dict[str, Any]]:
        left = expression.left
        right = expression.right
        op = expression.op
        match op:
            case FilterExpressionOperation.EQ:
                if type(right) == list:
                    return [{f"{left.field_name}": right[0]}]
                return [{f"{left.field_name}": right}]
            case FilterExpressionOperation.IN:
                if type(right) == list:
                    return [{f"{left.field_name}__in": right}]
                return [{f"{left.field_name}": right}]
            case FilterExpressionOperation.AND:
                left_result = AstraFilterExpressionCompiler().compile(left)
                right_result = AstraFilterExpressionCompiler().compile(right)
                return [{**d1, **d2} for d1, d2 in zip(left_result, right_result)]
            case FilterExpressionOperation.OR:
                if right.op == FilterExpressionOperation.AND:
                    right_side = AstraFilterExpressionCompiler().compile(right)
                    left_side = AstraFilterExpressionCompiler().compile(left)

                    # Extract the last item from the right_side dictionary
                    key = list(right_side[0])[-1]
                    value = right_side[0][key]

                    # Add the key-value pair to the left_side dictionary
                    left_side[0].update({key: value})

                    # Concatenate the left_side and right_side lists
                    return left_side + right_side
                else:
                    # Compile both sides separately and concatenate the results
                    return AstraFilterExpressionCompiler().compile(left) + AstraFilterExpressionCompiler().compile(
                        right)

            case _:
                func = f"{op.name.lower()[0]}" + "t" + f"{op.name.lower()[1]}" if op in (
                    FilterExpressionOperation.LE, FilterExpressionOperation.GE) else op.name.lower()
                if isinstance(right, list):
                    return [{f"{left.field_name}__{func}": right[0]}]
                return [{f"{left.field_name}__{func}": right}]


@final
class ArrowExpressionCompiler(FilterExpressionCompiler[pc.Expression]):
    """
        Translates a FilterExpression into a PyArrow expression.
        :param FilterExpressionCompiler: The base class for the compiler.
        :type FilterExpressionCompiler: TypeVar
    """

    def compile(self, expression: FilterExpression) -> pc.Expression:
        match expression.op:
            case FilterExpressionOperation.IN:
                # Compile an "isin" expression for the IN operator
                return pyarrow_field(expression.left.field_name).isin(expression.right)
            case FilterExpressionOperation.AND | FilterExpressionOperation.OR:
                # Compile a logical operator expression for 'AND' or 'OR'
                op_func = getattr(operator, expression.op.name.lower() + "_")
                return op_func(ArrowExpressionCompiler().compile(expression.left),
                               ArrowExpressionCompiler().compile(expression.right))
            case _:
                # For other operators, compile a binary operator expression
                op_func = getattr(operator, expression.op.name.lower())
                if type(expression.right) == list:
                    return op_func(pyarrow_field(expression.left.field_name), expression.right[0])
                # This is needed for compiling combined expressions
                return op_func(pyarrow_field(expression.left.field_name), expression.right)
