"""
    Models for generating filter expressions for PyArrow and Astra.
"""

from enum import Enum
from abc import abstractmethod, ABC
from typing import final, List, Dict, Generic, TypeVar, Any, Union, Type

import pyarrow.compute
from pyarrow.dataset import field as pyarrow_field

TCompileResult = TypeVar("TCompileResult")  # pylint: disable=invalid-name


class FilterExpressionOperation(Enum):
    """
    An enumeration of filter expression operations.
    """

    AND = {
        "arrow": pyarrow.compute.Expression.__and__,
        "astra": lambda left_exprs, right_exprs: [
            left_expr | right_expr for left_expr in left_exprs for right_expr in right_exprs
        ],
    }
    OR = {"arrow": pyarrow.compute.Expression.__or__, "astra": lambda left_exprs, right_exprs: left_exprs + right_exprs}
    GT = {"arrow": pyarrow.compute.Expression.__gt__, "astra": "__gt"}
    GE = {"arrow": pyarrow.compute.Expression.__ge__, "astra": "__gte"}
    LT = {"arrow": pyarrow.compute.Expression.__lt__, "astra": "__lt"}
    LE = {"arrow": pyarrow.compute.Expression.__le__, "astra": "__lte"}
    EQ = {"arrow": pyarrow.compute.Expression.__eq__, "astra": ""}
    IN = {"arrow": pyarrow.compute.Expression.isin, "astra": "__in"}

    def to_string(self):
        """
        Returns a string representation of the FilterExpressionOperation.

        This method maps each FilterExpressionOperation to its corresponding string representation using a dictionary.

        Returns:
            A string representation of the FilterExpressionOperation, or an empty string if the operation is not recognized.
        """
        operation_strings = {
            FilterExpressionOperation.AND: "AND",
            FilterExpressionOperation.OR: "OR",
            FilterExpressionOperation.GT: ">",
            FilterExpressionOperation.GE: ">=",
            FilterExpressionOperation.LT: "<",
            FilterExpressionOperation.LE: "<=",
            FilterExpressionOperation.EQ: "==",
            FilterExpressionOperation.IN: "IN",
        }
        if self not in operation_strings:
            raise ValueError(f"Operation {self} not recognized")
        return operation_strings[self]


@final
class FilterField:
    """
    A generic class that represents a field in a filter expression.
    """

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

    def isin(self, values: List) -> "Expression":
        """
        Generates a filter condition checking that field value is one of the values provided.
        """
        if len(values) <= 25:
            # If 25 or fewer values, create a single IN filter.
            return Expression(left_operand=self, right_operand=values, operation=FilterExpressionOperation.IN)

        # If more than 25, chunk the list and create IN filters for each chunk.
        chunked = chunk_list(values, math.ceil(len(values) / 25))
        sub_filters = [
            Expression(left_operand=self, right_operand=chunk, operation=FilterExpressionOperation.IN)
            for chunk in chunk_list(values, math.ceil(len(values) / 25))
        ]
        # Combine the sub-filters using OR operation.
        if sub_filters:
            combined_filter = reduce(
                lambda a, b: Expression(left_operand=a, right_operand=b, operation=FilterExpressionOperation.OR),
                sub_filters,
            )
            return combined_filter

    def __gt__(self, values: Any) -> "Expression":
        """
        Generates a filter condition checking that field is greater than value.
        """
        return Expression(left_operand=self, right_operand=values, operation=FilterExpressionOperation.GT)

    def __ge__(self, values: Any) -> "Expression":
        """
        Generates a filter condition checking that field is greater or equal to value.
        """
        return Expression(left_operand=self, right_operand=values, operation=FilterExpressionOperation.GE)

    def __lt__(self, values: Any) -> "Expression":
        """
        Generates a filter condition checking that field is less than a value.
        """
        return Expression(left_operand=self, right_operand=values, operation=FilterExpressionOperation.LT)

    def __le__(self, values: Any) -> "Expression":
        """
        Generates a filter condition checking that field is less than or equal to a value.
        """
        return Expression(left_operand=self, right_operand=values, operation=FilterExpressionOperation.LE)

    def __eq__(self, values: Any) -> "Expression":
        """
        Generates a filter condition checking that field is equal to a value.
        """
        return Expression(left_operand=self, right_operand=values, operation=FilterExpressionOperation.EQ)

    def __str__(self):
        """
        Returns the string representation of the field name.
        """
        return self._field_name


class Expression:
    """
    Represents an expression used to filter data.
    """

    def __init__(
        self,
        left_operand: Union["Expression", FilterField],
        right_operand: Union["Expression", Any, List],
        operation: FilterExpressionOperation,
    ):
        assert (isinstance(left_operand, Expression) and isinstance(right_operand, Expression)) or (
            isinstance(left_operand, FilterField) and not isinstance(right_operand, FilterExpression)
        ), (
            "Both left and right operands must either be of type "
            "'Expression' or the left operand should be of type "
            "'FilterField' and right operand should not be of type "
            "'Expression'"
        )

        self.left_operand = left_operand
        self.right_operand = right_operand
        self.operation = operation

    def __and__(self, other: "Expression") -> "Expression":
        return Expression(left_operand=self, right_operand=other, operation=FilterExpressionOperation.AND)

    def __or__(self, other: "Expression") -> "Expression":
        return Expression(left_operand=self, right_operand=other, operation=FilterExpressionOperation.OR)

    def __str__(self):
        if isinstance(self.left_operand, Expression):
            left_str = f"({str(self.left_operand)})"
        else:
            left_str = str(self.left_operand)

        if isinstance(self.right_operand, Expression):
            right_str = f"({str(self.right_operand)})"
        else:
            right_str = str(self.right_operand)
        return f"{left_str} {FilterExpressionOperation.to_string(self.operation)} {right_str}"


class FilterExpression(Generic[TCompileResult], ABC):
    """
    A filter expression that represents a comparison or combination of field values.
    """

    @abstractmethod
    def _compile_base_case(
        self, field_name: str, field_values: Any, operation: FilterExpressionOperation
    ) -> TCompileResult:
        """
        Compiles the base case of a filter expression.
        """

    @abstractmethod
    def _combine_results(
        self, compiled_result_a: TCompileResult, compiled_result_b: TCompileResult, operation: FilterExpressionOperation
    ) -> TCompileResult:
        """
        Combines two compiled results of filter expressions.
        """

    def compile(self, expr: Expression):
        """
        Compiles a filter expression recursively using the concrete implementation of the 'FilterExpression' class.
        """
        if isinstance(expr.left_operand, FilterField):
            return self._compile_base_case(expr.left_operand.field_name, expr.right_operand, expr.operation)

        left_compiled = self.compile(expr.left_operand)
        right_compiled = self.compile(expr.right_operand)

        return self._combine_results(left_compiled, right_compiled, expr.operation)


@final
class AstraFilterExpression(FilterExpression[List[Dict[str, Any]]]):
    """
    A concrete implementation of the 'FilterExpression' abstract class for Astra.
    """

    def _compile_base_case(
        self, field_name: str, field_values: Any, operation: FilterExpressionOperation
    ) -> TCompileResult:
        return [{f"{field_name}{operation.value['astra']}": field_values}]

    def _combine_results(
        self, compiled_result_a: TCompileResult, compiled_result_b: TCompileResult, operation: FilterExpressionOperation
    ) -> TCompileResult:
        return operation.value["astra"](compiled_result_a, compiled_result_b)


@final
class ArrowFilterExpression(FilterExpression[pyarrow.compute.Expression]):
    """
    A concrete implementation of the 'FilterExpression' abstract class for PyArrow.
    """

    def _compile_base_case(
        self, field_name: str, field_values: Any, filter_operation: FilterExpressionOperation
    ) -> TCompileResult:
        return filter_operation.value["arrow"](pyarrow_field(field_name), field_values)

    def _combine_results(
        self,
        compiled_result_a: TCompileResult,
        compiled_result_b: TCompileResult,
        filter_operation: FilterExpressionOperation,
    ) -> TCompileResult:
        return filter_operation.value["arrow"](compiled_result_a, compiled_result_b)


def compile_expression(expr: Expression, target: Type[FilterExpression[TCompileResult]]) -> TCompileResult:
    """
    Compiles a filter expression using the specified target implementation.
    """
    if not isinstance(expr, Expression):
        raise ValueError("Invalid expression type")
    return target().compile(expr)
