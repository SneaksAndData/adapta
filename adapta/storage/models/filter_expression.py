"""
    Models for generating filter expressions for PyArrow and Astra.
"""
import datetime
import math
from abc import ABC, abstractmethod
from typing import final, Any, TypeVar, Generic, Self
from enum import Enum
import pyarrow.compute
from pyarrow.dataset import field as pyarrow_field

from adapta.utils import chunk_list

TCompileResult = TypeVar("TCompileResult")  # pylint: disable=invalid-name


# pylint: disable=E1101
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

    def isin(self, values: list) -> "Expression":
        """
        Generates a filter condition checking that field value is one of the values provided.
        """
        return Expression(left_operand=self, right_operand=values, operation=FilterExpressionOperation.IN)

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


class _Subexpression:
    """
    Represents a subexpression of an expression.

    expression (Expression): The sub-expression. combine_operation (FilterExpressionOperation): The operation used to
    combine the sub-expression with the rest of the expression.
    """

    def __init__(self, expression: "Expression", combine_operation: FilterExpressionOperation):
        self.expression = expression
        self.combine_operation = combine_operation


class Expression:
    """
    Represents an expression used to filter data.
    """

    def __init__(
        self,
        left_operand: Self | FilterField,
        right_operand: Self | Any | list,
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

    def split_expression(self) -> list[_Subexpression]:
        """
        Splits the expression into smaller parts and returns a list of Subexpression.
        Each Subexpression contains a expression and the operation to combine it with.
        """
        expressions = []
        # Initialize a stack with the top-level expression and a null parent operation
        stack = [(self, None)]

        while stack:
            current, parent_operation = stack.pop()

            # If the current expression is a FilterField, add it to the expressions list as a sub-expression
            if isinstance(current.left_operand, FilterField):
                expressions.append(_Subexpression(current, parent_operation))
                continue

            if not isinstance(current, Expression):
                # Base case or leaf node
                expressions.append(_Subexpression(current, parent_operation))
                continue

            # If the current operation is not the same as the parent operation
            # add the current expression to the expressions list as a sub-expression
            if parent_operation and current.operation != parent_operation:
                expressions.append(_Subexpression(current, parent_operation))
                continue

            # Current operation is consistent with the parent operation or there's no parent
            # Push the right and left operands onto the stack with the current operation
            stack.append((current.right_operand, current.operation))
            stack.append((current.left_operand, current.operation))

        return expressions

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

    def compile(self, subexpressions: list[_Subexpression]) -> TCompileResult:
        """
        Compiles a subexpression, which includes a list of expressions and an operation.
        """
        if not subexpressions:
            raise ValueError("No expressions to compile")

        if len(subexpressions) == 1:
            return self._compile_single_expression(subexpressions[0].expression)

        # Compile each expression in the subexpression
        compiled_results = [self._compile_single_expression(subexpr.expression) for subexpr in subexpressions]

        # Combine the compiled results using the specified operation
        combined_result = compiled_results[0]
        for i, result in enumerate(compiled_results[1:]):
            combined_result = self._combine_results(combined_result, result, subexpressions[i].combine_operation)
        return combined_result

    def _compile_single_expression(self, expr: Expression) -> TCompileResult:
        """
        Compiles a single expression.
        """
        if isinstance(expr.left_operand, FilterField):
            return self._compile_base_case(expr.left_operand.field_name, expr.right_operand, expr.operation)

        # If the operands are also expressions, compile them recursively
        compiled_left = self.compile([_Subexpression(expr.left_operand, expr.operation)])
        compiled_right = self.compile([_Subexpression(expr.right_operand, expr.operation)])

        # Combine the compiled left and right operands
        return self._combine_results(compiled_left, compiled_right, expr.operation)


@final
class AstraFilterExpression(FilterExpression[list[dict[str, Any]]]):
    """
    A concrete implementation of the 'FilterExpression' abstract class for Astra.
    """

    # This value represents the threshold for the maximum length of a list in an IN filter in Astra
    in_select_cartesian_product_failure_threshold = 25

    def _compile_base_case(
        self, field_name: str, field_values: Any, operation: FilterExpressionOperation
    ) -> TCompileResult:
        if (
            operation == FilterExpressionOperation.IN
            and isinstance(field_values, list)
            and len(field_values) > self.in_select_cartesian_product_failure_threshold
        ):
            return self._isin_large_list_result(field_name, field_values, operation)
        return [{f"{field_name}{operation.value['astra']}": field_values}]

    def _combine_results(
        self, compiled_result_a: TCompileResult, compiled_result_b: TCompileResult, operation: FilterExpressionOperation
    ) -> TCompileResult:
        return operation.value["astra"](compiled_result_a, compiled_result_b)

    def _isin_large_list_result(
        self, field_name: str, field_values: list[Any], operation: FilterExpressionOperation
    ) -> TCompileResult:
        # Compile each chunk into an IN operation expression
        return [
            {f"{field_name}{operation.value['astra']}": chunk}
            for chunk in chunk_list(
                field_values, math.ceil(len(field_values) / self.in_select_cartesian_product_failure_threshold)
            )
        ]


@final
class ArrowFilterExpression(FilterExpression[pyarrow.compute.Expression]):
    """
    A concrete implementation of the 'FilterExpression' abstract class for PyArrow.
    """

    def _compile_base_case(
        self, field_name: str, field_values: Any, filter_operation: FilterExpressionOperation
    ) -> TCompileResult:
        field, field_values = self._handle_nested_types(
            pyarrow_field(field_name),
            field_values=field_values,
            filter_operation=filter_operation,
        )

        return filter_operation.value["arrow"](field, field_values)

    @staticmethod
    def _handle_nested_types(
        field: pyarrow.compute.Expression,
        field_values: Any,
        filter_operation: FilterExpressionOperation,
        separator: str = ",",
    ) -> tuple[pyarrow.compute.Expression, Any]:
        """
        Handle nested types in PyArrow filter expressions.

        Processes complex nested types, such as lists and dictionaries, by converting
        the values to strings and joining them with a specified separator. If the
        field values are not nested types, they are returned unchanged.

        :param field: The PyArrow field expression to process.
        :param field_values: The values to compare, which may be nested types
            (e.g., lists, dictionaries) or plain values.
        :param filter_operation: The filtering operation to apply.
        :param separator: The separator used for joining nested values. Defaults to ",".
        :returns: A tuple containing the processed field expression and transformed
            field values.
        """

        def join_list_of_lists(values: list[list], sep: str) -> list[str]:
            return [sep.join(map(str, inner_list)) for inner_list in values]

        def join_dict_values(values: list[dict] | dict, sep: str) -> str | list[str]:
            if isinstance(values, dict):
                return sep.join(map(str, values.values()))
            return [sep.join(map(str, d.values())) for d in values]

        # Handle list of lists
        if isinstance(field_values, list) and isinstance(field_values[0], list):
            field_values = join_list_of_lists(field_values, separator)
            field = pyarrow.compute.binary_join(
                field.cast(pyarrow.list_(pyarrow.string())), pyarrow.scalar(separator, pyarrow.large_string())
            )

        # Handle list of dicts
        elif isinstance(field_values, list) and isinstance(field_values[0], dict):
            field = pyarrow.compute.binary_join_element_wise(
                *[
                    pyarrow.compute.struct_field(field, key).cast(pyarrow.large_string())
                    for key in field_values[0].keys()
                ],
                pyarrow.scalar(separator, pyarrow.large_string()),
            )
            field_values = join_dict_values(field_values, separator)

        # Handle list comparison with EQUALS
        elif isinstance(field_values, list) and filter_operation == FilterExpressionOperation.EQ:
            field_values = separator.join(map(str, field_values))
            field = pyarrow.compute.binary_join(
                field.cast(pyarrow.list_(pyarrow.string())), pyarrow.scalar(separator, pyarrow.large_string())
            )

        # Handle dict
        elif isinstance(field_values, dict):
            field = pyarrow.compute.binary_join_element_wise(
                *[pyarrow.compute.struct_field(field, key).cast(pyarrow.large_string()) for key in field_values.keys()],
                pyarrow.scalar(separator, pyarrow.large_string()),
            )
            field_values = join_dict_values(field_values, separator)

        return field, field_values

    def _combine_results(
        self,
        compiled_result_a: TCompileResult,
        compiled_result_b: TCompileResult,
        filter_operation: FilterExpressionOperation,
    ) -> TCompileResult:
        return filter_operation.value["arrow"](compiled_result_a, compiled_result_b)


class TrinoFilterExpression(FilterExpression[str]):
    """
    A concrete implementation of the 'FilterExpression' abstract class for Trino SQL.
    Compiles filter expressions into Trino-compatible SQL WHERE clause fragments.
    """

    def _compile_base_case(self, field_name: str, field_values: Any, operation: FilterExpressionOperation) -> str:
        # Map EQ to '=' for Trino
        if operation == FilterExpressionOperation.EQ:
            return f"{field_name} = {self._format_value(field_values)}"

        # Handle IN as a series of ORs for Trino
        if operation == FilterExpressionOperation.IN:
            if not isinstance(field_values, list):
                raise ValueError("IN operation requires a list of values")
            return f"{field_name} IN ({', '.join(self._format_value(v) for v in field_values)})"
        # Handle other operations
        op_str = operation.to_string()
        return f"{field_name} {op_str} {self._format_value(field_values)}"

    def _combine_results(
        self, compiled_result_a: str, compiled_result_b: str, operation: FilterExpressionOperation
    ) -> str:
        op_str = operation.to_string()
        return f"({compiled_result_a} {op_str} {compiled_result_b})"

    @staticmethod
    def _format_value(value: Any) -> str:
        # Format value for SQL: quote strings, leave numbers as is and return NULL for None
        if isinstance(value, str):
            return f"'{value}'"
        if isinstance(value, datetime.datetime):
            return f"TIMESTAMP '{value}'"
        if isinstance(value, datetime.date):
            return f"DATE '{value}'"
        if value is None:
            return "NULL"
        return str(value)


def compile_expression(expression: Expression, target: type[FilterExpression[TCompileResult]]) -> TCompileResult:
    """
    Compiles a filter expression using the specified target implementation.
    """
    if not isinstance(expression, Expression):
        raise ValueError(f"Invalid expression type {type(expression)}")
    split_filters = expression.split_expression()
    return target().compile(split_filters)
