"""
    Models for generating filter expressions for PyArrow and Astra.
"""
from abc import ABC, abstractmethod
from typing import final, Any, TypeVar, Generic, Self
from enum import Enum
import pyarrow.compute

try:
    import pyiceberg.expressions
except (ImportError, ModuleNotFoundError):
    pass

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
        "trino": "AND",
        "iceberg": pyiceberg.expressions.And,
    }
    OR = {
        "arrow": pyarrow.compute.Expression.__or__,
        "astra": lambda left_exprs, right_exprs: left_exprs + right_exprs,
        "trino": "OR",
        "iceberg": pyiceberg.expressions.Or,
    }
    GT = {
        "arrow": pyarrow.compute.Expression.__gt__,
        "astra": "__gt",
        "trino": ">",
        "iceberg": pyiceberg.expressions.GreaterThan,
    }
    GE = {
        "arrow": pyarrow.compute.Expression.__ge__,
        "astra": "__gte",
        "trino": ">=",
        "iceberg": pyiceberg.expressions.GreaterThanOrEqual,
    }
    LT = {
        "arrow": pyarrow.compute.Expression.__lt__,
        "astra": "__lt",
        "trino": "<",
        "iceberg": pyiceberg.expressions.LessThan,
    }
    LE = {
        "arrow": pyarrow.compute.Expression.__le__,
        "astra": "__lte",
        "trino": "<=",
        "iceberg": pyiceberg.expressions.LessThanOrEqual,
    }
    EQ = {
        "arrow": pyarrow.compute.Expression.__eq__,
        "astra": "",
        "trino": "=",
        "iceberg": pyiceberg.expressions.EqualTo,
    }
    IN = {"arrow": pyarrow.compute.Expression.isin, "astra": "__in", "trino": "IN", "iceberg": pyiceberg.expressions.In}

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


def compile_expression(expression: Expression, target: type[FilterExpression[TCompileResult]]) -> TCompileResult:
    """
    Compiles a filter expression using the specified target implementation.
    """
    if not isinstance(expression, Expression):
        raise ValueError(f"Invalid expression type {type(expression)}")
    split_filters = expression.split_expression()
    return target().compile(split_filters)
