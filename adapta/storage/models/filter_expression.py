"""The operator module defines functions that implement the basic Python operators"""
import abc
import operator
from enum import Enum
from abc import abstractmethod, ABC
from typing import final, List, Dict, Generic, TypeVar, Any, Union, Type

import pyarrow.compute
import pyarrow.compute as pc
from pyarrow.dataset import field as pyarrow_field

TField = TypeVar("TField")  # pylint: disable=invalid-name
TCompileResult = TypeVar("TCompileResult")
TCompileTarget = TypeVar("TCompileTarget")  # pylint: disable=invalid-name


class FilterExpressionOperation(Enum):
    """
    An enumeration of filter expression operations.
    """

    AND = "&"
    OR = "|"
    GT = ">"
    GE = ">="
    LT = "<"
    LE = {
        "arrow": pyarrow.compute.Expression.__le__,
        "astra": "__lte"
    }
    EQ = {
        "arrow": pyarrow.compute.Expression.__eq__,
        "astra": ""
    }
    IN = "isin"


@final
class FilterField(Generic[TField]):
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

    def isin(self, values: List[TField]) -> "FilterExpression":
        """
        Generates a filter condition checking that field value is one of the values provided.
        """
        return FilterExpression(left_operand=self, right_operand=values, operation=FilterExpressionOperation.IN)

    def __gt__(self, values: List[TField]) -> "FilterExpression":
        """
        Generates a filter condition checking that field is greater than value.
        """
        return FilterExpression(left_operand=self, right_operand=values, operation=FilterExpressionOperation.GT)

    def __ge__(self, values: List[TField]) -> "FilterExpression":
        """
        Generates a filter condition checking that field is greater or equal to value.
        """
        return FilterExpression(left_operand=self, right_operand=values, operation=FilterExpressionOperation.GE)

    def __lt__(self, values: List[TField]) -> "FilterExpression":
        """
        Generates a filter condition checking that field is less than a value.
        """
        return FilterExpression(left_operand=self, right_operand=values, operation=FilterExpressionOperation.LT)

    def __le__(self, values: List[TField]) -> "FilterExpression":
        """
        Generates a filter condition checking that field is less than or equal to a value.
        """
        return FilterExpression(left_operand=self, right_operand=values, operation=FilterExpressionOperation.LE)

    def __eq__(self, values: List[TField]) -> "FilterExpression":
        """
        Generates a filter condition checking that field is equal to a value.
        """
        return FilterExpression(left_operand=self, right_operand=values, operation=FilterExpressionOperation.EQ)


class FilterExpression(Generic[TCompileResult], abc.ABC):
    """
    A filter expression that represents a comparison or combination of field values.
    """

    def __init__(
        self,
        left_operand: Union["FilterExpression", FilterField],
        right_operand: Union["FilterExpression", TField, List[TField]],
        operation: FilterExpressionOperation,
    ):
        assert (type(left_operand) is FilterExpression and type(right_operand) is FilterExpression) or (type(left_operand) is FilterField and type(right_operand) is not FilterExpression), "Both left and right operands must either be of type 'FilterExpression' or the left operand should be of type 'FilterField' and right operand should not be of type 'FilterExpression'"

        self.left_operand = left_operand
        self.right_operand = right_operand
        self.operation = operation

    @abstractmethod
    def _compile_base_case(self, field_name: str, field_values: Union[TField, List[TField]]) -> TCompileResult:
        """
         stuff
        """

    @abstractmethod
    def _combine_results(self, compiled_result_a: TCompileResult, compiled_result_b: TCompileResult, op: FilterExpressionOperation) -> TCompileResult:
        """
         stuff
        """

    def compile(self):

        if type(self.left_operand) is FilterField:
            return self._compile_base_case(self.left_operand.field_name, self.right_operand)

        left_compiled = self.left_operand.compile()
        right_compiled = self.right_operand.compile()

        return self._combine_results(left_compiled, right_compiled, self.operation)


    def __and__(self, other: "FilterExpression") -> "FilterExpression":
        return FilterExpression(left_operand=self, right_operand=other, operation=FilterExpressionOperation.AND)

    def __or__(self, other: "FilterExpression") -> "FilterExpression":
        return FilterExpression(left_operand=self, right_operand=other, operation=FilterExpressionOperation.OR)


@final
class AstraFilterExpression(FilterExpression[List[Dict[str, Any]]]):
    def _compile_base_case(self, field_name: str, field_values: Union[TField, List[TField]]) -> TCompileResult:
        pass

    def _combine_results(self, compiled_result_a: TCompileResult, compiled_result_b: TCompileResult,
                         op: FilterExpressionOperation) -> TCompileResult:
        pass


@final
class ArrowFilterExpression(FilterExpression[pyarrow.compute.Expression]):

    def _compile_base_case(self, field_name: str, field_values: Union[TField, List[TField]]) -> TCompileResult:
        pass

    def _combine_results(self, compiled_result_a: TCompileResult, compiled_result_b: TCompileResult,
                         op: FilterExpressionOperation) -> TCompileResult:
        pass

def compile_expression(expr: FilterExpression, target: Type[FilterExpression[TCompileResult]]) -> TCompileResult:
    pass


e = (FilterField("test") == FilterField("test2"))
astra_result = compile_expression(e, AstraFilterExpression)
arrow_result = compile_expression(e, ArrowFilterExpression)



@final
class AstraFilterExpressionCompiler(FilterExpressionCompiler[List[Dict[str, Any]]]):
    """
    Translates a FilterExpression into Astra expression.
    """

    def compile(self, expression: FilterExpression) -> List[Dict[str, Any]]:
        def compile_expression(right_op: FilterExpression, left_op: FilterExpression, op: FilterExpressionOperation) -> List[Dict[str, Any]]:
            return [{f"{left_op.field_name}__{op.name}": right_op.}]


        left = expression.left_operand
        right = expression.right_operand
        operation = expression.operation
        if operation == FilterExpressionOperation.EQ:
            return self.compile_equality_expression(right, left)
        if operation == FilterExpressionOperation.IN:
            return self.compile_isin_expression(right, left)
        if operation == FilterExpressionOperation.AND:
            return self.compile_and_expression(right, left)
        if operation == FilterExpressionOperation.OR:
            return self.compile_or_expression(right, left)

        func = (
            f"{operation.name.lower()[0]}" + "t" + f"{operation.name.lower()[1]}"
            if operation in (FilterExpressionOperation.LE, FilterExpressionOperation.GE)
            else operation.name.lower()
        )
        if isinstance(right, list):
            return [{f"{left.field_name}__{func}": right[0]}]
        return [{f"{left.field_name}__{func}": right}]

    @staticmethod
    def compile_equality_expression(right, left):
        """
        Compiles an equality expression into a dictionary that can be used to filter astra data.
        """
        if isinstance(right, list):
            return [{f"{left.field_name}": right[0]}]
        return [{left.field_name: right}]

    @staticmethod
    def compile_isin_expression(right, left):
        """
        Compiles an 'isin' expression into a dictionary that can be used to filter astra data.
        """
        return [{f"{left.field_name}__in": right}]

    @staticmethod
    def compile_and_expression(right, left):
        """
        Compiles an AND expression into a dictionary that can be used to filter astra data.
        """
        left_result = AstraFilterExpressionCompiler().compile(left)
        right_result = AstraFilterExpressionCompiler().compile(right)
        return [{**d1, **d2} for d1, d2 in zip(left_result, right_result)]

    @staticmethod
    def compile_or_expression(right, left):
        """
        Compiles an OR expression into a dictionary that can be used to filter astra data.
        """
        if right.operation == FilterExpressionOperation.AND:
            right_side = AstraFilterExpressionCompiler().compile(right)
            left_side = AstraFilterExpressionCompiler().compile(left)

            # Extract the last item from the right_side dictionary
            key = list(right_side[0])[-1]
            value = right_side[0][key]

            # Add the key-value pair to the left_side dictionary
            left_side[0].update({key: value})

            # Concatenate the left_side and right_side lists
            return left_side + right_side

        # Compile both sides separately and concatenate the results
        return AstraFilterExpressionCompiler().compile(left) + AstraFilterExpressionCompiler().compile(right)


@final
class ArrowExpressionCompiler(FilterExpressionCompiler[pc.Expression]):
    """
    Translates a FilterExpression into a PyArrow expression.
    """

    def compile(self, expression: FilterExpression) -> pc.Expression:
        """
        Compiles a FilterExpression into a PyArrow expression that can be used to filter data.
        """
        operation = expression.operation

        if operation == FilterExpressionOperation.IN:
            # Compile an "isin" expression for the IN operator
            return pyarrow_field(expression.left_operand.field_name).isin(expression.right_operand)
        if operation in (FilterExpressionOperation.AND, FilterExpressionOperation.OR):
            # Compile a logical operator expression for 'AND' or 'OR'
            op_func = getattr(operator, expression.operation.name.lower() + "_")
            return op_func(
                ArrowExpressionCompiler().compile(expression.left_operand), ArrowExpressionCompiler().compile(expression.right_operand)
            )

        # For other operators, compile a binary operator expression
        op_func = getattr(operator, expression.operation.name.lower())
        if isinstance(expression.right_operand, list):
            return op_func(pyarrow_field(expression.left_operand.field_name), expression.right_operand[0])
        # This is needed for compiling combined expressions
        return op_func(pyarrow_field(expression.left_operand.field_name), expression.right_operand)
