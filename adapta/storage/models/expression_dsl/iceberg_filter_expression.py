from typing import final, Any
import pyiceberg.expressions
from pyiceberg.expressions import BooleanExpression

from adapta.storage.models.expression_dsl.filter_expression import (
    FilterExpression,
    FilterExpressionOperation,
)


@final
class IcebergFilterExpression(FilterExpression[BooleanExpression]):
    def _compile_base_case(
        self, field_name: str, field_values: Any, operation: FilterExpressionOperation
    ) -> BooleanExpression:
        return operation.value["iceberg"](field_name, field_values)

    def _combine_results(
        self,
        compiled_result_a: BooleanExpression,
        compiled_result_b: BooleanExpression,
        operation: FilterExpressionOperation,
    ) -> BooleanExpression:
        return operation.value["iceberg"](compiled_result_a, compiled_result_b)
