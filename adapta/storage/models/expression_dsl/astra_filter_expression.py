import math
from typing import final, Any

from adapta.storage.models.expression_dsl.filter_expression import (
    FilterExpression,
    FilterExpressionOperation,
    TCompileResult,
)
from adapta.utils import chunk_list


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
