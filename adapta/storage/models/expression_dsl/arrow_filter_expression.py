from typing import final, Any

import pyarrow.compute
from pyarrow.compute import field as pyarrow_field

from adapta.storage.models.expression_dsl.filter_expression import (
    FilterExpression,
    FilterExpressionOperation,
    TCompileResult,
)


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
