import datetime
from typing import Any, final

from adapta.storage.models.expression_dsl.filter_expression import FilterExpression, FilterExpressionOperation


@final
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
        op_str = operation.value["trino"]
        return f"{field_name} {op_str} {self._format_value(field_values)}"

    def _combine_results(
        self, compiled_result_a: str, compiled_result_b: str, operation: FilterExpressionOperation
    ) -> str:
        op_str = operation.value["trino"]
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
