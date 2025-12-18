"""
 Models for Astra DB.
"""
#  Copyright (c) 2023-2026. ECCO Data & AI and other project contributors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import re

from enum import Enum
from typing import Any

from adapta.storage.models.filter_expression import (
    Expression,
    compile_expression,
    AstraFilterExpression,
    FilterExpressionOperation,
)


class SimilarityFunction(Enum):
    """
    Supported similarity functions.
    """

    COSINE = "similarity_cosine"
    DOT_PRODUCT = "similarity_dot_product"
    EUCLIDEAN = "similarity_euclidean"


class VectorSearchQuery:
    """
    Builder for a vector search query. Result would resemble the following:

    select description, similarity_cosine(item_vector, [0.1, 0.15, 0.3, 0.12, 0.05])
    from vsearch.products

    order by item_vector ann of [0.1, 0.15, 0.3, 0.12, 0.05]
    limit 1;

    """

    def __init__(
        self,
        table_fqn: str,
        data_fields: list[str],
        sim_func: SimilarityFunction,
        vector: list[float],
        field_name: str,
        num_results=1,
        key_column_filter_values: Expression | list[dict[str, Any]] | None = None,
    ):
        self._sim_func = sim_func
        self._vector = vector
        self._field_name = field_name
        self._num_results = num_results
        self._table_fqn = table_fqn
        self._data_fields = data_fields
        self._key_column_filter_values = key_column_filter_values

    def _get_similarity_colum(self) -> str:
        return f"{self._sim_func.value}({self._field_name}, {self._vector})"

    def _get_order_by(self) -> str:
        return f"order by {self._field_name} ann of {self._vector} limit {self._num_results};"

    def _get_filter(self) -> str:
        if self._key_column_filter_values is None:
            return ""

        def format_value_for_cql(val: Any) -> str:
            if isinstance(val, (tuple, list, set)):
                return f"({', '.join(format_value_for_cql(v) for v in val)})"
            if isinstance(val, str):
                return f"'{val}'"
            return str(val)

        def get_astra_operator(column_expression: str) -> str:
            operator_map = {
                FilterExpressionOperation.GT.value["astra"]: ">",
                FilterExpressionOperation.GE.value["astra"]: ">=",
                FilterExpressionOperation.LT.value["astra"]: "<",
                FilterExpressionOperation.LE.value["astra"]: "<=",
                FilterExpressionOperation.IN.value["astra"]: "IN",
            }

            for suffix, operator in operator_map.items():
                if column_expression.endswith(suffix):
                    return operator

            return "="

        def remove_operator_suffix(col: str) -> str:
            """
            Removes the double underscore and the subsequent operator suffix from a column expression.
            Returns only the column name.
            """
            return re.sub(r"__\w+$", "", col)

        compiled_filter_values = (
            compile_expression(self._key_column_filter_values, AstraFilterExpression)
            if isinstance(self._key_column_filter_values, Expression)
            else self._key_column_filter_values
        )
        if len(compiled_filter_values) > 1:
            raise ValueError("Restriction on key columns must not be nested under OR operator")

        cql_filter_expressions = [
            f"{remove_operator_suffix(col)} {get_astra_operator(col)} {format_value_for_cql(val)}"
            for col, val in compiled_filter_values[0].items()
        ]
        return f"where {' and '.join(cql_filter_expressions)}"

    def __str__(self):
        return " ".join(
            [
                "select",
                ", ".join(self._data_fields),
                ", " f"{self._get_similarity_colum()} as sim_value",
                f"from {self._table_fqn}",
                self._get_filter(),
                self._get_order_by(),
            ]
        )
