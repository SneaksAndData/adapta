"""
 Models for Astra DB.
"""
from enum import Enum


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
    ):
        self._sim_func = sim_func
        self._vector = vector
        self._field_name = field_name
        self._num_results = num_results
        self._table_fqn = table_fqn
        self._data_fields = data_fields

    def _get_similarity_colum(self) -> str:
        return f"{self._sim_func.value}({self._field_name}, {self._vector})"

    def _get_order_by(self) -> str:
        return f"order by {self._field_name} ann of {self._vector} limit {self._num_results};"

    def __str__(self):
        return " ".join(
            [
                "select",
                ", ".join(self._data_fields),
                ", " f"{self._get_similarity_colum()} as sim_value",
                f"from {self._table_fqn}",
                self._get_order_by(),
            ]
        )
