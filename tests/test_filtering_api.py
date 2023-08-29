import pytest

from adapta.storage.distributed_object_store.datastax_astra import AstraField, AstraFilterExpression
from adapta.schema_management.schema_entity import PythonSchemaEntity

from dataclasses import dataclass, field
from typing import List, Any, Dict, Union


@dataclass
class TestEntity:
    col_a: str = field(metadata={"is_primary_key": True, "is_partition_key": True})
    col_b: str = field(metadata={"is_primary_key": True, "is_partition_key": False})
    col_c: int
    col_d: List[int]


TEST_ENTITY_SCHEMA: TestEntity = PythonSchemaEntity(TestEntity)


@pytest.mark.parametrize(
    "filter_expression,expected_expression",
    [
        (AstraField(TEST_ENTITY_SCHEMA.col_a) == "test", {"col_a": "test"}),
        (AstraField(TEST_ENTITY_SCHEMA.col_a) >= "test", {"col_a__gte": "test"}),
        (AstraField(TEST_ENTITY_SCHEMA.col_a) > "test", {"col_a__gt": "test"}),
        (AstraField(TEST_ENTITY_SCHEMA.col_a) < "test", {"col_a__lt": "test"}),
        (AstraField(TEST_ENTITY_SCHEMA.col_a) <= "test", {"col_a__lte": "test"}),
        (AstraField(TEST_ENTITY_SCHEMA.col_a).isin(["val1", "val2"]), {"col_a__in": ["val1", "val2"]}),
        (
            (AstraField(TEST_ENTITY_SCHEMA.col_a) == "test") & (AstraField(TEST_ENTITY_SCHEMA.col_b) == "other"),
            [{"col_a": "test", "col_b": "other"}],
        ),
        (
            (AstraField(TEST_ENTITY_SCHEMA.col_a) == "test") & (AstraField(TEST_ENTITY_SCHEMA.col_c).isin([1, 2, 3])),
            [{"col_a": "test", "col_c__in": [1, 2, 3]}],
        ),
        (
            (AstraField(TEST_ENTITY_SCHEMA.col_a) == "test") | (AstraField(TEST_ENTITY_SCHEMA.col_b) == "other"),
            [{"col_a": "test"}, {"col_b": "other"}],
        ),
        (
            (AstraField(TEST_ENTITY_SCHEMA.col_a) == "test")
            | (AstraField(TEST_ENTITY_SCHEMA.col_b) == "other")
            | (AstraField(TEST_ENTITY_SCHEMA.col_c) == 1),
            [{"col_a": "test"}, {"col_b": "other"}, {"col_c": 1}],
        ),
    ],
)
def test_astra_filters(
    filter_expression: Union[AstraField, AstraFilterExpression], expected_expression: Dict[str, Any]
):
    assert filter_expression.expression == expected_expression
