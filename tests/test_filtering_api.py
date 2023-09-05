import pytest

from adapta.storage.models.filter_expression import FilterField, FilterExpression, AstraFilterExpressionCompiler, \
    ArrowExpressionCompiler

from adapta.schema_management.schema_entity import PythonSchemaEntity

from dataclasses import dataclass, field
from typing import List, Any, Dict, Union
from pyarrow.dataset import field as pyarrow_field
import pyarrow.compute as pc


@dataclass
class TestEntity:
    col_a: str = field(metadata={"is_primary_key": True, "is_partition_key": True})
    col_b: str = field(metadata={"is_primary_key": True, "is_partition_key": False})
    col_c: int
    col_d: List[int]


TEST_ENTITY_SCHEMA: TestEntity = PythonSchemaEntity(TestEntity)

@pytest.mark.parametrize(
    "filter_expr, pyarrow_expected_expr, astra_expected_expr",
    [
        (FilterField(TEST_ENTITY_SCHEMA.col_a) == ["test"], (pyarrow_field("col_a") == "test"), [{"col_a": "test"}]),
        (FilterField(TEST_ENTITY_SCHEMA.col_a) >= ["test"], (pyarrow_field("col_a") >= "test"),
         [{"col_a__gte": "test"}]),
        (FilterField(TEST_ENTITY_SCHEMA.col_a) > ["test"], (pyarrow_field("col_a") > "test"), [{"col_a__gt": "test"}]),
        (FilterField(TEST_ENTITY_SCHEMA.col_a) < ["test"], (pyarrow_field("col_a") < "test"), [{"col_a__lt": "test"}]),
        (FilterField(TEST_ENTITY_SCHEMA.col_a) <= ["test"], (pyarrow_field("col_a") <= "test"),
         [{"col_a__lte": "test"}]),
        (FilterField(TEST_ENTITY_SCHEMA.col_a).isin(["val1", "val2"]), (pyarrow_field("col_a").isin(["val1", "val2"])),
        [{"col_a__in": ["val1", "val2"]}]),
        (
                (FilterField(TEST_ENTITY_SCHEMA.col_a) == "test") & (FilterField(TEST_ENTITY_SCHEMA.col_b) == "other"),
                (pyarrow_field("col_a") == "test") & (pyarrow_field("col_b") == "other"),
                [{"col_a": "test", "col_b": "other"}],
        ),
        (
                (FilterField(TEST_ENTITY_SCHEMA.col_a) == ["test"]) &
                (FilterField(TEST_ENTITY_SCHEMA.col_c).isin([1, 2, 3])),
                ((pyarrow_field("col_a") == "test") & (pyarrow_field("col_c").isin([1, 2, 3]))),
                [{"col_a": "test", "col_c__in": [1, 2, 3]}],
        ),
        (
                (FilterField(TEST_ENTITY_SCHEMA.col_a) == "test") | (FilterField(TEST_ENTITY_SCHEMA.col_b) == "other"),
                (pyarrow_field("col_a") == "test") | (pyarrow_field("col_b") == "other"),
                [{"col_a": "test"}, {"col_b": "other"}],
        ),
        (
                (FilterField(TEST_ENTITY_SCHEMA.col_a) == ["test"])
                | (FilterField(TEST_ENTITY_SCHEMA.col_b) == ["other"])
                | (FilterField(TEST_ENTITY_SCHEMA.col_c) == [1]),
                ((pyarrow_field("col_a") == "test") | (pyarrow_field("col_b") == "other") | (
                        pyarrow_field("col_c") == 1)),
                [{"col_a": "test"}, {"col_b": "other"}, {"col_c": 1}],
        ),
        (
                (FilterField(TEST_ENTITY_SCHEMA.col_a) == ["test"])
                | (FilterField(TEST_ENTITY_SCHEMA.col_b) == ["other"])
                & (FilterField(TEST_ENTITY_SCHEMA.col_c) == [1]),
                ((pyarrow_field("col_a") == "test") | (pyarrow_field("col_b") == "other") & (
                        pyarrow_field("col_c") == 1)),
                [{"col_a": "test", "col_c": 1}, {"col_b": "other", "col_c": 1}],
        ),
    ]
)
def test_generic_filtering(
        filter_expr: Union[FilterField, FilterExpression], pyarrow_expected_expr: pc.Expression,
        astra_expected_expr: Dict[str, Any]):
    assert ArrowExpressionCompiler().compile(filter_expr).equals(pyarrow_expected_expr)
    assert AstraFilterExpressionCompiler().compile(filter_expr) == astra_expected_expr
