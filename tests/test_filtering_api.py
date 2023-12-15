from functools import reduce

import pytest

from adapta.storage.models.filter_expression import (
    FilterField,
    FilterExpression,
    ArrowFilterExpression,
    AstraFilterExpression,
    compile_expression,
)

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
        (FilterField(TEST_ENTITY_SCHEMA.col_a) == "test", (pyarrow_field("col_a") == "test"), [{"col_a": "test"}]),
        (
            FilterField(TEST_ENTITY_SCHEMA.col_a) >= "test",
            (pyarrow_field("col_a") >= "test"),
            [{"col_a__gte": "test"}],
        ),
        (FilterField(TEST_ENTITY_SCHEMA.col_a) > "test", (pyarrow_field("col_a") > "test"), [{"col_a__gt": "test"}]),
        (FilterField(TEST_ENTITY_SCHEMA.col_a) < "test", (pyarrow_field("col_a") < "test"), [{"col_a__lt": "test"}]),
        (
            FilterField(TEST_ENTITY_SCHEMA.col_a) <= "test",
            (pyarrow_field("col_a") <= "test"),
            [{"col_a__lte": "test"}],
        ),
        (
            FilterField(TEST_ENTITY_SCHEMA.col_a).isin(["val1", "val2"]),
            (pyarrow_field("col_a").isin(["val1", "val2"])),
            [{"col_a__in": ["val1", "val2"]}],
        ),
        (
            (FilterField(TEST_ENTITY_SCHEMA.col_a) == "test") & (FilterField(TEST_ENTITY_SCHEMA.col_b) == "other"),
            (pyarrow_field("col_a") == "test") & (pyarrow_field("col_b") == "other"),
            [{"col_a": "test", "col_b": "other"}],
        ),
        (
            (FilterField(TEST_ENTITY_SCHEMA.col_a) == "test") & (FilterField(TEST_ENTITY_SCHEMA.col_c).isin([1, 2, 3])),
            ((pyarrow_field("col_a") == "test") & (pyarrow_field("col_c").isin([1, 2, 3]))),
            [{"col_a": "test", "col_c__in": [1, 2, 3]}],
        ),
        (
            (FilterField(TEST_ENTITY_SCHEMA.col_a) == "test") | (FilterField(TEST_ENTITY_SCHEMA.col_b) == "other"),
            (pyarrow_field("col_a") == "test") | (pyarrow_field("col_b") == "other"),
            [{"col_a": "test"}, {"col_b": "other"}],
        ),
        (
            (FilterField(TEST_ENTITY_SCHEMA.col_a) == "test")
            | (FilterField(TEST_ENTITY_SCHEMA.col_b) == "other")
            | (FilterField(TEST_ENTITY_SCHEMA.col_c) == 1),
            ((pyarrow_field("col_a") == "test") | (pyarrow_field("col_b") == "other") | (pyarrow_field("col_c") == 1)),
            [{"col_a": "test"}, {"col_b": "other"}, {"col_c": 1}],
        ),
        (
            ((FilterField(TEST_ENTITY_SCHEMA.col_a) == "test") | (FilterField(TEST_ENTITY_SCHEMA.col_b) == "other"))
            & (FilterField(TEST_ENTITY_SCHEMA.col_c) == 1),
            ((pyarrow_field("col_a") == "test") | (pyarrow_field("col_b") == "other")) & (pyarrow_field("col_c") == 1),
            [{"col_a": "test", "col_c": 1}, {"col_b": "other", "col_c": 1}],
        ),
        (
            (FilterField(TEST_ENTITY_SCHEMA.col_a) == "test") & (FilterField(TEST_ENTITY_SCHEMA.col_c) == 1)
            | (FilterField(TEST_ENTITY_SCHEMA.col_b) == "other"),
            ((pyarrow_field("col_a") == "test") & (pyarrow_field("col_c") == 1) | (pyarrow_field("col_b") == "other")),
            [{"col_a": "test", "col_c": 1}, {"col_b": "other"}],
        ),
        (
            (
                (FilterField(TEST_ENTITY_SCHEMA.col_a) == "test") & (FilterField(TEST_ENTITY_SCHEMA.col_c) == 1)
                | (FilterField(TEST_ENTITY_SCHEMA.col_b) == "other")
                & ((FilterField(TEST_ENTITY_SCHEMA.col_c) == 2) | (FilterField(TEST_ENTITY_SCHEMA.col_d).isin([1, 2])))
            ),
            (
                (pyarrow_field("col_a") == "test") & (pyarrow_field("col_c") == 1)
                | (pyarrow_field("col_b") == "other")
                & ((pyarrow_field("col_c") == 2) | (pyarrow_field("col_d").isin([1, 2])))
            ),
            ([{"col_a": "test", "col_c": 1}, {"col_b": "other", "col_c": 2}, {"col_b": "other", "col_d__in": [1, 2]}]),
        ),
    ],
)
def test_generic_filtering(
    filter_expr: Union[FilterField, FilterExpression],
    pyarrow_expected_expr: pc.Expression,
    astra_expected_expr: Dict[str, Any],
):
    assert compile_expression(filter_expr, ArrowFilterExpression).equals(pyarrow_expected_expr)
    assert compile_expression(filter_expr, AstraFilterExpression) == astra_expected_expr


@pytest.mark.parametrize(
    "filter_expr, expected_output",
    [
        (FilterField(TEST_ENTITY_SCHEMA.col_a) == "test", "col_a == test"),
        (FilterField(TEST_ENTITY_SCHEMA.col_a) > "test", "col_a > test"),
        (FilterField(TEST_ENTITY_SCHEMA.col_a) >= "test", "col_a >= test"),
        (FilterField(TEST_ENTITY_SCHEMA.col_a) < "test", "col_a < test"),
        (FilterField(TEST_ENTITY_SCHEMA.col_a) <= "test", "col_a <= test"),
        (
            (
                (FilterField(TEST_ENTITY_SCHEMA.col_a) == "test") & (FilterField(TEST_ENTITY_SCHEMA.col_c) == 1)
                | (FilterField(TEST_ENTITY_SCHEMA.col_b) == "other")
                & ((FilterField(TEST_ENTITY_SCHEMA.col_c) == 2) | (FilterField(TEST_ENTITY_SCHEMA.col_d).isin([1, 2])))
            ),
            "((col_a == test) AND (col_c == 1)) OR ((col_b == other) AND ((col_c == 2) OR (col_d IN [1, 2])))",
        ),
    ],
)
def test_print_filter_expression(filter_expr: FilterExpression, expected_output: str):
    assert str(filter_expr) == expected_output


@pytest.mark.parametrize(
    "filter_expr, pyarrow_expected_expr, astra_expected_expr",
    [
        (
            (FilterField(TEST_ENTITY_SCHEMA.col_d).isin([str(i) for i in range(1, 28)])),
            ((pyarrow_field("col_d").isin([str(i) for i in range(1, 28)]))),
            (
                [
                    {"col_d__in": [str(i) for i in range(1, 15)]},
                    {"col_d__in": [str(i) for i in range(15, 28)]},
                ]
            ),
        ),
        (
            (FilterField(TEST_ENTITY_SCHEMA.col_d).isin([str(i) for i in range(1, 26)])),
            (pyarrow_field("col_d").isin([str(i) for i in range(1, 26)])),
            (
                [
                    {"col_d__in": [str(i) for i in range(1, 26)]},
                ]
            ),
        ),
        (
            (FilterField(TEST_ENTITY_SCHEMA.col_d).isin([str(i) for i in range(1, 101)])),
            ((pyarrow_field("col_d").isin([str(i) for i in range(1, 101)]))),
            (
                [
                    {"col_d__in": [str(i) for i in range(1, 26)]},
                    {"col_d__in": [str(i) for i in range(26, 51)]},
                    {"col_d__in": [str(i) for i in range(51, 76)]},
                    {"col_d__in": [str(i) for i in range(76, 101)]},
                ]
            ),
        ),
    ],
)
def test_long_is_in_list(
    filter_expr: Union[FilterField, FilterExpression],
    pyarrow_expected_expr: pc.Expression,
    astra_expected_expr: Dict[str, Any],
):
    assert compile_expression(filter_expr, ArrowFilterExpression).equals(pyarrow_expected_expr)
    assert compile_expression(filter_expr, AstraFilterExpression) == astra_expected_expr


@pytest.mark.parametrize(
    "filter_expr",
    [
        reduce(
            lambda x, y: x | y,
            [
                (FilterField(TEST_ENTITY_SCHEMA.col_d).isin([col_d]))
                & (FilterField(TEST_ENTITY_SCHEMA.col_a) == col_a)
                & (FilterField(TEST_ENTITY_SCHEMA.col_b) == col_b)
                for col_a in [str(i) for i in range(1, 30)]
                for col_b in [str(i) for i in range(1, 50)]
                for col_d in [str(i) for i in range(1, 150)]
            ],
        )
    ],
)
def test_large_filter(filter_expr: Union[FilterField, FilterExpression]):
    try:
        compile_expression(filter_expr, AstraFilterExpression)
    except RecursionError as re:
        assert False, f"Raised RecursionError for large filters"
