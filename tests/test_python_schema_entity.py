from dataclasses import dataclass, field

import pytest

from adapta.schema_management.schema_entity import PythonSchemaEntity


@dataclass
class TestSchema:
    col_a: str
    col_b: float
    col_c: int
    col_d: object


@dataclass
class ReviewTime:
    location_key: str = field(
        metadata={
            "display_name": "location Key",
            "description": "Unique identifier for location.",
            "is_primary_key": True,
            "is_partition_key": True,
        }
    )

    sku_key: str = field(
        metadata={
            "display_name": "sku key",
            "description": "Unique identifier of a sku_key",
            "is_primary_key": True,
            "is_partition_key": True,
        }
    )

    review_time: int = field(
        metadata={
            "display_name": "Review time",
            "description": "Review time found from auto replenishment.",
        }
    )


@pytest.mark.parametrize(
    "SCHEMA, columns",
    [
        PythonSchemaEntity(TestSchema),
        ["col_a", "col_b", "col_c", "col_d"],
    ],
    [
        PythonSchemaEntity(ReviewTime),
        ["location_key", "sku_key", "review_time"],
    ],
)
def test_get_field_names(SCHEMA, columns: list[str]):
    assert SCHEMA.get_field_names() == columns
