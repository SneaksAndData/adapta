from dataclasses import dataclass

import pytest

from adapta.schema_management.schema_entity import PythonSchemaEntity


@dataclass
class TestSchema:
    col_a: str
    col_b: float
    col_c: int
    col_d: object

@pytest.mark.parametrize(
    "SCHEMA, columns",
    [
        PythonSchemaEntity(TestSchema),
        ["col_a", "col_b", "col_c", "col_d"],
    ],
)
def test_generic_filtering(
    SCHEMA, columns: List[str]
):
    assert SCHEMA.get_field_names() == columns