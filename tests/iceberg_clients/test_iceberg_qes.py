import polars
import pytest
import sqlalchemy
from polars.testing import assert_frame_equal

from adapta.storage.models import parse_data_path
from adapta.storage.models.expression_dsl.filter_expression import FilterField, Expression
from adapta.storage.query_enabled_store import IcebergQueryEnabledStore, IcebergSettings, IcebergCredential
from tests.iceberg_clients._functions import get_input_data, prepare_iceberg_table

_qes_input_data = get_input_data() | {"cold": [-1, 1, 2, -3, 0, 5, 6, 10, -5, 2]}
_qes_input = polars.DataFrame(_qes_input_data)


@pytest.mark.parametrize(
    "table_id, expr, column_selector, limit, expected",
    [
        (
            "isin_range",
            FilterField("cola").isin([1, 2]),
            list(),
            None,
            _qes_input.filter(polars.col("cola").is_in([1, 2])),
        ),
        (
            "equal",
            FilterField("cola") == 5,
            list(),
            None,
            _qes_input.filter(polars.col("cola") == 5),
        ),
        (
            "two_expressions",
            (FilterField("cola") > 5) & (FilterField("cold") > 0),
            list(),
            None,
            _qes_input.filter((polars.col("cola") > 5) & (polars.col("cold") > 0)),
        ),
        (
            "expression_and_column_selector",
            FilterField("cola") > 5,
            ["cola", "colb"],
            None,
            _qes_input.filter((polars.col("cola") > 5)).select(polars.col("cola"), polars.col("colb")),
        ),
    ],
)
def test_iceberg_qes(
    table_id: str,
    expr: Expression,
    column_selector: list[str],
    limit: int | None,
    expected: polars.DataFrame,
    trino_test_connection: sqlalchemy.engine.Engine,
):
    table_name = f"qes_test_{table_id}"
    prepare_iceberg_table(
        table_name,
        data=_qes_input_data,
        trino_test_connection=trino_test_connection,
    )
    store = IcebergQueryEnabledStore(
        settings=IcebergSettings(
            lazy_read=False,
        ),
        credentials=IcebergCredential(oauth_enabled=False),
    )._init_catalog()

    data = store.open(parse_data_path(f"iceberg://test@{table_name}")).select(*column_selector).filter(expr).read()
    assert_frame_equal(data.to_polars().sort("cola"), expected.sort("cola"), check_column_order=False)
