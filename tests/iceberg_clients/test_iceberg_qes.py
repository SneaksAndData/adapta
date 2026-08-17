import polars
import pytest
import sqlalchemy
from polars.testing import assert_frame_equal

from adapta.storage.models import parse_data_path
from adapta.storage.models.expression_dsl.filter_expression import FilterField, Expression
from adapta.utils.metaframe import MetaFrame
from adapta.storage.query_enabled_store import (
    IcebergQueryEnabledStore,
    IcebergSettings,
    IcebergCredential,
    QueryEnabledStoreMode,
    QueryEnabledStoreSelectParameter,
    QueryEnabledStoreFilterParameter,
    QueryEnabledStoreDataParameter,
    QueryEnabledStoreOverwriteParameter,
    QueryEnabledStoreBlockSizeParameter,
)
from tests.iceberg_clients._functions import get_input_data, prepare_iceberg_table, generate_random_string

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

    data = (
        store.open(parse_data_path(f"iceberg://test@{table_name}"), access_mode=QueryEnabledStoreMode.READ)
        .set_parameters(
            QueryEnabledStoreSelectParameter(column_selector),
            QueryEnabledStoreFilterParameter(expr),
        )
        .execute()
    )
    assert_frame_equal(data.to_polars().sort("cola"), expected.sort("cola"), check_column_order=False)


def test_iceberg_qes_write():
    table_name = f"qes_test_write_{generate_random_string(8)}".lower()
    input_data1 = {
        "cola": [1, 2, 3],
        "colb": ["a", "b", "c"],
    }
    df1 = polars.DataFrame(input_data1)

    store = IcebergQueryEnabledStore(
        settings=IcebergSettings(
            lazy_read=False,
        ),
        credentials=IcebergCredential(oauth_enabled=False),
    )._init_catalog()

    # 1. Test Write with overwrite=True (Create)
    store.open(parse_data_path(f"iceberg://test@{table_name}"), access_mode=QueryEnabledStoreMode.WRITE).set_parameters(
        QueryEnabledStoreDataParameter(MetaFrame.from_polars(df1)),
        QueryEnabledStoreOverwriteParameter(True),
        QueryEnabledStoreBlockSizeParameter(10_000),
    ).execute()

    # Read back and assert
    data = store.open(parse_data_path(f"iceberg://test@{table_name}"), access_mode=QueryEnabledStoreMode.READ).execute()
    assert_frame_equal(data.to_polars().sort("cola"), df1.sort("cola"), check_column_order=False)

    # 2. Test Write with overwrite=False (Append)
    input_data2 = {
        "cola": [4, 5],
        "colb": ["d", "e"],
    }
    df2 = polars.DataFrame(input_data2)

    store.open(parse_data_path(f"iceberg://test@{table_name}"), access_mode=QueryEnabledStoreMode.WRITE).set_parameters(
        QueryEnabledStoreDataParameter(MetaFrame.from_polars(df2)),
        QueryEnabledStoreOverwriteParameter(False),
        QueryEnabledStoreBlockSizeParameter(10_000),
    ).execute()

    # Read back and assert total appended
    expected_df = polars.concat([df1, df2])
    data_appended = store.open(
        parse_data_path(f"iceberg://test@{table_name}"), access_mode=QueryEnabledStoreMode.READ
    ).execute()
    assert_frame_equal(data_appended.to_polars().sort("cola"), expected_df.sort("cola"), check_column_order=False)
