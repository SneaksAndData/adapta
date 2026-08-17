import polars
import pytest
import sqlalchemy
from polars.testing import assert_frame_equal

from adapta.storage.models import parse_data_path
from adapta.storage.models.expression_dsl.filter_expression import FilterField, Expression
from adapta.utils.metaframe import MetaFrame
from adapta.storage.query_enabled_store import (
    QueryEnabledStore,
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
    store = QueryEnabledStore.from_string(
        'qes://engine=ICEBERG;plaintext_credentials={"oauth_enabled": false};settings={"lazy_read": false}'
    )

    data = (
        store.open(parse_data_path(f"iceberg://test@{table_name}"), access_mode=QueryEnabledStoreMode.READ)
        .set_parameters(
            QueryEnabledStoreSelectParameter(column_selector),
            QueryEnabledStoreFilterParameter(expr),
        )
        .execute()
    )
    assert_frame_equal(data.to_polars().sort("cola"), expected.sort("cola"), check_column_order=False)


@pytest.mark.parametrize(
    "dataset1, dataset2, overwrite, expected",
    [
        (
            polars.DataFrame({"cola": [1, 2, 3], "colb": ["a", "b", "c"]}),
            polars.DataFrame({"cola": [4, 5], "colb": ["d", "e"]}),
            True,
            polars.DataFrame({"cola": [4, 5], "colb": ["d", "e"]}),
        ),
        (
            polars.DataFrame({"cola": [1, 2, 3], "colb": ["a", "b", "c"]}),
            polars.DataFrame({"cola": [4, 5], "colb": ["d", "e"]}),
            False,
            polars.concat(
                [
                    polars.DataFrame({"cola": [1, 2, 3], "colb": ["a", "b", "c"]}),
                    polars.DataFrame({"cola": [4, 5], "colb": ["d", "e"]}),
                ]
            ),
        ),
    ],
)
def test_iceberg_qes_write(
    dataset1: polars.DataFrame, dataset2: polars.DataFrame, overwrite: bool, expected: polars.DataFrame
):
    table_name = f"qes_test_write_{generate_random_string(8)}".lower()

    store = QueryEnabledStore.from_string(
        'qes://engine=ICEBERG;plaintext_credentials={"oauth_enabled": false};settings={"lazy_read": false}'
    )

    # First write (Create table)
    store.open(parse_data_path(f"iceberg://test@{table_name}"), access_mode=QueryEnabledStoreMode.WRITE).set_parameters(
        QueryEnabledStoreDataParameter(MetaFrame.from_polars(dataset1)),
        QueryEnabledStoreOverwriteParameter(True),
        QueryEnabledStoreBlockSizeParameter(10_000),
    ).execute()

    # Second write (Overwrite or Append)
    store.open(parse_data_path(f"iceberg://test@{table_name}"), access_mode=QueryEnabledStoreMode.WRITE).set_parameters(
        QueryEnabledStoreDataParameter(MetaFrame.from_polars(dataset2)),
        QueryEnabledStoreOverwriteParameter(overwrite),
        QueryEnabledStoreBlockSizeParameter(10_000),
    ).execute()

    # Read back and assert
    data = store.open(parse_data_path(f"iceberg://test@{table_name}"), access_mode=QueryEnabledStoreMode.READ).execute()
    assert_frame_equal(data.to_polars().sort("cola"), expected.sort("cola"), check_column_order=False)
