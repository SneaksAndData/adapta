import pytest
import sqlalchemy

from adapta.storage.models import parse_data_path
from adapta.storage.models.expression_dsl.filter_expression import FilterField, Expression
from adapta.storage.query_enabled_store import IcebergQueryEnabledStore, IcebergSettings, IcebergCredential
from tests.iceberg_clients._functions import get_input_data, prepare_iceberg_table


@pytest.mark.parametrize(
    "table_id, expr, column_selector, limit",
    [("isin_range", FilterField("cola").isin([[1, 2]]), list(), None)],
)
def test_iceberg_qes(
    table_id: str,
    expr: Expression,
    column_selector: list[str],
    limit: int | None,
    trino_test_connection: sqlalchemy.engine.Engine,
):
    input_data = get_input_data()
    table_name = f"qes_test_{table_id}"
    prepare_iceberg_table(
        table_name,
        data=input_data,
        trino_test_connection=trino_test_connection,
    )
    store = IcebergQueryEnabledStore(
        settings=IcebergSettings(
            lazy_read=False,
        ),
        credentials=IcebergCredential(oauth_enabled=False),
    )._init_catalog()

    x = store.open(parse_data_path(f"iceberg://test@{table_name}")).select(*column_selector).filter(expr).read()
    assert x is not None
