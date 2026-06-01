from typing import Any

import pandas
import sqlalchemy


def prepare_iceberg_table(
    name: str, data: dict[str, list[Any]], trino_test_connection: sqlalchemy.engine.Engine
) -> None:
    """
    Writes a provided dataset to a table named {name}
    """
    pandas.DataFrame(data).to_sql(
        name=name, schema="iceberg.test", con=trino_test_connection, if_exists="replace", index=False
    )
