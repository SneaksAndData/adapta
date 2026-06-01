import random
import string
from typing import Any

import pandas
import sqlalchemy
from sqlalchemy import ARRAY, BIGINT


def prepare_iceberg_table(
    name: str, data: dict[str, list[Any]], trino_test_connection: sqlalchemy.engine.Engine
) -> None:
    """
    Writes a provided dataset to a table named {name}
    """
    pandas.DataFrame(data).to_sql(
        name=name,
        schema="test",
        con=trino_test_connection,
        if_exists="replace",
        index=False,
        dtype={"colc": ARRAY(BIGINT)},
    )


def generate_random_strings(list_size: int, string_length: int):
    """
    Generate a list of random strings
    """
    pool = string.ascii_letters + string.digits
    return ["".join(random.choices(pool, k=string_length)) for _ in range(list_size)]


def get_input_data():
    """
    Generate input data
    """
    return {
        "cola": list(range(10)),
        "colb": list(generate_random_strings(10, 10)),
        "colc": list([list(range(random.randint(1, 10))) for _ in range(10)]),
    }
