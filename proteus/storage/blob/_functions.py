"""
 Utility functions for storage operations.
"""
import json
from typing import Union

import pandas


def pandas_to_parquet(dataframe: pandas.DataFrame) -> bytes:
    """
     Converts pandas DataFrame to parquet bytestream.

    :param dataframe: Pandas DataFrame.
    :return: Byte array.
    """
    return dataframe.to_parquet()


def json_to_bytes(data: Union[dict, list]) -> bytes:
    """
      Converts dict or list to json-serialized byte array.

    :param data: Dictionary or list to serialize.
    :return: Byte array.
    """
    return json.dumps(data).encode(encoding='utf-8')
