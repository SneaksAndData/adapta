#  Copyright (c) 2023-2024. ECCO Sneaks & Data
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import pytest
from typing import Type
import pandas
import polars
from adapta.storage.models.format import (
    DictJsonSerializationFormat,
    SerializationFormat,
    DataFrameParquetSerializationFormat,
    DataFrameJsonSerializationFormat,
    DataFrameCsvSerializationFormat,
    UnitSerializationFormat,
    PickleSerializationFormat,
    PolarsDataFrameJsonSerializationFormat,
    PolarsDataFrameCsvSerializationFormat,
    PolarsDataFrameParquetSerializationFormat,
)


@pytest.mark.parametrize(
    "serializer, data",
    [
        (DictJsonSerializationFormat, {"test": "test"}),
        (
            DataFrameParquetSerializationFormat,
            pandas.DataFrame(data={"test": [1, 2, 3]}),
        ),
        (DataFrameJsonSerializationFormat, pandas.DataFrame(data={"test": [1, 2, 3]})),
        (DataFrameCsvSerializationFormat, pandas.DataFrame(data={"test": [1, 2, 3]})),
        (
            PolarsDataFrameParquetSerializationFormat,
            polars.DataFrame(data={"test": [1, 2, 3]}),
        ),
        (
            PolarsDataFrameCsvSerializationFormat,
            polars.DataFrame(data={"test": [1, 2, 3]}),
        ),
        (
            PolarsDataFrameJsonSerializationFormat,
            polars.DataFrame(data={"test": [1, 2, 3]}),
        ),
        (PickleSerializationFormat, pandas.DataFrame(data={"test": [1, 2, 3]})),
        (PickleSerializationFormat, [1, 2, 3]),
        (PickleSerializationFormat, {"foo": "bar"}),
        (PickleSerializationFormat, "Hello, World!"),
        (PickleSerializationFormat, b"Test string"),
        (UnitSerializationFormat, b"Test string"),
    ],
)
def test_unit_serialization(serializer: Type[SerializationFormat], data: any):
    """
    Tests that serializing and then immediately deserializing any data equals the original data.
    """
    if isinstance(data, pandas.DataFrame) | isinstance(data, polars.DataFrame):
        assert data.equals(serializer().deserialize(serializer().serialize(data)))
    else:
        assert data == serializer().deserialize(serializer().serialize(data))
