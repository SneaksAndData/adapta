#  Copyright (c) 2023-2026. ECCO Data & AI and other project contributors.
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
from copy import deepcopy

import pytest
import pandas
import polars
from adapta.storage.models.format import (
    SerializationFormat,
)
from adapta.storage.models.formatters import (
    DictJsonSerializationFormat,
    DictJsonSerializationFormatWithFileFormat,
    MetaFrameParquetSerializationFormat,
    MetaFrameParquetSerializationFormatWithFileFormat,
    PandasDataFrameJsonSerializationFormat,
    PandasDataFrameJsonSerializationFormatWithFileFormat,
    PandasDataFrameCsvSerializationFormat,
    PandasDataFrameCsvSerializationFormatWithFileFormat,
    PandasDataFrameParquetSerializationFormat,
    PandasDataFrameParquetSerializationFormatWithFileFormat,
    PandasDataFrameExcelSerializationFormat,
    PandasDataFrameExcelSerializationFormatWithFileFormat,
    PickleSerializationFormat,
    PickleSerializationFormatWithFileFormat,
    PolarsDataFrameExcelSerializationFormat,
    PolarsDataFrameExcelSerializationFormatWithFileFormat,
    PolarsLazyFrameJsonSerializationFormat,
    PolarsLazyFrameJsonSerializationFormatWithFileFormat,
    PolarsLazyFrameCsvSerializationFormat,
    PolarsLazyFrameCsvSerializationFormatWithFileFormat,
    PolarsLazyFrameParquetSerializationFormat,
    PolarsLazyFrameParquetSerializationFormatWithFileFormat,
    PolarsDataFrameJsonSerializationFormat,
    PolarsDataFrameJsonSerializationFormatWithFileFormat,
    PolarsDataFrameCsvSerializationFormat,
    PolarsDataFrameCsvSerializationFormatWithFileFormat,
    PolarsDataFrameParquetSerializationFormat,
    PolarsDataFrameParquetSerializationFormatWithFileFormat,
    UnitSerializationFormat,
)
from adapta.storage.models.formatters.exceptions import SerializationError

from adapta.utils.metaframe import MetaFrame


@pytest.mark.parametrize(
    "serializer, data",
    [
        (DictJsonSerializationFormat, {"test": "test"}),
        (DictJsonSerializationFormatWithFileFormat, {"test": "test"}),
        (
            PandasDataFrameParquetSerializationFormat,
            pandas.DataFrame(data={"test": [1, 2, 3]}),
        ),
        (PandasDataFrameJsonSerializationFormat, pandas.DataFrame(data={"test": [1, 2, 3]})),
        (PandasDataFrameJsonSerializationFormatWithFileFormat, pandas.DataFrame(data={"test": [1, 2, 3]})),
        (PandasDataFrameCsvSerializationFormat, pandas.DataFrame(data={"test": [1, 2, 3]})),
        (
            PolarsDataFrameParquetSerializationFormat,
            polars.DataFrame(data={"test": [1, 2, 3]}),
        ),
        (PandasDataFrameCsvSerializationFormatWithFileFormat, pandas.DataFrame(data={"test": [1, 2, 3]})),
        (
            PolarsDataFrameCsvSerializationFormat,
            polars.DataFrame(data={"test": [1, 2, 3]}),
        ),
        (PolarsDataFrameCsvSerializationFormatWithFileFormat, polars.DataFrame(data={"test": [1, 2, 3]})),
        (
            PolarsDataFrameJsonSerializationFormat,
            polars.DataFrame(data={"test": [1, 2, 3]}),
        ),
        (PolarsDataFrameJsonSerializationFormatWithFileFormat, polars.DataFrame(data={"test": [1, 2, 3]})),
        (
            PolarsLazyFrameParquetSerializationFormat,
            polars.LazyFrame(data={"test": [1, 2, 3]}),
        ),
        (PolarsLazyFrameParquetSerializationFormatWithFileFormat, polars.LazyFrame(data={"test": [1, 2, 3]})),
        (
            PolarsLazyFrameCsvSerializationFormat,
            polars.LazyFrame(data={"test": [1, 2, 3]}),
        ),
        (PolarsLazyFrameCsvSerializationFormatWithFileFormat, polars.LazyFrame(data={"test": [1, 2, 3]})),
        (
            PolarsLazyFrameJsonSerializationFormat,
            polars.LazyFrame(data={"test": [1, 2, 3]}),
        ),
        (PolarsLazyFrameJsonSerializationFormatWithFileFormat, polars.LazyFrame(data={"test": [1, 2, 3]})),
        (PickleSerializationFormat, pandas.DataFrame(data={"test": [1, 2, 3]})),
        (PickleSerializationFormatWithFileFormat, pandas.DataFrame(data={"test": [1, 2, 3]})),
        (PickleSerializationFormat, [1, 2, 3]),
        (PickleSerializationFormatWithFileFormat, [1, 2, 3]),
        (PickleSerializationFormat, {"foo": "bar"}),
        (PickleSerializationFormatWithFileFormat, {"foo": "bar"}),
        (PickleSerializationFormat, "Hello, World!"),
        (PickleSerializationFormatWithFileFormat, "Hello, World!"),
        (PickleSerializationFormat, b"Test string"),
        (PickleSerializationFormatWithFileFormat, b"Test string"),
        (MetaFrameParquetSerializationFormat, MetaFrame.from_pandas(pandas.DataFrame(data={"test": [1, 2, 3]}))),
        (
            MetaFrameParquetSerializationFormatWithFileFormat,
            MetaFrame.from_pandas(pandas.DataFrame(data={"test": [1, 2, 3]})),
        ),
        (MetaFrameParquetSerializationFormat, MetaFrame.from_polars(polars.DataFrame(data={"test": [1, 2, 3]}))),
        (
            MetaFrameParquetSerializationFormatWithFileFormat,
            MetaFrame.from_polars(polars.DataFrame(data={"test": [1, 2, 3]})),
        ),
        (PolarsDataFrameExcelSerializationFormat, polars.DataFrame(data={"test": [1, 2, 3]})),
        (PolarsDataFrameExcelSerializationFormatWithFileFormat, polars.DataFrame(data={"test": [1, 2, 3]})),
        (PandasDataFrameExcelSerializationFormat, pandas.DataFrame(data={"test": [1, 2, 3]})),
        (PandasDataFrameExcelSerializationFormatWithFileFormat, pandas.DataFrame(data={"test": [1, 2, 3]})),
    ],
)
def test_unit_serialization(serializer: type[SerializationFormat], data: any):
    """
    Tests that serializing and then immediately deserializing any data equals the original data.
    """
    if isinstance(data, MetaFrame):
        assert deepcopy(data).to_pandas().equals(serializer().deserialize(serializer().serialize(data)).to_pandas())
    elif isinstance(data, pandas.DataFrame):
        assert data.equals(serializer().deserialize(serializer().serialize(data)))
    elif isinstance(data, polars.LazyFrame) | isinstance(data, polars.DataFrame):
        assert data.lazy().collect().equals(serializer().deserialize(serializer().serialize(data)).lazy().collect())
    else:
        assert data == serializer().deserialize(serializer().serialize(data))


@pytest.mark.parametrize(
    "serializer, data",
    [
        (DictJsonSerializationFormat, b'"{\\"key\\": \\"value\\"}"')
    ],
)
def test_throws_serializer_error(serializer: type[SerializationFormat], data: any):
    with pytest.raises(SerializationError):
        serializer().deserialize(data)
