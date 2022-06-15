import pytest
from typing import Type
import pandas
from proteus.storage.models.format import DictJsonSerializationFormat, SerializationFormat, \
    DataFrameParquetSerializationFormat, DataFrameJsonSerializationFormat, UnitSerializationFormat, \
    PickleSerializationFormat


@pytest.mark.parametrize(
    'serializer, data',
    [
        (DictJsonSerializationFormat, {'test': 'test'}),
        (DataFrameParquetSerializationFormat, pandas.DataFrame(data={'test': [1, 2, 3]})),
        (DataFrameJsonSerializationFormat, pandas.DataFrame(data={'test': [1, 2, 3]})),
        (PickleSerializationFormat, pandas.DataFrame(data={'test': [1, 2, 3]})),
        (UnitSerializationFormat, b'Test string'),
    ]
)
def test_unit_serialization(serializer: Type[SerializationFormat], data: any):
    """
    Tests that serializing and then immediately deserializing any data equals the original data.
    """
    if isinstance(data, pandas.DataFrame):
        assert data.equals(serializer().deserialize(serializer().serialize(data)))
    else:
        assert data == serializer().deserialize(serializer().serialize(data))
