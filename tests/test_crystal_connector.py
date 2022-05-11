import pytest
from typing import Type
import pandas
from proteus.storage.models.format import DictJsonSerializationFormat, SerializationFormat, \
    DataFrameParquetSerializationFormat, DataFrameJsonSerializationFormat
from proteus.connectors.crystal import CrystalConnector
from proteus.utils import CrystalEntrypointArguments


class MockHttpResponse:
    def __init__(self, data: bytes):
        self.content = data

    def raise_for_status(self):
        pass


class MockHttpConnection:
    def __init__(self, response: MockHttpResponse):
        self.response = response

    def get(self, *args: any, **kwargs: any):
        return self.response

    def close(self):
        pass
    

@pytest.mark.parametrize(
    'serializer, data',
    [
        (DictJsonSerializationFormat, {'test': 'test'}),
        (DataFrameParquetSerializationFormat, pandas.DataFrame(data={'test': [1, 2, 3]})),
        (DataFrameJsonSerializationFormat, pandas.DataFrame(data={'test': [1, 2, 3]}))
    ]
)
def test_crystal_read_input(mocker, serializer: Type[SerializationFormat], data: any):
    """
    Test that the function `read_input` in the `CrystalConnector` object deserializes and returns the correct data.
    """
    mocker.patch(
        'proteus.connectors.crystal._connector.session_with_retries',
        return_value=MockHttpConnection(MockHttpResponse(serializer().serialize(data)))
    )

    args = CrystalEntrypointArguments(
        sas_uri='https://some.sas.url.com',
        request_id='test-id',
        results_receiver='https://some.url.com'
    )

    conn = CrystalConnector(base_url='https://some.url.com', user='test-user', password='test-pass')
    read_data = conn.read_input(args, serializer)

    if isinstance(data, pandas.DataFrame):
        assert data.equals(read_data)
    else:
        assert data == read_data
