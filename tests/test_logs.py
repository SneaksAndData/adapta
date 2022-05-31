import json
import os
import socket
from logging import StreamHandler

import tempfile

import pytest
import uuid

from datadog_api_client.v2.model.http_log_item import HTTPLogItem
from pytest_mock import MockerFixture

from proteus.logs.handlers.datadog_api_handler import DataDogApiHandler
from proteus.logs import ProteusLogger
from proteus.logs.models import LogLevel, InfoLog, WarnLog, ErrorLog, DebugLog


@pytest.mark.parametrize('log_data,expected_message', [
    (
            InfoLog(template='This a unit test logger {index}', args={'index': 1}),
            '{"template": "This a unit test logger {index}", "text": "This a unit test logger 1"}'
    ),
    (
            WarnLog(template='This a unit test logger {index}', args={'index': 1},
                    exception=ValueError("test warning")),
            '{"template": "This a unit test logger {index}", "exception": "test warning", "text": "This a unit test logger 1"}'
    ),
    (
            ErrorLog(template='This a unit test logger {index}', args={'index': 1}, exception=ValueError("test error")),
            '{"template": "This a unit test logger {index}", "exception": "test error", "text": "This a unit test logger 1"}'
    ),
    (
            DebugLog(template='This a unit test logger {index}', args={'index': 1}, exception=ValueError("test error"),
                     diagnostics='additional debug info'),
            '{"template": "This a unit test logger {index}", "diagnostics": "additional debug info", "exception": "test error", "text": "This a unit test logger 1"}'
    )
])
def test_log_format(log_data, expected_message):
    test_file_path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
    with open(test_file_path, 'w') as log_stream:
        stream_logger = ProteusLogger() \
            .add_log_source(log_source_name=str(uuid.uuid4()), min_log_level=LogLevel.DEBUG, is_default=True,
                            log_handlers=[StreamHandler(stream=log_stream)])

        stream_logger.log(data=log_data)

    logged_lines = open(test_file_path, 'r').readlines()
    try:
        entry = json.loads(''.join(logged_lines))
        assert entry['message'] == expected_message
    except Exception as ex:
        raise ex


def test_datadog_api_handler(mocker: MockerFixture):
    os.environ.setdefault('DD_API_KEY', 'some-key')
    os.environ.setdefault('DD_APP_KEY', 'some-app-key')
    os.environ.setdefault('DD_SITE', 'some-site.dog')

    mocker.patch('proteus.logs.handlers.datadog_api_handler.DataDogApiHandler._flush', return_value=None)
    mock_handler = DataDogApiHandler(buffer_size=1)
    mock_source = str(uuid.uuid4())

    dd_logger = ProteusLogger() \
        .add_log_source(log_source_name=mock_source, min_log_level=LogLevel.INFO,
                        log_handlers=[mock_handler], is_default=True)

    dd_logger.log(data=WarnLog(template='This a unit test logger {index}', args={'index': 1},
                               exception=ValueError("test warning")))

    assert mock_handler._buffer[0] == HTTPLogItem(
        ddsource=mock_source,
        ddtags=None,
        hostname=socket.gethostname(),
        message='{"template": "This a unit test logger {index}", "text": "This a ''unit test logger 1", "error": {"stack": "", "message": null, ''"kind": "ValueError"}}',
        status='WARNING'
    )
