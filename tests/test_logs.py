import json
import os
import socket
from logging import StreamHandler

import tempfile
from typing import Dict

import pytest
import uuid

from datadog_api_client.v2.model.http_log_item import HTTPLogItem
from pytest_mock import MockerFixture

from proteus.logs.handlers.datadog_api_handler import DataDogApiHandler
from proteus.logs import ProteusLogger
from proteus.logs.models import LogLevel


@pytest.mark.parametrize('level,template,args,exception,diagnostics,expected_message', [
    (
            LogLevel.INFO,
            'This a unit test logger {index}',
            {'index': 1},
            None,
            None,
            '{"template": "This a unit test logger {index}", "text": "This a unit test logger 1", "index": 1}'
    ),
    (
            LogLevel.WARN,
            'This a unit test logger {index}',
            {'index': 1},
            ValueError("test warning"),
            None,
            '{"template": "This a unit test logger {index}", "text": "This a unit test logger 1", "index": 1}'
    ),
    (
            LogLevel.ERROR,
            'This a unit test logger {index}',
            {'index': 1},
            ValueError("test error"),
            None,
            '{"template": "This a unit test logger {index}", "text": "This a unit test logger 1", "index": 1}'
    ),
    (
            LogLevel.DEBUG,
            'This a unit test logger {index}',
            {'index': 1},
            ValueError("test error"),
            'additional debug info',
            '{"template": "This a unit test logger {index}", "text": "This a unit test logger 1", "diagnostics": "additional debug info", "index": 1}'
    )
])
def test_log_format(level: LogLevel, template: str, args: Dict, exception: BaseException, diagnostics: str,
                    expected_message: str):
    test_file_path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
    with open(test_file_path, 'w') as log_stream:
        stream_logger = ProteusLogger() \
            .add_log_source(log_source_name=str(uuid.uuid4()), min_log_level=LogLevel.DEBUG, is_default=True,
                            log_handlers=[StreamHandler(stream=log_stream)])

        if level == LogLevel.INFO:
            stream_logger.info(template=template, **args)
        if level == LogLevel.WARN:
            stream_logger.warning(template=template, exception=exception, **args)
        if level == LogLevel.ERROR:
            stream_logger.error(template=template, exception=exception, **args)
        if level == LogLevel.DEBUG:
            stream_logger.debug(template=template, exception=exception, diagnostics=diagnostics, **args)

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

    dd_logger.warning(template='This a unit test logger {index}', exception=ValueError("test warning"), index=1)

    assert mock_handler._buffer[0] == HTTPLogItem(
        ddsource=mock_source,
        ddtags=None,
        hostname=socket.gethostname(),
        message='{"template": "This a unit test logger {index}", "text": "This a ''unit test logger 1", "error": {"stack": "", "message": null, ''"kind": "ValueError"}}',
        status='WARNING'
    )
