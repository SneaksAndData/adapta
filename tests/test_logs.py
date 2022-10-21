import json
import logging
import os
import traceback
from logging import StreamHandler

import tempfile
from typing import Dict
import pytest
import uuid

from pytest_mock import MockerFixture

from proteus.logs import ProteusLogger, inject_datadog_logging
from proteus.logs.handlers.datadog_api_handler import DataDogApiHandler
from proteus.logs.models import LogLevel


EXPECTED_MESSAGE = 'This a unit test logger 1, Fixed message1 this is a fixed message1, Fixed message2 this is a fixed message2\n'
@pytest.mark.parametrize('level,template,args,exception,diagnostics,expected_message', [
    (
            LogLevel.INFO,
            'This a unit test logger {index}',
            {'index': 1},
            None,
            None,
            EXPECTED_MESSAGE
    ),
    (
            LogLevel.WARN,
            'This a unit test logger {index}',
            {'index': 1},
            ValueError("test warning"),
            None,
            EXPECTED_MESSAGE
    ),
    (
            LogLevel.ERROR,
            'This a unit test logger {index}',
            {'index': 1},
            ValueError("test error"),
            None,
            EXPECTED_MESSAGE
    ),
    (
            LogLevel.DEBUG,
            'This a unit test logger {index}',
            {'index': 1},
            ValueError("test error"),
            'additional debug info',
            EXPECTED_MESSAGE
    )
])
def test_log_format(level: LogLevel, template: str, args: Dict, exception: BaseException, diagnostics: str,
                    expected_message: str):
    test_file_path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
    with open(test_file_path, 'w') as log_stream:
        stream_logger = ProteusLogger(fixed_template={
            'Fixed message1 {message1}': {
                'message1': 'this is a fixed message1'
            },
            'Fixed message2 {message2}': {
                'message2': 'this is a fixed message2'
            },
        }) \
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
    assert expected_message in logged_lines


def test_datadog_api_handler(mocker: MockerFixture):
    os.environ.setdefault('PROTEUS__DD_API_KEY', 'some-key')
    os.environ.setdefault('PROTEUS__DD_APP_KEY', 'some-app-key')
    os.environ.setdefault('PROTEUS__DD_SITE', 'some-site.dog')

    mocker.patch('proteus.logs.handlers.datadog_api_handler.DataDogApiHandler._flush', return_value=None)
    mock_handler = DataDogApiHandler(buffer_size=1)
    mock_source = str(uuid.uuid4())

    dd_logger = ProteusLogger() \
        .add_log_source(log_source_name=mock_source, min_log_level=LogLevel.INFO,
                        log_handlers=[mock_handler], is_default=True)

    ex_str = None
    try:
        raise ValueError("test warning")
    except BaseException as ex:
        dd_logger.warning(template='This a unit test logger {index}', exception=ex, index=1)
        ex_str = traceback.format_exc().removesuffix("\n")

    log_item = mock_handler._buffer[0]
    message = json.loads(log_item.message)

    assert log_item.ddsource == mock_source
    assert log_item.ddtags == 'environment:local'
    assert log_item.status == 'WARNING'
    assert message["text"] == "This a unit test logger 1"
    assert message["index"] == 1
    assert message["error"] == {"stack": ex_str, "message": None, ''"kind": "ValueError"}
    assert message["template"] == "This a unit test logger {index}"
    assert "tags" not in message


def test_proteus_logger_replacement(mocker: MockerFixture):
    os.environ.setdefault('PROTEUS__DD_API_KEY', 'some-key')
    os.environ.setdefault('PROTEUS__DD_APP_KEY', 'some-app-key')
    os.environ.setdefault('PROTEUS__DD_SITE', 'some-site.dog')

    mocker.patch('proteus.logs.handlers.datadog_api_handler.DataDogApiHandler._flush', return_value=None)
    mock_handler = DataDogApiHandler(buffer_size=1)
    mock_source = "third.party.library"

    klass = logging.getLoggerClass()
    try:
        inject_datadog_logging()
        dd_logger = logging.getLogger(mock_source)
        dd_logger.addHandler(mock_handler)
        ex_str = None
        try:
            raise ValueError("test warning")
        except BaseException as ex:
            dd_logger.warning("Something bad happened!", exc_info=True)
            ex_str = traceback.format_exc().removesuffix("\n")

        log_item = mock_handler._buffer[0]
        message = json.loads(log_item.message)

        assert log_item.ddsource == mock_source
        assert log_item.ddtags == 'environment:local'
        assert log_item.status == 'WARNING'
        assert message["text"] == "Something bad happened!"
        assert message["error"] == {"stack": ex_str, "message": None, ''"kind": "ValueError"}
        assert "tags" not in message
    finally:
        logging.setLoggerClass(klass)
