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
import asyncio
import ctypes
import json
import logging
import os
import sys
import traceback
from ctypes.util import find_library
from logging import StreamHandler

import tempfile
from threading import Thread
from time import sleep

import pytest
import uuid

import requests

from adapta.logs import SemanticLogger, create_async_logger
from adapta.logs.handlers.datadog_api_handler import DataDogApiHandler
from adapta.logs.models import LogLevel

EXPECTED_MESSAGE = (
    "This a unit test logger 1, Fixed message1 this is a fixed message1, Fixed message2 this is a fixed message2\n"
)


class TestLoggerClass:
    pass


@pytest.mark.parametrize(
    "level,template,args,exception,diagnostics,expected_message",
    [
        (
            LogLevel.INFO,
            "This a unit test logger {index}",
            {"index": 1},
            None,
            None,
            EXPECTED_MESSAGE,
        ),
        (
            LogLevel.WARN,
            "This a unit test logger {index}",
            {"index": 1},
            ValueError("test warning"),
            None,
            EXPECTED_MESSAGE,
        ),
        (
            LogLevel.ERROR,
            "This a unit test logger {index}",
            {"index": 1},
            ValueError("test error"),
            None,
            EXPECTED_MESSAGE,
        ),
        (
            LogLevel.DEBUG,
            "This a unit test logger {index}",
            {"index": 1},
            ValueError("test error"),
            "additional debug info",
            EXPECTED_MESSAGE,
        ),
    ],
)
def test_log_format(
    level: LogLevel,
    template: str,
    args: dict,
    exception: BaseException,
    diagnostics: str,
    expected_message: str,
):
    test_file_path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
    with open(test_file_path, "w") as log_stream:
        stream_logger = SemanticLogger(
            fixed_template={
                "Fixed message1 {message1}": {"message1": "this is a fixed message1"},
                "Fixed message2 {message2}": {"message2": "this is a fixed message2"},
            }
        ).add_log_source(
            log_source_name=str(uuid.uuid4()),
            min_log_level=LogLevel.DEBUG,
            is_default=True,
            log_handlers=[StreamHandler(stream=log_stream)],
        )

        if level == LogLevel.INFO:
            stream_logger.info(template=template, **args)
        if level == LogLevel.WARN:
            stream_logger.warning(template=template, exception=exception, **args)
        if level == LogLevel.ERROR:
            stream_logger.error(template=template, exception=exception, **args)
        if level == LogLevel.DEBUG:
            stream_logger.debug(template=template, exception=exception, diagnostics=diagnostics, **args)

    logged_lines = open(test_file_path).readlines()
    assert expected_message in logged_lines


def test_datadog_api_handler(datadog_handler: DataDogApiHandler):
    mock_source = str(uuid.uuid4())

    dd_logger = SemanticLogger().add_log_source(
        log_source_name=mock_source,
        min_log_level=LogLevel.INFO,
        log_handlers=[datadog_handler],
        is_default=True,
    )

    try:
        raise ValueError("test warning")
    except BaseException as ex:
        dd_logger.warning(template="This a unit test logger {index}", exception=ex, index=1)
        ex_str = traceback.format_exc().removesuffix("\n")

    log_item = datadog_handler._buffer[0]
    message = json.loads(log_item.message)

    assert log_item.ddsource == mock_source
    assert log_item.ddtags == "environment:local"
    assert log_item.status == "WARNING"
    assert message["text"] == "This a unit test logger 1"
    assert message["index"] == 1
    assert message["error"] == {
        "stack": ex_str,
        "message": "test warning",
        "kind": "ValueError",
    }
    assert message["template"] == "This a unit test logger {index}"
    assert "tags" not in message


def test_adapta_logger_replacement(datadog_handler: DataDogApiHandler, restore_logger_class):
    SemanticLogger().add_log_source(
        log_source_name="urllib3",
        min_log_level=LogLevel.DEBUG,
        log_handlers=[datadog_handler],
    )
    requests.get("https://example.com")

    requests_log = logging.getLogger("urllib3")
    handler = [handler for handler in requests_log.handlers if isinstance(handler, DataDogApiHandler)][0]
    buffers = [json.loads(msg.message) for msg in handler._buffer]
    assert {"text": "Starting new HTTPS connection (1): example.com:443"} in buffers


def test_log_level(datadog_handler: DataDogApiHandler, restore_logger_class):
    logger = SemanticLogger().add_log_source(
        log_source_name="test",
        min_log_level=LogLevel.INFO,
        log_handlers=[datadog_handler],
    )
    logger.debug("Debug message", log_source_name="test")
    logger.info("Info message", log_source_name="test")

    requests_log = logging.getLogger("test")
    handler = [handler for handler in requests_log.handlers if isinstance(handler, DataDogApiHandler)][0]
    buffers = [json.loads(msg.message) for msg in handler._buffer]
    assert buffers == [{"template": "Info message", "text": "Info message"}]


def test_fixed_template(datadog_handler: DataDogApiHandler, restore_logger_class):
    logger = SemanticLogger(
        fixed_template={
            "running with job id {job_id} on {owner}": {
                "job_id": "my_job_id",
                "owner": "owner",
            }
        },
        fixed_template_delimiter="|",
    ).add_log_source(
        log_source_name="test_fixed_template",
        min_log_level=LogLevel.INFO,
        log_handlers=[datadog_handler],
    )
    logger.info(
        "Custom template={custom_value}",
        log_source_name="test_fixed_template",
        custom_value="my-value",
    )

    requests_log = logging.getLogger("test_fixed_template")
    handler = [handler for handler in requests_log.handlers if isinstance(handler, DataDogApiHandler)][0]
    buffers = [json.loads(msg.message) for msg in handler._buffer]
    assert buffers == [
        {
            "template": "Custom template={custom_value}|running with job id {job_id} on {owner}",
            "custom_value": "my-value",
            "job_id": "my_job_id",
            "owner": "owner",
            "text": "Custom template=my-value|running with job id my_job_id on owner",
        }
    ]


def test_fixed_template_duplicate_handler(datadog_handler: DataDogApiHandler, restore_logger_class):
    logger = SemanticLogger(
        fixed_template={
            "running with job id {job_id} on {owner}": {
                "job_id": "my_job_id",
                "owner": "owner",
            }
        },
        fixed_template_delimiter="|",
    ).add_log_source(
        log_source_name="test_fixed_template",
        min_log_level=LogLevel.INFO,
        log_handlers=[datadog_handler],
    )
    logger.info(
        "About to log a duplicate={custom_value} for {job_id} on {owner}",
        log_source_name="test_fixed_template",
        custom_value="my-value",
        job_id="my_job_id2",
        owner="owner2",
    )

    requests_log = logging.getLogger("test_fixed_template")
    handler = [handler for handler in requests_log.handlers if isinstance(handler, DataDogApiHandler)][0]
    buffers = [json.loads(msg.message) for msg in handler._buffer]
    assert buffers == [
        {
            "template": "About to log a duplicate={custom_value} for {job_id} on {owner}|running with job id {job_id} on {owner}",
            "custom_value": "my-value",
            "job_id": "my_job_id",
            "owner": "owner",
            "text": "About to log a duplicate=my-value for my_job_id on owner|running with job id my_job_id on owner",
        },
        {
            "template": "Duplicated log properties provided: {job_id}, {owner}",
            "custom_value": "my-value",
            "job_id": "my_job_id",
            "owner": "owner",
            "text": "Duplicated log properties provided: my_job_id2, owner2",
        },
    ]


@pytest.mark.asyncio
async def test_log_level_async(restore_logger_class, datadog_handler):
    with create_async_logger(logger_type=TestLoggerClass, log_handlers=[datadog_handler]) as logger:
        logger.debug("Debug message: {value}", value=1)
        logger.info("Info message: {value}", value=2)

    await asyncio.sleep(1)
    buffer = [json.loads(msg.message) for msg in logger._log_handlers[0]._buffer]
    assert buffer == [{"template": "Info message: {value}", "text": "Info message: 2", "value": 2}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "level,template,args,exception,diagnostics,expected_message",
    [
        (
            LogLevel.INFO,
            "This a unit test logger {index}",
            {"index": 1},
            None,
            None,
            EXPECTED_MESSAGE,
        ),
        (
            LogLevel.WARN,
            "This a unit test logger {index}",
            {"index": 1},
            ValueError("test warning"),
            None,
            EXPECTED_MESSAGE,
        ),
        (
            LogLevel.ERROR,
            "This a unit test logger {index}",
            {"index": 1},
            ValueError("test error"),
            None,
            EXPECTED_MESSAGE,
        ),
        (
            LogLevel.DEBUG,
            "This a unit test logger {index}",
            {"index": 1},
            ValueError("test error"),
            "additional debug info",
            EXPECTED_MESSAGE,
        ),
    ],
)
async def test_log_format_async(
    level: LogLevel,
    template: str,
    args: dict,
    exception: BaseException,
    diagnostics: str,
    expected_message: str,
):
    test_file_path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
    with open(test_file_path, "w") as log_stream:
        with create_async_logger(
            logger_type=TestLoggerClass,
            min_log_level=LogLevel.DEBUG,
            log_handlers=[StreamHandler(stream=log_stream)],
            fixed_template={
                "Fixed message1 {message1}": {"message1": "this is a fixed message1"},
                "Fixed message2 {message2}": {"message2": "this is a fixed message2"},
            },
        ) as logger:
            if level == LogLevel.INFO:
                logger.info(template=template, **args)
            if level == LogLevel.WARN:
                logger.warning(template=template, exception=exception, **args)
            if level == LogLevel.ERROR:
                logger.error(template=template, exception=exception, **args)
            if level == LogLevel.DEBUG:
                logger.debug(template=template, exception=exception, diagnostics=diagnostics, **args)

        await asyncio.sleep(1)

        logged_lines = open(test_file_path).readlines()
        assert expected_message in logged_lines


def printf_messages(message_count: int, output_type: str) -> None:
    libc = ctypes.cdll.LoadLibrary(find_library("c"))
    cstd = None
    if sys.platform == "darwin":
        cstd = ctypes.c_void_p.in_dll(libc, "__stdoutp" if output_type == "stdout" else "__stderrp")
    if sys.platform == "linux":
        cstd = ctypes.c_void_p.in_dll(libc, output_type)

    if sys.platform == "win32":
        raise RuntimeError(f"Unsupported platform: {sys.platform}")

    libc.setbuf(cstd, None)
    for log_n in range(message_count):
        libc.fprintf(cstd, bytes(f"Test log message: #{log_n}\n", encoding="utf-8"))


@pytest.mark.parametrize(
    "std_type",
    ["stdout", "stderr"],
)
@pytest.mark.skipif(sys.platform in ["win32"], reason="redirect is only supported on Linux/MacOS")
def test_redirect(datadog_handler: DataDogApiHandler, restore_logger_class, std_type: str):
    """
    Test sync redirect in a sync program from an external non-python process print.
    """
    logger = SemanticLogger().add_log_source(
        log_source_name="test",
        min_log_level=LogLevel.INFO,
        log_handlers=[datadog_handler],
        is_default=True,
    )

    print_thread = Thread(
        target=printf_messages,
        args=(
            10,
            std_type,
        ),
    )

    with logger.redirect():
        print_thread.start()
        sleep(1)

    buffer = [json.loads(msg.message) for msg in datadog_handler._buffer]

    assert len(buffer) == 10


@pytest.mark.parametrize(
    "std_type",
    ["stdout", "stderr"],
)
@pytest.mark.asyncio
@pytest.mark.skipif(sys.platform in ["win32"], reason="redirect is only supported on Linux/MacOS")
async def test_redirect_async_legacy(restore_logger_class, datadog_handler, std_type: str):
    """
    Test sync redirect when running inside asyncio loop, from an external non-python process print.
    """
    with create_async_logger(
        logger_type=TestLoggerClass,
        min_log_level=LogLevel.DEBUG,
        log_handlers=[datadog_handler],
        fixed_template={"Fixed message1 {message1}": {"message1": "this is a fixed message1"}},
    ) as logger:
        print_thread = Thread(
            target=printf_messages,
            args=(
                10,
                std_type,
            ),
        )
        with logger.redirect():
            print_thread.start()
            await asyncio.sleep(1)

        buffer = [json.loads(msg.message) for msg in logger._log_handlers[0]._buffer]

        assert len(buffer) == 10


@pytest.mark.parametrize(
    "std_type",
    ["stdout", "stderr"],
)
@pytest.mark.asyncio
@pytest.mark.skipif(sys.platform in ["win32"], reason="redirect is only supported on Linux/MacOS")
async def test_redirect_async(restore_logger_class, datadog_handler, std_type: str):
    """
    Test async redirect from an external non-python process print, when running inside asyncio loop
    """
    with create_async_logger(
        logger_type=TestLoggerClass,
        min_log_level=LogLevel.DEBUG,
        log_handlers=[datadog_handler],
        fixed_template={"Fixed message1 {message1}": {"message1": "this is a fixed message1"}},
    ) as logger:
        print_thread = Thread(
            target=printf_messages,
            args=(
                10,
                std_type,
            ),
        )
        async with logger.redirect_async():
            print_thread.start()
            print_thread.join()

        # await asyncio.sleep(1)

        buffer = [json.loads(msg.message) for msg in logger._log_handlers[0]._buffer]

        assert len(buffer) == 10
