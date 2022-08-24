#!/usr/bin/env python
import os
import sys
import time
from io import TextIOWrapper
from logging import StreamHandler

from proteus.logs import ProteusLogger
from proteus.logs.models import LogLevel

if __name__ == "__main__":
    logger: ProteusLogger = ProteusLogger().add_log_source(
        log_source_name='AutoReplenishmentModel',
        min_log_level=LogLevel.INFO,
        log_handlers=[StreamHandler()],
        is_default=True
    )
    with logger.redirect():
        #while True:
        print('message')
        logger.info(template='another message')
        #time.sleep(3)
