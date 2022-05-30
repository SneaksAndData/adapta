# Logging Integrations

This module provides a generic interface to plug json-formatted logging into your python application.

## Generic Usage

First, create a logger object and add some log sources. Then log on a desired level directly using default handler:

```python
from proteus.logs import ProteusLogger
from proteus.logs.models import LogLevel, InfoLog, ErrorLog

proteus_logger = ProteusLogger() \
    .add_log_source(log_source='proteus_test_logger_1', lowest_log_level=LogLevel.INFO) \
    .add_log_source(log_source='proteus_test_logger_2', lowest_log_level=LogLevel.ERROR)

# INFO level

proteus_logger.log(log_source='proteus_test_logger_1', data=InfoLog(template='Test message: {message}', args={'message': 'important'}))

# ERROR level with exception

try:
    raise ValueError('Big boom')
except ValueError as ex:
    proteus_logger.log(log_source='proteus_test_logger_2', data=ErrorLog(template='Test error message: {message}', args={'message': 'failure'}, exception=ex))
```

### DataDog handler

In order to send logs to DataDog, use `DataDogApiHandler` when adding a log source. If you still want messages in `stdout` or `stderr`, add `StreamHandler` on top:
```python
from logging import StreamHandler

from proteus.logs import ProteusLogger
from proteus.logs.models import LogLevel
from proteus.logs.handlers.datadog_api_handler import DataDogApiHandler

proteus_logger = ProteusLogger() \
    .add_log_source(log_source='proteus_test_logger_1', lowest_log_level=LogLevel.INFO, log_handlers=[DataDogApiHandler(), StreamHandler()])
```

Remember to set `DD_API_KEY`, `DD_APP_KEY` and `DD_SITE` environment variables before creating an instance of `DataDogApiHandler()`.
