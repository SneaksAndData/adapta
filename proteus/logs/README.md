# Logging Integrations

This module provides a generic interface to plug json-formatted logging into your python application.

## Generic Usage

First, create a logger object and add some log sources. Then log on a desired level directly using default handler:

```python
from proteus.logs import ProteusLogger
from proteus.logs.models import LogLevel

proteus_logger = ProteusLogger() \
    .add_log_source(log_source_name='proteus_test_logger_1', min_log_level=LogLevel.INFO, is_default=True) \
    .add_log_source(log_source_name='proteus_test_logger_2', min_log_level=LogLevel.ERROR)

# INFO level, default log source

proteus_logger.info(template='Test message: {message}', message='important')

# ERROR level with exception

try:
    raise ValueError('Big boom')
except ValueError as ex:
    proteus_logger.error(log_source_name='proteus_test_logger_2',
                         template='Test error message: {message}', message='failure',
                         exception=ex)
```

You can also use `Logger` instances directly:

```python
from proteus.logs import ProteusLogger
from proteus.logs.models import LogLevel

proteus_logger = ProteusLogger() \
    .add_log_source(log_source_name='proteus_test_logger_1', min_log_level=LogLevel.INFO, is_default=True)

logger = proteus_logger.proteus_test_logger_1
```

### DataDog handler

In order to send logs to DataDog, use `DataDogApiHandler` when adding a log source. If you still want messages
in `stdout` or `stderr`, add `StreamHandler` on top:

```python
from logging import StreamHandler

from proteus.logs import ProteusLogger
from proteus.logs.models import LogLevel
from proteus.logs.handlers.datadog_api_handler import DataDogApiHandler

proteus_logger = ProteusLogger() \
    .add_log_source(log_source_name='proteus_test_logger_1', min_log_level=LogLevel.INFO,
                    log_handlers=[DataDogApiHandler(), StreamHandler()], is_default=True)
```

Remember to set `DD_API_KEY`, `DD_APP_KEY` and `DD_SITE` environment variables before creating an instance
of `DataDogApiHandler()`.
