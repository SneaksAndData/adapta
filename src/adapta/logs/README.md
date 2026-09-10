# Logging Integrations

This module provides a generic interface to plug json-formatted logging into your python application.

## Generic Usage

First, create a logger object and add some log sources. Then log on a desired level directly using default handler:

```python
from adapta.logs import SemanticLogger
from adapta.logs.models import LogLevel

c_logger = SemanticLogger()
.add_log_source(log_source_name='test_logger_1', min_log_level=LogLevel.INFO, is_default=True)
.add_log_source(log_source_name='test_logger_2', min_log_level=LogLevel.ERROR)

# INFO level, default log source

c_logger.info(template='Test message: {message}', message='important')

# ERROR level with exception

try:
    raise ValueError('Big boom')
except ValueError as ex:
    c_logger.error(log_source_name='test_logger_2',
                   template='Test error message: {message}', message='failure',
                   exception=ex)
```

You can also use `Logger` instances directly:

```python
from adapta.logs import SemanticLogger
from adapta.logs.models import LogLevel

c_logger = SemanticLogger()
.add_log_source(log_source_name='test_logger_1', min_log_level=LogLevel.INFO, is_default=True)

logger = c_logger.test_logger_1
```

### DataDog handler

In order to send logs to DataDog, use `DataDogApiHandler` when adding a log source. If you still want messages
in `stdout` or `stderr`, add `SafeStreamHandler` on top:

```python
from adapta.logs import SemanticLogger
from adapta.logs.models import LogLevel
from adapta.logs.handlers.datadog_api_handler import DataDogApiHandler
from adapta.logs.handlers.safe_stream_handler import SafeStreamHandler

c_logger = SemanticLogger()
.add_log_source(log_source_name='test_logger_1', min_log_level=LogLevel.INFO,
                log_handlers=[DataDogApiHandler(), SafeStreamHandler()], is_default=True)

# you can also add fixed parts to your log messages, for example add a job execution id:

my_job_id = '000-000-111'
owner = 'host-1'
c_logger = SemanticLogger(fixed_template={
    'running with job id {job_id} on {owner}': {
        'job_id': my_job_id,
        'owner': owner
    }
}, fixed_template_delimiter='|')
.add_log_source(log_source_name='test_logger_1', min_log_level=LogLevel.INFO,
                log_handlers=[DataDogApiHandler(), SafeStreamHandler()], is_default=True)

# messages emitted by the logger will look like this:
# a message is here | running with job id 000-000-111 on host-1
# another message is here | running with job id 000-000-111 on host-1
# ...
```

Note: StreamHandler from logging package should not be used together with stdout redirection, it could lead
to duplicated messages in datadog. If you want to print log messages to stdout, you should use SafeStreamHandler.

Remember to set `PROTEUS__DD_API_KEY`, `PROTEUS__DD_APP_KEY` and `PROTEUS__DD_SITE` environment variables before creating an instance
of `DataDogApiHandler()`.

### Overriding existing loggers
This module supports integration with existing logger, you can use it as following:

```python
import sys
from logging import StreamHandler, Formatter
from adapta.logs import SemanticLogger
from adapta.logs.models import LogLevel
from adapta.logs.handlers.datadog_api_handler import DataDogApiHandler

# Create stream handler for loggers
stream_handler = StreamHandler(sys.stdout)

# Set up format for stdout logs
formatter = Formatter('%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)

# Create stream handler for loggers. Datadog handler use REST api to push log messages, so it do not need a formatter
datadog_handler = DataDogApiHandler()

logger = SemanticLogger().add_log_source(
    log_source_name="azure",
    min_log_level=LogLevel.ERROR,
    log_handlers=[stream_handler, datadog_handler]
)
.add_log_source(
    log_source_name="my-app",
    min_log_level=LogLevel.INFO,
    log_handlers=[stream_handler, datadog_handler]
)
```

This will add `CompositeLogger` for logging source `my-app` and reconfigure python Logger with name `azure` to use 
supplied handlers and `ERROR` as minimum log level.
