# adapta.utils
A set of utility functions that can be used in any python project.

## doze
The doze function is used to sleep for a given amount of time  (in seconds), in short intervals.

This is useful in environments when your process needs to sleep, but respond to SIGTERM/SIGINT as soon as they are received.

Example usage:
```python
from adapta.utils import doze

# Sleep for 5 seconds
elapsed_time_ns = doze(5)
print(f'Time elapsed (in nanoseconds): {elapsed_time_ns}')
```

Output:
```
Time elapsed (in nanoseconds): 5000000000
```

## session_with_retries
The session_with_retries function is used to provision an HTTP session manager with built-in retries.

Example usage:
```python
from adapta.utils import session_with_retries

# Provision an HTTP session manager with 4 retries
http_session = session_with_retries(retry_count=4)
```

## convert_datadog_tags
The convert_datadog_tags function is used to convert a dictionary of tags into the Datadog tag format.

Example usage:
```python
from adapta.utils import convert_datadog_tags

# Convert a tag dictionary to Datadog tag format
tags = {'environment': 'production', 'version': '1.0.0'}
datadog_tags = convert_datadog_tags(tags)
print(datadog_tags)
```

Output:
```
['environment:production', 'version:1.0.0']
```

## operation_time
The operation_time context manager is used to measure the execution time of a given operation.

Example usage:
```python
from adapta.utils import operation_time, doze

# Measure execution time of the doze function
with operation_time() as op:
    doze(5)

# Print execution time
print(f'Time elapsed (in nanoseconds): {op.elapsed}')
```

Output:
```
Time elapsed (in nanoseconds): 5000000000
```

## chunk_list
The chunk_list function is used to split a given list into a specified number of chunks.

Example usage:
```python
from adapta.utils import chunk_list

# Split a list of numbers into up to 4 chunks
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9]
chunks = chunk_list(numbers, 4)
print(chunks)
```

Output:
```
[[1, 2, 3], [4, 5, 6], [7, 8, 9]]
```

### memory_limit:

Context manager to limit the amount of memory used by a process. On context exit, the memory limit is reset to the total memory available.

Parameters:
* memory_limit_percentage (optional): Percentage of total memory to limit usage to
* memory_limit_bytes (optional): Number of bytes to limit usage to

Raises:
* ValueError: If neither memory_limit_percentage or memory_limit_bytes is specified

Example Usage:
```python
from adapta.utils import memory_limit

with memory_limit(memory_limit_percentage=0.2):
    # Memory limited to 20% of total available
    # Do some stuff here
    # if the provided limit is exceeded, MemoryError will be raised and can be caught by client code
    pass
```