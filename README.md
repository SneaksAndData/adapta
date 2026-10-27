# Adapta
This project aim at providing tools needed for everyday activities of data scientists and engineers:
- Connectors for various cloud APIs
- Secure secret handlers for various remote storages
- Logging framework
- Metrics reporting framework
- Storage drivers for various clouds and storage types

## Delta Lake

This module provides basic Delta Lake operations without Spark session, based on [delta-rs](https://github.com/delta-io/delta-rs) project.

Please refer to the [module](adapta/storage/delta_lake/v3/README.md) documentation for examples.

## Secret Storages

Please refer to the [module](adapta/storage/secrets/README.md) documentation for examples.

## NoSql (Astra DB)

Please refer to the [module](adapta/storage/distributed_object_store/v3/datastax_astra/README.md) documentation for examples.

## Dataclass Validation framework
Please refer to the [module](adapta/dataclass_validation/README.md) documentation for examples.

## Development

### Debugging tests

`pyproject.toml` enables code coverage for every `pytest` run via `addopts` (`--cov=.`). Coverage's tracer
conflicts with debugger tracers, so breakpoints are silently ignored while coverage is active.

To debug a test, disable coverage for that run without editing `pyproject.toml`, since `--no-cov` always
overrides the `--cov` option configured in `addopts`:

```bash
uv run pytest tests/path/to/test_file.py --no-cov
```

In an IDE run/debug configuration, add `--no-cov` to the test runner's arguments (e.g. PyCharm's
"Additional Arguments" field), or set the `PYTEST_ADDOPTS=--no-cov` environment variable on the debug
configuration template so it applies automatically whenever you debug.
