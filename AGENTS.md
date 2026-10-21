# AGENTS.md — Developer and AI Agent Guidelines for Adapta

This document outlines key architecture principles, coding standards, performance patterns, and operational guidelines for AI agents and contributors working in the `adapta` repository.

---

## 1. Project Overview & Architecture

`adapta` is a Python library providing tools for data engineering and data science workflows:
- **Storage Engines & Connectors**: Cassandra/Astra, Apache Iceberg, Delta Lake, Snowflake, Trino, AWS S3, Azure Blob/ADLS.
- **Query Enabled Store (QES)**: Unified query and filtering abstraction across heterogeneous storage backends.
- **Data Transport**: Polars (`LazyFrame`, `DataFrame`), Apache Arrow, and `MetaFrame` (multi-engine DataFrame abstraction).
- **Resilience & Monitoring**: Backoff-based retries, rate limiting, structured logging, and metrics providers.

---

## 2. Environment & Commands

The project uses `uv` for dependency management and execution.

- **Python Version**: Python 3.11+
- **Run Tests**:
  ```bash
  uv run pytest
  ```
- **Targeted Test Execution**:
  ```bash
  uv run pytest tests/path/to/test_file.py
  uv run pytest tests/path/to/test_file.py -k "test_function_name"
  ```
- **Linting & Code Style**:
  ```bash
  uv run ruff check src tests
  uv run ruff format --check src tests
  ```

---

## 3. Code Style & Implementation Standards

### Method-Local Helper Functions
When defining helper functions inside a method or function body:
1. **Position at the Top**: Place all method-local functions at the very top of the method body, immediately below the docstring and before any procedural logic or local variable declarations.
2. **Strict Closure Scope**: Method-local functions must **only** access parent method parameters (and `self` for instance methods) or their own passed arguments. **Never** capture local variables declared later in the parent method body via closure.

```python
# GOOD
def upload_concurrent_batch(self, source: LazyFrame, table_name: str, ...):
    """Docstring."""
    @run_time_metrics(...)
    def _upsert_with_metric(entities: list[dict], **kwargs) -> int:
        ...

    def _upload_shard(shard_source: LazyFrame, target_table: str, target_pks: list[str]) -> int:
        # Only uses parent parameters (self, batch_size, entity_type) and its arguments
        ...

    # Local procedural logic starts here
    lazy_source = _prepare_lazy_source(source)
    cassandra_mapper = get_mapper(...)
    ...
```

### Typing and Formatting
- **Line Length**: 120 characters (enforced by Ruff).
- **Type Annotations**: Comprehensive type hints are required everywhere.
  - Use Python 3.10+ union types: `T | None` instead of `Optional[T]`.
  - Use built-in generics: `list[str]`, `dict[str, Any]`, `type[T]`.
- **Docstrings**: Sphinx/Google style specifying `:param:` and `:return:`.

### Error Handling & Retries
- Use `@on_exception` from `backoff` with exponential jitter for transient network or database errors (e.g. `OverloadedErrorMessage`, `WriteTimeout`, `IsBootstrappingErrorMessage`).
- Include rate limit decorators (`@rate_limit`) where API rate control is necessary.

---

## 4. Data Processing & Performance Patterns

### Zero-RAM Materialization for LazyFrames
- **Never collect source LazyFrames to RAM**: Avoid calling `.collect()`, `.to_pandas()`, or `.to_dicts()` on full datasets or sources in batch/shard loops.
- Use `.collect_batches(chunk_size=..., maintain_order=False)` or streaming execution to process records chunk by chunk.

### Arrow C-Stream Handling
- Iceberg table scans (`load_using_catalog(..., lazy_read=True)`) return Polars `LazyFrame` backed by an Arrow C-stream (`ArrowArrayStream`).
- Arrow C-streams are single-pass, forward-only pointers. Attempting to filter or branch a single stream multiple times concurrently will cause deadlocks or stream exhaustion.
- When multiple passes or concurrent readers are required, buffer the stream lazily to temporary disk storage via `sink_ipc`:
  ```python
  def _cache_lazyframe(lazy_source: polars.LazyFrame) -> polars.LazyFrame:
      tmp_path = os.path.join(tempfile.mkdtemp(), "source.ipc")
      lazy_source.sink_ipc(tmp_path)
      return polars.scan_ipc(tmp_path)
  ```
---

## 5. Agent Workflow Guidelines

- **Direct Action Over Overhead**: Do not generate unnecessary tracking issues, tasks, or planning markdown files unless explicitly requested. Directly implement code changes, verify them, and report results concisely.
- **Verification Requirement**: Always validate changes using `uv run ruff check` and `uv run pytest` before concluding a task. Ensure all affected integration and unit tests pass without regressions.
