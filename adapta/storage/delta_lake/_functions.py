"""
 Operations on Delta Lake tables.
"""
#  Copyright (c) 2023-2024. ECCO Sneaks & Data
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

import datetime
import hashlib
import zlib
from typing import Optional, Union, Iterator, List, Iterable, Tuple

from pandas import DataFrame, concat
import pyarrow
from deltalake import DeltaTable
from pyarrow import RecordBatch, Table
from pyarrow._dataset_parquet import ParquetReadOptions  # pylint: disable=E0611

from adapta.logs import SemanticLogger
from adapta.security.clients._base import AuthenticationClient
from adapta.storage.models.base import DataPath
from adapta.storage.delta_lake._models import DeltaTransaction
from adapta.storage.cache import KeyValueCache
from adapta.storage.models.format import DataFrameParquetSerializationFormat
from adapta.storage.models.filter_expression import Expression, ArrowFilterExpression, compile_expression


def load(  # pylint: disable=R0913
    auth_client: AuthenticationClient,
    path: DataPath,
    version: Optional[int] = None,
    row_filter: Optional[Union[Expression, pyarrow.compute.Expression]] = None,
    columns: Optional[List[str]] = None,
    batch_size: Optional[int] = None,
    partition_filter_expressions: Optional[List[Tuple]] = None,
) -> Union[DeltaTable, DataFrame, Iterator[DataFrame]]:
    """
     Loads Delta Lake table from Azure storage and converts it to a pandas dataframe.

    :param auth_client: AuthenticationClient for target storage.
    :param path: Path to delta table, in HDFS format: abfss://container@account.dfs.core.windows.net/my/path
    :param version: Optional version to read. Defaults to latest.
    :param row_filter: Optional filter to apply, as pyarrow expression. Example:
      from pyarrow.dataset import field as pyarrow_field

      filter = (pyarrow_field("year") == "2021") & (pyarrow_field("value") > "4")

    :param columns: Optional list of columns to select when reading. Defaults to all columns of not provided.
    :param batch_size: Optional batch size when reading in batches. If not set, whole table will be loaded into memory.
    :param partition_filter_expressions: Optional partitions filters. Examples:

       partition_filter_expressions = [("day", "=", "3")]
       partition_filter_expressions = [("day", "in", ["3", "20"])]
       partition_filter_expressions = [("day", "not in", ["3", "20"]), ("year", "=", "2021")]

    :return: A DeltaTable wrapped Rust class, pandas Dataframe or an iterator of pandas Dataframes, for batched reads.
    """
    pyarrow_ds = DeltaTable(
        path.to_delta_rs_path(), version=version, storage_options=auth_client.connect_storage(path)
    ).to_pyarrow_dataset(
        partitions=partition_filter_expressions,
        parquet_read_options=ParquetReadOptions(coerce_int96_timestamp_unit="ms"),
        filesystem=auth_client.get_pyarrow_filesystem(path),
    )

    row_filter = (
        compile_expression(row_filter, ArrowFilterExpression) if isinstance(row_filter, Expression) else row_filter
    )

    if batch_size:
        batches: Iterator[RecordBatch] = pyarrow_ds.to_batches(
            filter=row_filter, columns=columns, batch_size=batch_size
        )

        return map(lambda batch: batch.to_pandas(timestamp_as_object=True), batches)

    pyarrow_table: Table = pyarrow_ds.to_table(filter=row_filter, columns=columns)

    return pyarrow_table.to_pandas(timestamp_as_object=True)


def history(auth_client: AuthenticationClient, path: DataPath, limit: Optional[int] = 1) -> Iterable[DeltaTransaction]:
    """
      Returns transaction history for the table under path.

    :param auth_client: AuthenticationClient for target storage.
    :param path: Path to delta table, in HDFS format: abfss://container@account.dfs.core.windows.net/my/path
    :param limit: Limit number of history records retrieved.
    :return: An iterable of Delta transactions for this table.
    """
    delta_table = DeltaTable(path.to_delta_rs_path(), storage_options=auth_client.connect_storage(path))

    return [DeltaTransaction.from_dict(tran) for tran in delta_table.history(limit=limit)]


def get_cache_key(
    auth_client: AuthenticationClient,
    path: DataPath,
    batch_size=1000,
    version: Optional[int] = None,
    row_filter: Optional[Expression] = None,
    columns: Optional[List[str]] = None,
    partition_filter_expressions: Optional[List[Tuple]] = None,
) -> str:
    """
      Returns a cache key for the path and data read arguments

    :param auth_client: AuthenticationClient for target storage.
    :param path: Path to delta table, in HDFS format: abfss://container@account.dfs.core.windows.net/my/path
    :param version: Optional version to read. Defaults to latest.
    :param row_filter: Optional filter to apply, as pyarrow expression. Example:
      from pyarrow.dataset import field as pyarrow_field

      filter = (pyarrow_field("year") == "2021") & (pyarrow_field("value") > "4")

    :param columns: Optional list of columns to select when reading. Defaults to all columns of not provided.
    :param partition_filter_expressions: Optional partitions filters. Examples:

           partition_filter_expressions = [("day", "=", "3")]
           partition_filter_expressions = [("day", "in", ["3", "20"])]
           partition_filter_expressions = [("day", "not in", ["3", "20"]), ("year", "=", "2021")]

    :param batch_size: Batch size used to read table in batches.
    :return: A cache key (string)
    """
    base_attributes = []
    if version:
        base_attributes.append(str(version))
    else:
        base_attributes.append(str(list(history(auth_client, path))[0].timestamp))

    if row_filter is not None:
        base_attributes.append(str(row_filter))
    if columns:
        base_attributes.extend(columns)
    if partition_filter_expressions:
        base_attributes.append(str(partition_filter_expressions))

    base_attributes.append(str(batch_size))

    return hashlib.md5("#".join([path.to_delta_rs_path(), "_".join(base_attributes)]).encode("utf-8")).hexdigest()


def load_cached(  # pylint: disable=R0913
    auth_client: AuthenticationClient,
    path: DataPath,
    cache: KeyValueCache,
    cache_expires_after: Optional[datetime.timedelta] = datetime.timedelta(hours=1),
    batch_size=1000,
    version: Optional[int] = None,
    row_filter: Optional[Expression] = None,
    columns: Optional[List[str]] = None,
    partition_filter_expressions: Optional[List[Tuple]] = None,
    logger: Optional[SemanticLogger] = None,
) -> DataFrame:
    """
     Loads Delta Lake table from an external cache and converts it to a single pandas dataframe (after applying column projections and row filters).
     If a cache entry is missing, falls back to reading data from storage path.

    :param auth_client: AuthenticationClient for target storage.
    :param path: Path to delta table, in HDFS format: abfss://container@account.dfs.core.windows.net/my/path
    :param version: Optional version to read. Defaults to latest.
    :param row_filter: Optional filter to apply, as pyarrow expression. Example:
      from pyarrow.dataset import field as pyarrow_field

      filter = (pyarrow_field("year") == "2021") & (pyarrow_field("value") > "4")

    :param columns: Optional list of columns to select when reading. Defaults to all columns of not provided.
    :param partition_filter_expressions: Optional partitions filters. Examples:

           partition_filter_expressions = [("day", "=", "3")]
           partition_filter_expressions = [("day", "in", ["3", "20"])]
           partition_filter_expressions = [("day", "not in", ["3", "20"]), ("year", "=", "2021")]

    :param batch_size: Batch size used to read table in batches.
    :param cache: Optional cache store to read the version from. If not supplied, data will be read from the path. If supplied and cached entry is not present, data will be read from storage and saved in cache.
    :param cache_expires_after: Optional time to live for a cached table entry. Defaults to 1 hour.
    :param cache_exceptions: Optional additional exceptions on cache level to ignore.
    :param logger: Optional logger for debugging purposes.
    :return: A DeltaTable wrapped Rust class, pandas Dataframe or an iterator of pandas Dataframes, for batched reads.
    """

    cache_key = get_cache_key(
        auth_client=auth_client,
        path=path,
        batch_size=batch_size,
        version=version,
        row_filter=row_filter,
        columns=columns,
        partition_filter_expressions=partition_filter_expressions,
    )

    if logger:
        logger.debug(
            "Generated cache key {cache_key} for {table_path}",
            cache_key=cache_key,
            table_path=path.to_delta_rs_path(),
        )

    # first check that we have cached batches for all given inputs (columns, filters etc.)
    # we read a special cache entry which tells us number of cached batches for this table query
    if cache.exists(cache_key, "completed"):
        if logger:
            logger.debug("Cache hit for {cache_key}", cache_key=cache_key)

        try:
            return concat(
                [
                    DataFrameParquetSerializationFormat().deserialize(zlib.decompress(cached_batch))
                    for batch_key, cached_batch in cache.get(cache_key, is_map=True).items()
                    if batch_key != b"completed"
                ]
            )
        except (
            pyarrow.ArrowInvalid,
            ValueError,
            pyarrow.ArrowException,
            ConnectionError,
            ConnectionResetError,
            ConnectionAbortedError,
            ConnectionRefusedError,
        ) as ex:
            if logger:
                logger.warning(
                    "Error when reading data from cache - most likely some cache entries have been evicted. Falling back to storage.",
                    exception=ex,
                )

    if logger:
        logger.debug("Cache miss for {cache_key}, populating cache.", cache_key=cache_key)

    data = load(
        auth_client=auth_client,
        path=path,
        version=version,
        row_filter=row_filter,
        columns=columns,
        batch_size=batch_size,
        partition_filter_expressions=partition_filter_expressions,
    )

    aggregate_batch = concat(
        [
            cache.include(
                key=cache_key,
                attribute=str(batch_index),
                value=zlib.compress(DataFrameParquetSerializationFormat().serialize(batch)),
            )
            for batch_index, batch in enumerate(data)
        ],
        ignore_index=True,
        copy=False,
    )

    # we add a 'completion' indicator to this cached key so clients that now safely read the value
    # by doing it this way, we avoid doing a transaction - thus this method is non-blocking
    cache.include(key=cache_key, attribute="completed", value=1)
    cache.set_expiration(cache_key, cache_expires_after)

    if logger:
        logger.debug(
            "Cache updated for {cache_key}, total rows {row_count}",
            cache_key=cache_key,
            row_count=len(aggregate_batch),
        )

    return aggregate_batch
