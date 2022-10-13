"""
 Operations on Delta Lake tables.
"""
import datetime
import hashlib
import time
from typing import Optional, Union, Iterator, List, Iterable

import pandas
from deltalake import DeltaTable
from pyarrow import RecordBatch, Table
from pyarrow._compute import Expression  # pylint: disable=E0611
from pyarrow._dataset_parquet import ParquetReadOptions  # pylint: disable=E0611

from proteus.security.clients._base import ProteusClient
from proteus.storage.models.base import DataPath
from proteus.storage.delta_lake._models import DeltaTransaction
from proteus.storage.cache import KeyValueCache
from proteus.storage.models.format import DataFrameParquetSerializationFormat


def load(  # pylint: disable=R0913
        proteus_client: ProteusClient,
        path: DataPath,
        version: Optional[int] = None,
        row_filter: Optional[Expression] = None,
        columns: Optional[List[str]] = None,
        batch_size: Optional[int] = None
) -> Union[DeltaTable, pandas.DataFrame, Iterator[pandas.DataFrame]]:
    """
     Loads Delta Lake table from Azure storage and converts it to a pandas dataframe.

    :param proteus_client: ProteusClient for target storage.
    :param path: Path to delta table, in HDFS format: abfss://container@account.dfs.core.windows.net/my/path
    :param version: Optional version to read. Defaults to latest.
    :param row_filter: Optional filter to apply, as pyarrow expression. Example:
      from pyarrow.dataset import field as pyarrow_field

      filter = (pyarrow_field("year") == "2021") & (pyarrow_field("value") > "4")

    :param columns: Optional list of columns to select when reading. Defaults to all columns of not provided.
    :param batch_size: Optional batch size when reading in batches. If not set, whole table will be loaded into memory.
    :return: A DeltaTable wrapped Rust class, pandas Dataframe or an iterator of pandas Dataframes, for batched reads.
    """
    pyarrow_ds = DeltaTable(path.to_delta_rs_path(), version=version,
                            storage_options=proteus_client.connect_storage(path)) \
        .to_pyarrow_dataset(parquet_read_options=ParquetReadOptions(coerce_int96_timestamp_unit="ms"),
                            filesystem=proteus_client.get_pyarrow_filesystem(path))

    if batch_size:
        batches: Iterator[RecordBatch] = pyarrow_ds.to_batches(filter=row_filter, columns=columns,
                                                               batch_size=batch_size)

        return map(lambda batch: batch.to_pandas(timestamp_as_object=True), batches)

    pyarrow_table: Table = pyarrow_ds.to_table(filter=row_filter, columns=columns)

    return pyarrow_table.to_pandas(timestamp_as_object=True)


def history(proteus_client: ProteusClient, path: DataPath, limit: Optional[int] = 1) -> Iterable[DeltaTransaction]:
    """
      Returns transaction history for the table under path.

    :param proteus_client: ProteusClient for target storage.
    :param path: Path to delta table, in HDFS format: abfss://container@account.dfs.core.windows.net/my/path
    :param limit: Limit number of history records retrieved.
    :return: An iterable of Delta transactions for this table.
    """
    delta_table = DeltaTable(path.to_delta_rs_path(), storage_options=proteus_client.connect_storage(path))

    return [DeltaTransaction.from_dict(tran) for tran in delta_table.history(limit=limit)]


def load_cached(  # pylint: disable=R0913
        proteus_client: ProteusClient,
        path: DataPath,
        cache: KeyValueCache,
        cache_expires_after: Optional[datetime.timedelta] = datetime.timedelta(hours=1),
        batch_size=1000,
        version: Optional[int] = None,
        row_filter: Optional[Expression] = None,
        columns: Optional[List[str]] = None,
) -> pandas.DataFrame:
    """
     Loads Delta Lake table from an external cache and converts it to a single pandas dataframe (after applying column projections and row filters).
     If a cache entry is missing, falls back to reading data from storage path.

    :param proteus_client: ProteusClient for target storage.
    :param path: Path to delta table, in HDFS format: abfss://container@account.dfs.core.windows.net/my/path
    :param version: Optional version to read. Defaults to latest.
    :param row_filter: Optional filter to apply, as pyarrow expression. Example:
      from pyarrow.dataset import field as pyarrow_field

      filter = (pyarrow_field("year") == "2021") & (pyarrow_field("value") > "4")

    :param columns: Optional list of columns to select when reading. Defaults to all columns of not provided.
    :param batch_size: Batch size used to read table in batches.
    :param cache: Optional cache store to read the version from. If not supplied, data will be read from the path. If supplied and cached entry is not present, data will be read from storage and saved in cache.
    :param cache_expires_after: Optional time to live for a cached table entry. Defaults to 1 hour.
    :return: A DeltaTable wrapped Rust class, pandas Dataframe or an iterator of pandas Dataframes, for batched reads.
    """
    base_attributes = []
    if version:
        base_attributes.append(str(version))
    if row_filter is not None:
        base_attributes.append(str(row_filter))
    if columns:
        base_attributes.extend(columns)

    base_attributes.append(str(batch_size))
    base_attributes.append(str(list(history(proteus_client, path))[0].version))

    base_cache_key = hashlib.md5(
        '#'.join([path.to_delta_rs_path(), '_'.join(base_attributes)]).encode('utf-8')).hexdigest()
    print(base_cache_key)

    # first check that we have cached batches for all given inputs (columns, filters etc.)
    # we read a special cache entry which tells us number of cached batches for this table query
    if cache.exists(f"{base_cache_key}_size"):
        max_batch_number = int(cache.get(f"{base_cache_key}_size"))

        return pandas.concat(
            [
                DataFrameParquetSerializationFormat().deserialize(cached_batch) for cached_batch
                in cache.multi_get([f"{base_cache_key}_{batch_number}" for batch_number in range(0, max_batch_number)])
            ]
        )

    aggregate_batch: Optional[pandas.DataFrame] = None
    data = load(
        proteus_client=proteus_client,
        path=path,
        version=version,
        row_filter=row_filter,
        columns=columns,
        batch_size=batch_size
    )

    batch_index = 0
    cache_start = time.monotonic_ns()
    for batch in data:
        cache.set(f"{base_cache_key}_{batch_index}", DataFrameParquetSerializationFormat().serialize(batch),
                  expires_after=cache_expires_after)

        aggregate_batch = pandas.concat([aggregate_batch, batch])
        batch_index += 1

    cache_duration = (time.monotonic_ns() - cache_start) / 1e9
    cache.set(f"{base_cache_key}_size", batch_index, expires_after=cache_expires_after - datetime.timedelta(seconds=cache_duration))

    return aggregate_batch
