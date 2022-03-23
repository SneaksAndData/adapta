"""
 Operations on Delta Lake tables.
"""
from typing import Optional, Union, Iterator, List, Tuple, Any

import pandas
import pyarrow
from deltalake import DeltaTable, RawDeltaTable
from deltalake.fs import DeltaStorageHandler
from pyarrow import RecordBatch, Table
from pyarrow._compute import Expression  # pylint: disable=E0611
from pyarrow._dataset import FileSystemDataset
from pyarrow._dataset_parquet import ParquetFileFormat, ParquetReadOptions
import pyarrow.fs as pa_fs

from proteus.security.clients._base import ProteusClient
from proteus.storage.models.base import DataPath


def _to_pyarrow_dataset(
        table: RawDeltaTable,
        schema: pyarrow.Schema,
        partitions: Optional[List[Tuple[str, str, Any]]] = None,
        filesystem: Optional[Union[str, pa_fs.FileSystem]] = None,
) -> pyarrow.dataset.Dataset:
    """
    Build a PyArrow Dataset using data from the DeltaTable.

    :param partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
    :param filesystem: A concrete implementation of the Pyarrow FileSystem or a fsspec-compatible interface. If None, the first file path will be used to determine the right FileSystem
    :return: the PyArrow dataset in PyArrow
    """
    if not filesystem:
        filesystem = pa_fs.PyFileSystem(
            DeltaStorageHandler(table.table_uri())
        )

    format = ParquetFileFormat(read_options=ParquetReadOptions(coerce_int96_timestamp_unit='ms'))

    fragments = [
        format.make_fragment(
            file,
            filesystem=filesystem,
            partition_expression=part_expression,
        )
        for file, part_expression in table.dataset_partitions(partitions)
    ]

    return FileSystemDataset(fragments, schema, format, filesystem)


def load(proteus_client: ProteusClient,  # pylint: disable=R0913
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
    proteus_client.connect_storage(path, set_env=True)
    delta_table = DeltaTable(path.to_delta_rs_path(), version=version)
    pyarrow_ds = _to_pyarrow_dataset(delta_table._table, delta_table.pyarrow_schema())

    if batch_size:
        batches: Iterator[RecordBatch] = pyarrow_ds.to_batches(filter=row_filter, columns=columns,
                                                               batch_size=batch_size)

        return map(lambda batch: batch.to_pandas(timestamp_as_object=True), batches)

    pyarrow_table: Table = pyarrow_ds.to_table(filter=row_filter, columns=columns)

    return pyarrow_table.to_pandas(timestamp_as_object=True)
