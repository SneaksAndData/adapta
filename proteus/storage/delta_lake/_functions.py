from typing import Optional, Union, Iterator, List

import pandas
from deltalake import DeltaTable
from pyarrow import RecordBatch, Table
from pyarrow._compute import Expression

from proteus.security.clients._base import ProteusClient
from proteus.storage.models.base import DataPath


def load(proteus_client: ProteusClient,
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

    if batch_size:
        batches: Iterator[RecordBatch] = delta_table.to_pyarrow_dataset().to_batches(filter=row_filter, columns=columns,
                                                                                     batch_size=batch_size)

        return map(lambda batch: batch.to_pandas(), batches)

    pyarrow_table: Table = delta_table.to_pyarrow_dataset().to_table(filter=row_filter, columns=columns)

    return pyarrow_table.to_pandas()
