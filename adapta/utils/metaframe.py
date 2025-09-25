"""
This module contains the MetaFrame class which contains structured data for a dataframe.
The MetaFrame can be used to convert the latent representation to other formats.
"""
import itertools
from abc import ABC
from collections.abc import Callable, Iterable

import pandas
import polars
import pyarrow


class MetaFrameOptions(ABC):
    """
    Base class for MetaFrame options.
    """

    def __init__(self, **kwargs):
        self.kwargs = kwargs


class PandasOptions(MetaFrameOptions):
    """
    Options for Pandas operations.
    """


class PolarsOptions(MetaFrameOptions):
    """
    Options for Polars operations.
    """


class MetaFrame:
    """
    MetaFrame class which contains structured data for a dataframe.
    The MetaFrame can be used to convert the latent representation to other formats.
    """

    def __init__(
        self,
        data: any,
        convert_to_polars: Callable[[any], polars.DataFrame],
        convert_to_pandas: Callable[[any], pandas.DataFrame],
    ):
        self._materialized = False
        self._data = data
        self._convert_to_polars = convert_to_polars
        self._convert_to_pandas = convert_to_pandas

    @classmethod
    def from_pandas(
        cls, data: pandas.DataFrame, convert_to_polars: Callable[[any], polars.DataFrame] | None = None
    ) -> "MetaFrame":
        """
        Create a MetaFrame from a pandas DataFrame.

        :param data: Pandas DataFrame
        :param convert_to_polars: Override default function to convert to polars DataFrame
        :return: MetaFrame
        """
        return cls(
            data=data,
            convert_to_polars=convert_to_polars or polars.DataFrame,
            convert_to_pandas=lambda x: x,
        )

    @classmethod
    def from_polars(
        cls, data: polars.DataFrame, convert_to_pandas: Callable[[any], pandas.DataFrame] | None = None
    ) -> "MetaFrame":
        """
        Create a MetaFrame from a Polars DataFrame.

        :param data: Polars DataFrame
        :param convert_to_pandas: Override default function to convert to pandas DataFrame
        :return: MetaFrame
        """
        return cls(
            data=data,
            convert_to_polars=lambda x: x,
            convert_to_pandas=convert_to_pandas or (lambda x: x.to_pandas()),
        )

    @classmethod
    def from_arrow(
        cls,
        data: pyarrow.Table,
        convert_to_polars: Callable[[any], polars.DataFrame] | None = None,
        convert_to_pandas: Callable[[any], pandas.DataFrame] | None = None,
    ) -> "MetaFrame":
        """
        Create a MetaFrame from an Arrow Table.

        :param data: Arrow Table
        :param convert_to_polars: Override default function to convert to polars DataFrame
        :param convert_to_pandas: Override default function to convert to pandas DataFrame
        :return: MetaFrame
        """
        return cls(
            data=data,
            convert_to_polars=convert_to_polars or polars.from_arrow,
            convert_to_pandas=convert_to_pandas or (lambda x: x.to_pandas()),
        )

    def _check_if_materialized(self) -> None:
        if self._materialized:
            raise RuntimeError(
                "MetaFrame has already been materialized. You can only call 'to_pandas' or 'to_polars' once."
            )

        self._materialized = True

    def to_pandas(self) -> pandas.DataFrame:
        """
        Convert the MetaFrame to a pandas DataFrame.
        """
        self._check_if_materialized()
        return self._convert_to_pandas(self._data)

    def to_polars(self) -> polars.DataFrame:
        """
        Convert the MetaFrame to a Polars DataFrame.
        """
        self._check_if_materialized()
        return self._convert_to_polars(self._data)


def concat(dataframes: Iterable[MetaFrame], options: Iterable[MetaFrameOptions] | None = None) -> MetaFrame:
    """
    Concatenate a list of MetaFrames.
    :param dataframes: List of MetaFrames to concatenate.
    :param options: Options for the concatenation.
    :return: Concatenated MetaFrame.
    """

    dataframes_iter = iter(dataframes)
    first = next(dataframes_iter, None)
    if first is None:
        return MetaFrame(
            data=[],
            convert_to_polars=lambda _: polars.DataFrame(),
            convert_to_pandas=lambda _: pandas.DataFrame(),
        )

    dataframes = itertools.chain([first], dataframes_iter)

    if options is None:
        options = []

    return MetaFrame(
        data=dataframes,
        convert_to_polars=lambda data: polars.concat(
            map(lambda df: df.to_polars(), data),
            **{
                k: v
                for options_object in options
                for k, v in options_object.kwargs.items()
                if isinstance(options_object, PolarsOptions)
            }
        ),
        convert_to_pandas=lambda data: pandas.concat(
            map(lambda df: df.to_pandas(), data),
            **{
                k: v
                for options_object in options
                for k, v in options_object.kwargs.items()
                if isinstance(options_object, PandasOptions)
            }
        ),
    )
