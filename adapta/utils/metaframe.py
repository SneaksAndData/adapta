"""
This module contains the MetaFrame class which contains structured data for a dataframe.
The MetaFrame can be used to convert the latent representation to other formats.
"""
from abc import ABC
from typing import Callable, Iterable, Optional

import pandas
import polars


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

    pass


class PolarsOptions(MetaFrameOptions):
    """
    Options for Polars operations.
    """

    pass


class MetaFrame:
    """
    MetaFrame class which contains structured data for a dataframe.
    """

    def __init__(
        self,
        data: any,
        convert_to_polars: Callable[[any], polars.DataFrame],
        convert_to_pandas: Callable[[any], pandas.DataFrame],
    ):
        self.data = data
        self.convert_to_polars = convert_to_polars
        self.convert_to_pandas = convert_to_pandas

    @classmethod
    def from_pandas(cls, data: pandas.DataFrame) -> "MetaFrame":
        """
        Create a MetaFrame from a pandas DataFrame.
        """
        return cls(
            data=data,
            convert_to_polars=lambda x: polars.DataFrame(x),
            convert_to_pandas=lambda x: x,
        )

    @classmethod
    def from_polars(cls, data: polars.DataFrame) -> "MetaFrame":
        """
        Create a MetaFrame from a Polars DataFrame.
        """
        return cls(
            data=data,
            convert_to_polars=lambda x: x,
            convert_to_pandas=lambda x: x.to_pandas(),
        )

    def to_pandas(self) -> pandas.DataFrame:
        """
        Convert the MetaFrame to a pandas DataFrame.
        """
        return self.convert_to_pandas(self.data)

    def to_polars(self) -> polars.DataFrame:
        """
        Convert the MetaFrame to a Polars DataFrame.
        """
        return self.convert_to_polars(self.data)


def concat(dataframes: Iterable[MetaFrame], options: Optional[Iterable[MetaFrameOptions]] = None) -> MetaFrame:
    """
    Concatenate a list of MetaFrames.
    :param dataframes: List of MetaFrames to concatenate.
    :param options: Options for the concatenation.
    :return: Concatenated MetaFrame.
    """
    if options is None:
        options = []

    return MetaFrame(
        data=dataframes,
        convert_to_polars=lambda data: polars.concat(
            [df.to_polars() for df in data],
            **{
                k: v
                for options_object in options
                for k, v in options_object.kwargs.items()
                if isinstance(options_object, PolarsOptions)
            }
        ),
        convert_to_pandas=lambda data: pandas.concat(
            [df.to_pandas() for df in data],
            **{
                k: v
                for options_object in options
                for k, v in options_object.kwargs.items()
                if isinstance(options_object, PandasOptions)
            }
        ),
    )
