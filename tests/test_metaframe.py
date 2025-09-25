from copy import deepcopy

import pandas
import polars
import pytest

from adapta.utils.metaframe import MetaFrame, concat, PandasOptions


def test_to_df():
    """
    Test the to_pandas and to_polars methods.
    """
    metaframe = MetaFrame(
        data={"A": [1, 2, 3]},
        convert_to_pandas=lambda x: pandas.DataFrame.from_dict(x),
        convert_to_polars=lambda x: polars.from_dict(x),
    )
    assert deepcopy(metaframe).to_pandas().equals(pandas.DataFrame({"A": [1, 2, 3]}))
    assert deepcopy(metaframe).to_polars().equals(polars.DataFrame({"A": [1, 2, 3]}))


metaframe1 = MetaFrame(
    data={"A": [1, 2, 3]},
    convert_to_pandas=lambda x: pandas.DataFrame.from_dict(x),
    convert_to_polars=lambda x: polars.from_dict(x),
)
metaframe2 = MetaFrame(
    data={"A": [4, 5, 6]},
    convert_to_pandas=lambda x: pandas.DataFrame.from_dict(x),
    convert_to_polars=lambda x: polars.from_dict(x),
)


@pytest.mark.parametrize(
    "dataframes,expected",
    [
        (
            [deepcopy(metaframe1), deepcopy(metaframe2)],  # list
            polars.DataFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
        (
            (mf for mf in [deepcopy(metaframe1), deepcopy(metaframe2)]),  # generator
            polars.DataFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
        (
            (deepcopy(metaframe1), deepcopy(metaframe2)),  # tuple
            polars.DataFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
        ([], polars.DataFrame()),  # empty list
    ],
)
def test_concat_polars(dataframes, expected):
    """
    Test the concat method for polars dataframes.
    """

    metaframe = concat(dataframes=dataframes)
    assert metaframe.to_polars().equals(expected)


@pytest.mark.parametrize(
    "dataframes,expected",
    [
        (
            [deepcopy(metaframe1), deepcopy(metaframe2)],  # list
            pandas.DataFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
        (
            (mf for mf in [deepcopy(metaframe1), deepcopy(metaframe2)]),  # generator
            pandas.DataFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
        (
            (deepcopy(metaframe1), deepcopy(metaframe2)),  # tuple
            pandas.DataFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
        (
            [],  # empty list
            pandas.DataFrame(),
        ),
    ],
)
def test_concat_pandas(dataframes, expected):
    """
    Test the concat method for pandas dataframes and the PandasOptions.
    """

    metaframe = concat(dataframes=dataframes, options=[PandasOptions(ignore_index=True)])
    assert metaframe.to_pandas().equals(expected)


def test_from_df():
    """
    Test the from_pandas and from_polars methods.
    """
    pandas_df = pandas.DataFrame({"A": [1, 2, 3]})
    polars_df = polars.DataFrame({"A": [1, 2, 3]})
    metaframe_pandas = MetaFrame.from_pandas(pandas_df)
    metaframe_polars = MetaFrame.from_polars(polars_df)
    assert deepcopy(metaframe_pandas).to_pandas().equals(pandas_df)
    assert deepcopy(metaframe_polars).to_polars().equals(polars_df)
    assert deepcopy(metaframe_pandas).to_polars().equals(polars_df)
    assert deepcopy(metaframe_polars).to_pandas().equals(pandas_df)


def test_materialized_check():
    """
    Test the _check_if_materialized method work accordingly. It should raise a runtime error the second time we call
    to_pandas or to_polars on the same MetaFrame instance.
    """
    metaframe = MetaFrame(
        data={"A": [1, 2, 3]},
        convert_to_pandas=lambda x: pandas.DataFrame.from_dict(x),
        convert_to_polars=lambda x: polars.from_dict(x),
    )
    assert metaframe.to_polars().equals(polars.DataFrame({"A": [1, 2, 3]}))
    assert pytest.raises(RuntimeError, metaframe.to_polars)
    assert pytest.raises(RuntimeError, metaframe.to_pandas)
