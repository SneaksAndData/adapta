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
    assert metaframe.to_pandas().equals(pandas.DataFrame({"A": [1, 2, 3]}))
    assert metaframe.to_polars().equals(polars.DataFrame({"A": [1, 2, 3]}))


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
            [metaframe1.clone(), metaframe2.clone()],  # list
            polars.DataFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
        (
            (mf for mf in [metaframe1.clone(), metaframe2.clone()]),  # generator
            polars.DataFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
        (
            (metaframe1.clone(), metaframe2.clone()),  # tuple
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
            [metaframe1.clone(), metaframe2.clone()],  # list
            pandas.DataFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
        (
            (mf for mf in [metaframe1.clone(), metaframe2.clone()]),  # generator
            pandas.DataFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
        (
            (metaframe1.clone(), metaframe2.clone()),  # tuple
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


@pytest.mark.parametrize(
    "dataframes,expected_pandas, expected_polars",
    [
        (
            [metaframe1.clone(), metaframe2.clone()],  # list
            pandas.DataFrame({"A": [1, 2, 3, 4, 5, 6]}),
            polars.DataFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
    ],
)
def test_concat(dataframes, expected_pandas, expected_polars):
    """
    Test the concat method for pandas dataframes and the PandasOptions.
    """

    metaframe1_ = concat(dataframes=dataframes, options=[PandasOptions(ignore_index=True)])
    metaframe2_ = metaframe1.clone()
    assert metaframe1_.to_pandas().equals(expected_pandas)
    assert metaframe2_.to_polars().equals(expected_polars)


def test_from_df():
    """
    Test the from_pandas and from_polars methods.
    """
    pandas_df = pandas.DataFrame({"A": [1, 2, 3]})
    polars_df = polars.DataFrame({"A": [1, 2, 3]})
    metaframe_pandas = MetaFrame.from_pandas(pandas_df)
    metaframe_polars = MetaFrame.from_polars(polars_df)
    assert metaframe_pandas.to_pandas().equals(pandas_df)
    assert metaframe_polars.to_polars().equals(polars_df)
    assert metaframe_pandas.to_polars().equals(polars_df)
    assert metaframe_polars.to_pandas().equals(pandas_df)
