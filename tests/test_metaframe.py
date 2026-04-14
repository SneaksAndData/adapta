from copy import deepcopy

import pandas
import polars
import polars.testing
import pytest

from adapta.utils.metaframe import MetaFrame, concat, PandasOptions


def test_to_df():
    """
    Test the to_pandas, to_polars, and to_polars_lazy methods.
    """
    metaframe = MetaFrame(
        data={"A": [1, 2, 3]},
        convert_to_pandas=lambda x: pandas.DataFrame.from_dict(x),
        convert_to_polars=lambda x: polars.from_dict(x),
        convert_to_polars_lazy=lambda x: polars.from_dict(x).lazy(),
    )
    assert deepcopy(metaframe).to_pandas().equals(pandas.DataFrame({"A": [1, 2, 3]}))
    assert deepcopy(metaframe).to_polars().equals(polars.DataFrame({"A": [1, 2, 3]}))
    assert (
        polars.testing.assert_frame_equal(deepcopy(metaframe).to_polars_lazy(), polars.LazyFrame({"A": [1, 2, 3]}))
        is None
    )


metaframe1 = MetaFrame(
    data={"A": [1, 2, 3]},
    convert_to_pandas=lambda x: pandas.DataFrame.from_dict(x),
    convert_to_polars=lambda x: polars.from_dict(x),
    convert_to_polars_lazy=lambda x: polars.from_dict(x).lazy(),
)
metaframe2 = MetaFrame(
    data={"A": [4, 5, 6]},
    convert_to_pandas=lambda x: pandas.DataFrame.from_dict(x),
    convert_to_polars=lambda x: polars.from_dict(x),
    convert_to_polars_lazy=lambda x: polars.from_dict(x).lazy(),
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
            polars.LazyFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
        (
            (mf for mf in [deepcopy(metaframe1), deepcopy(metaframe2)]),  # generator
            polars.LazyFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
        (
            (deepcopy(metaframe1), deepcopy(metaframe2)),  # tuple
            polars.LazyFrame({"A": [1, 2, 3, 4, 5, 6]}),
        ),
        ([], polars.LazyFrame()),  # empty list
    ],
)
def test_concat_polars_lazy(dataframes, expected):
    """
    Test the concat method for polars lazyframes.
    """

    metaframe = concat(dataframes=dataframes)
    assert polars.testing.assert_frame_equal(metaframe.to_polars_lazy(), expected) is None


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
    Test the from_pandas, from_polars, and from_polars_lazy methods.
    """
    pandas_df = pandas.DataFrame({"A": [1, 2, 3]})
    polars_df = polars.DataFrame({"A": [1, 2, 3]})
    polars_lazy_df = polars.LazyFrame({"A": [1, 2, 3]})
    metaframe_pandas = MetaFrame.from_pandas(pandas_df)
    metaframe_polars = MetaFrame.from_polars(polars_df)
    metaframe_lazy = MetaFrame.from_polars_lazy(polars_lazy_df)
    metaframes = [metaframe_pandas, metaframe_polars, metaframe_lazy]
    assert all(deepcopy(metaframe).to_pandas().equals(pandas_df) for metaframe in metaframes)
    assert all(deepcopy(metaframe).to_polars().equals(polars_df) for metaframe in metaframes)
    assert all(
        polars.testing.assert_frame_equal(deepcopy(metaframe).to_polars_lazy(), polars_lazy_df) is None
        for metaframe in metaframes
    )


def test_materialized_check():
    """
    Test the _check_if_materialized method work accordingly. It should raise a runtime error the second time we call
    to_pandas or to_polars on the same MetaFrame instance.
    """
    metaframe = MetaFrame(
        data={"A": [1, 2, 3]},
        convert_to_pandas=lambda x: pandas.DataFrame.from_dict(x),
        convert_to_polars=lambda x: polars.from_dict(x),
        convert_to_polars_lazy=lambda x: polars.from_dict(x).lazy(),
    )
    assert metaframe.to_polars().equals(polars.DataFrame({"A": [1, 2, 3]}))
    with pytest.raises(RuntimeError):
        metaframe.to_polars()
    with pytest.raises(RuntimeError):
        metaframe.to_pandas()
    with pytest.raises(RuntimeError):
        metaframe.to_polars_lazy()
