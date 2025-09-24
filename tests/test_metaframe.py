import pandas
import polars

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


def test_concat():
    """
    Test the concat method and the PandasOptions.
    """
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
    metaframe = concat(dataframes=[metaframe1, metaframe2], options=[PandasOptions(ignore_index=True)])
    assert metaframe.to_pandas().equals(pandas.DataFrame({"A": [1, 2, 3, 4, 5, 6]}))
    assert metaframe.to_polars().equals(polars.DataFrame({"A": [1, 2, 3, 4, 5, 6]}))


def test_concat_with_generator():
    """
    Test the concat method with a generator instead of a list.
    """
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

    # Create a generator instead of a list
    metaframes_generator = (mf for mf in [metaframe1, metaframe2])

    metaframe = concat(metaframes_generator, options=[PandasOptions(ignore_index=True)])
    assert metaframe.to_pandas().equals(pandas.DataFrame({"A": [1, 2, 3, 4, 5, 6]}))
    assert metaframe.to_polars().equals(polars.DataFrame({"A": [1, 2, 3, 4, 5, 6]}))


def test_empty_concat():
    """
    Test the concat method on empty dataframes.
    """

    metaframe = concat(dataframes=[])
    assert metaframe.to_pandas().equals(pandas.DataFrame())
    assert metaframe.to_polars().equals(polars.DataFrame())


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
