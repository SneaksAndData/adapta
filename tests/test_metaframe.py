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
    metaframe = concat([metaframe1, metaframe2], options=[PandasOptions(ignore_index=True)])
    assert metaframe.to_pandas().equals(pandas.DataFrame({"A": [1, 2, 3, 4, 5, 6]}))
    assert metaframe.to_polars().equals(polars.DataFrame({"A": [1, 2, 3, 4, 5, 6]}))
