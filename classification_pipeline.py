"""Prepare the data for the classification model."""
import polars as pl
from sklearn.feature_selection import VarianceThreshold
from sklearn.preprocessing import OrdinalEncoder


# %%
from sklearn.preprocessing import MaxAbsScaler
from sklearn.pipeline import make_pipeline
from sklearn.compose import ColumnTransformer
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.impute import SimpleImputer
import numpy as np


class PolarsTransformer(BaseEstimator, TransformerMixin):
    """Custom transformer for selecting the numerical and categorical data."""
    def __init__(self, df=None):
        self.df = df

    def transform(self, X: pl.DataFrame, y=None) -> pl.DataFrame:
        pipe_df = X.with_columns(
            pl.col(pl.Boolean).cast(pl.String)
        )
        pipe_df = pipe_df.with_columns(pl.col(pl.String).fill_null('0'))

        return pipe_df

    def cat_columns(self, X: pl.DataFrame) -> list:
        cat_df = self.transform(X)
        return cat_df.select(pl.col(pl.String)).columns

    def num_columns(self, X: pl.DataFrame) -> list:
        num_df = self.transform(X)
        return num_df.select(pl.all().exclude(pl.String)).columns


def cat_pipiline(df: pl.DataFrame) -> pl.DataFrame:
    """Pipeline for classification model."""
    cat_attribs = PolarsTransformer().cat_columns(df)

    cat_pipeline = make_pipeline(

        OrdinalEncoder(dtype=np.int64).set_output(transform='polars')

    )

    num_attribs = PolarsTransformer().num_columns(df)

    num_pipeline = make_pipeline(
        VarianceThreshold(threshold=0.8).set_output(transform='polars'),
        SimpleImputer(strategy="median").set_output(transform='polars'),
        MaxAbsScaler().set_output(transform='polars')
    )
    preprocessing = ColumnTransformer([
        ("num", num_pipeline, num_attribs),
        ("cat", cat_pipeline, cat_attribs),
    ]).set_output(transform='polars')

    df_prepared = (preprocessing.fit_transform(PolarsTransformer().transform(df))).select(pl.all().shrink_dtype())

    return df_prepared

# %%
