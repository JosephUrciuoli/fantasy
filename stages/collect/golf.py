from .data_golf import create_data_golf_df
import pandas as pd


def create_golf_dataset(use_csvs: bool) -> None:
    create_data_golf_df(use_csvs)
    df = _combine()
    df = _add_ordering(df)
    df.to_csv("data/processed/golf.csv", index=False)


def _combine() -> pd.DataFrame:
    df = pd.read_csv("data/interim/data_golf_final.csv")
    # eventually join other CSVS
    return df


def _add_ordering(df: pd.DataFrame) -> pd.DataFrame:
    # for cross validation later down the line, we'll want to
    # add a column to ensure we train on only earlier data
    # TODO: currently incorrect
    order_df = df[["event_id", "year"]].copy()
    order_df = order_df.drop_duplicates(subset=["event_id", "year"]).sort_values(
        by=["year", "event_id"]
    )
    order_df["order"] = range(len(order_df))
    df = df.merge(order_df, on=["event_id", "year"], how="left")
    print(df.columns)
    return df
