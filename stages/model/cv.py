import pandas as pd


def get_golf_dataset() -> pd.DataFrame:
    df = pd.read_csv("data/processed/golf.csv")
    return df


def split_dataset(df: pd.DataFrame) -> tuple:
    # split into train and test
    max_order = df["order"].max()
    # last 2 tourneys
    validation_df = df[df["order"] >= max_order - 2]
    train_df = df[df["order"] < max_order - 2]
    return train_df, validation_df
