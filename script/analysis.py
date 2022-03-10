# import modules
import pandas as pd
import numpy as np
from pathlib import Path
from scipy.stats import zscore
import plotly.express as px

# file i/o
INPUT_PATH = "../input/"
OUTPUT_PATH = "../output/"

def read_files(source):
    """
    Load multiple excel files into one pandas dataframe
    :param source: source path
    :return: dataframe
    """

    # read files
    path = Path(source)
    # create empty dataframe
    df = pd.DataFrame()
    # iterate all files
    for file in path.iterdir():
        df = df.append(pd.read_excel(file, engine="openpyxl"))

    return df

def detect_anomaly(df):
    """
    detect anomalous transaction based on z-score
    :param df: input dataframe
    :return anomalous dataframe
    """
    aggfunc = ["count", "sum"]
    df = df.groupby(["User", "Date of Transaction", "Transaction ID"])["USD Net Amount"] \
            .agg(aggfunc).reset_index()
    df["z_score"] = df.groupby(["User"])["sum"].transform(lambda x : zscore(x, ddof=1))
    df = df.loc[abs(df["z_score"]) > 3]
    return df

if __name__ == "__main__":
    df = read_files(INPUT_PATH)
    anomaly = detect_anomaly(df)
    print(anomaly.head())
