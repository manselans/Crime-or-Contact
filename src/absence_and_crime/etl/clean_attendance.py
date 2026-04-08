"""
Gather and clean attendance records.
Save cleaned records to `temp/absence.parquet`.
"""

from pathlib import Path
import pandas as pd

from absence_and_crime.config import PATHS


def run() -> None:
    """
    Gather and clean attendance records.
    Save cleaned records to `temp/absence.parquet`.
    """

    paths = PATHS

    # list of recorded school years ('11-'15 cover 2 years each)
    years = [2011, 2013, 2015, 2017, 2018, 2019, 2020, 2021, 2022, 2023]

    # paths to data sets
    p1 = paths.data.dst_raw / Path(r"Eksterne data\STIL\Trivsel_fravaer_nov_2018")
    p2 = paths.data.dst_raw / Path(r"Eksterne data\STIL 2023")
    p3 = paths.data.dst_raw / Path(r"Eksterne data\STIL_2024")

    absence = {}
    for y in years:

        # ---- Load ----

        if y in [2011, 2013, 2015]:
            ny = y + 1 - 2000
            df = pd.read_sas(p1 / f"fravaer_{y}_{ny}.sas7bdat", format="sas7bdat")
        elif y in [2017, 2019]:
            df = pd.read_sas(p1 / f"fravaer_{y}.sas7bdat", format="sas7bdat")
        elif y in [2018, 2020, 2023]:
            df = pd.read_sas(p3 / f"fravaer_{y}.sas7bdat", format="sas7bdat")
        elif y in [2021, 2022]:
            df = pd.read_sas(p2 / f"afid_fravaer_{y}.sas7bdat", format="sas7bdat")
        else:
            raise ValueError(f"Unexpected year: {y}")

        # ---- Basic Editing ----

        # Renaming variables
        df.columns = df.columns.str.lower()

        if "cprnummer" in df.columns:
            df.rename(columns={"cprnummer": "pnr"}, inplace=True)

        # Only regular schools, grades 0.-9.
        df = df[
            (df["insttype"].str.decode("utf-8") == "1012")
            & (df["klassetrin"] >= 0)
            & (df["klassetrin"] <= 9)
        ]

        # Define absenteeism
        df["abs_rate"] = (df["dageialtfra"] / df["dageaktiv"] * 100).clip(
            lower=0, upper=100
        )

        # Selecting variables
        vl = ["pnr", "maaned", "klassetrin", "abs_rate"]
        df = df[vl]

        # Making dates monthly
        df["maaned"] = pd.to_datetime(df["maaned"], format="%Y%m").dt.to_period("M")

        # Destringing
        df["pnr"] = pd.to_numeric(df["pnr"])

        absence[y] = df
        del df

    # Combine all years
    absence = pd.concat(absence.values(), ignore_index=True)

    # Make child-month observations unique
    absence = (
        absence.rename(columns={"maaned": "month", "klassetrin": "grade"})
        .groupby(["pnr", "month"], as_index=False)
        .agg({"grade": "last", "abs_rate": "max"})
    )

    # Limit to children observed since grade 0
    absence = absence.loc[lambda d: d["grade"].eq(0).groupby(d["pnr"]).transform("any")]

    # Limit observation period
    absence = absence[absence["month"] <= pd.Period("2021-06", freq="M")]

    # Save
    absence.to_parquet(paths.temp / "absence.parquet", index=False)


if __name__ == "__main__":
    run()
