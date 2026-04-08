"""
Functions to build tables.
"""

import pickle

import pandas as pd

from absence_and_crime.config import PATHS


def table_1() -> None:
    """
    Build table 1.
    NOTE: Requires `temp/covariates.parquet`, `temp/panels.pkl` and àbsence.parquet`.
    Save table to `output/tables/Table 1.html`.
    """

    paths = PATHS

    # Load data
    absence = pd.read_parquet(paths.temp / "absence.parquet")
    covariates = pd.read_parquet(paths.temp / "covariates.parquet")
    with open(paths.temp / "panels.pkl", "rb") as f:
        panels = pickle.load(f)

    # Set up data
    df = (
        covariates.merge(  # add grade at time of crime
            panels["crime"]
            .loc[lambda d: d.event_time.le(0), ["pnr", "event_time", "grade"]]
            .sort_values(["pnr", "event_time"])
            .groupby("pnr", as_index=False)
            .last()[["pnr", "grade"]],
            on=["pnr"],
            how="left",
            validate="1:1",
        )
        .merge(  # add absence rate
            absence.groupby("pnr")["abs_rate"].mean().reset_index(),
            on="pnr",
            how="left",
            validate="1:1",
        )
        .assign(  # define variables
            crime=lambda d: d.pnr.isin(set(panels["crime"]["pnr"])),
            incar=lambda d: d.pnr.isin(set(panels["incarceration"]["pnr"])),
            # experienced crime in grades K-2
            k_to_2=lambda d: d.grade.le(2).where(d.grade.notna()),
            # parent college degree or higher
            college=lambda d: d.education.eq(4).where(d.education.notna()),
        )
    )

    # Set up table
    vl = [
        "female",
        "k_to_2",
        "non_danish",
        "wages",
        "assets",
        "fulltime",
        "college",
        "abs_rate",
    ]
    names = {
        "female": "Female",
        "k_to_2": "Grade K-2 at crime",
        "non_danish": "1st or 2nd gen immigrant",
        "wages": "earnings",
        "assets": "Net assets",
        "fulltime": "F-T employment",
        "college": "College or higher",
        "abs_rate": "Absence rate",
    }
    stats = ["mean", "std"]
    groups = ["Crime", "Incarceration", "None"]
    conds = [df.crime, df.incar, ~df.crime]

    tbl = []
    for c, g in zip(conds, groups):
        block = df.loc[c, vl].agg(stats).transpose().round(2)
        block.columns = pd.MultiIndex.from_product([[g], block.columns])
        nobs = pd.DataFrame(
            [[len(df[c])] + [None] * (len(stats) - 1)],
            columns=block.columns,
            index=["N"],
        )
        block = pd.concat([block, nobs], axis=0)
        tbl.append(block)

    tbl = pd.concat(tbl, axis=1)
    tbl = tbl.rename(index=names)

    tbl.to_html(paths.tables / "Table 1.html")
