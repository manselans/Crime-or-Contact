"""
Create panels of monthly absence around first parent crime and contact.
NOTE: Requires dict of criminal records to exist in `temp/records.pkl`.
Save panels to a dict of DataFrames in `temp/panels.pkl`.
"""

import pickle

import pandas as pd

from absence_and_crime.config import PATHS
from absence_and_crime.transforms import age_from_months


def run() -> None:
    """
    Create panels of monthly absence around first parent crime and contact.
    NOTE: Requires dict of criminal records to exist in `temp/records.pkl`.
    Save panels to a dict of DataFrames in `temp/panels.pkl`.
    """

    paths = PATHS

    # Load data
    absence = pd.read_parquet(paths.temp / "absence.parquet")
    families = pd.read_parquet(paths.temp / "families.parquet")
    with open(paths.temp / "records.pkl", "rb") as f:
        records = pickle.load(f)

    records = { # in_prison records spells not events
        k: v for k, v in records.items() if k not in ["in_prison"]
    }

    panels = {
        event: (
            # Set up panel around event
            _find_first_event(records=record, families=families)
            .merge(absence, on="pnr", how="left")
            .assign(event_time=lambda d: d.month.astype("int64")-d.t0.astype("int64"))

            # Require that child is observed pre- and post-event
            .loc[
                lambda d: d.event_time.between(-11, -1).groupby(d["pnr"]).transform("any")
                & d.event_time.between(0, 11).groupby(d["pnr"]).transform("any")
            ]
            .reset_index(drop=True)
        )
        for event, record in records.items()
    }

    # Restrict other panels to be a subset of crime panel
    ids = set(panels["crime"].pnr)
    for k, v in panels.items():
        if k != "crime":
            panels[k] = v.loc[lambda d: d.pnr.isin(ids)]

    # Add age in years to panels
    for k, v in panels.items():
        v["age"] = age_from_months(v.date_of_birth, v.month)

    with open(paths.temp / "panels.pkl", "wb") as f:
        pickle.dump(panels, f)


def _find_first_event(
    records: pd.DataFrame, families: pd.DataFrame
) -> pd.DataFrame:
    """
    Finds a parent's earliest criminal record since child started school.

    Parameters
    ----------
    records
        DataFrame of a given type of monthly criminal records.
    families
        DataFrame of unique child-parent relationships and their active period.

    Returns
    -------
    first_event
        DataFrame of children's first parental criminal record since starting school.
    """

    first_event = (
        families
        # Keep children w. parent w. a given event
        .merge(
            records[["pnr", "month"]],
            left_on="parent",
            right_on="pnr",
            how="inner",
            suffixes=("", "_y"),
        )
        # Keep only events since starting school
        .loc[lambda d: d["month"] >= d["start_of_school"]]
        # Keep only events caused by active parents
        .loc[lambda d: d["month"].dt.year.between(d["parent_start"], d["parent_end"])]
        .drop(columns=["pnr_y", "parent_start", "parent_end"])
        # Drop children with more than one event within month
        .loc[
            lambda d: ~d.duplicated(["pnr", "month"]).groupby(d["pnr"]).transform("any")
        ]
        # Keep earliest case
        .sort_values(["pnr", "month"])
        .drop_duplicates(["pnr"], keep="first")
        # Clean-up
        .rename(columns={"month": "t0"})
        .reset_index(drop=True)
    )

    return first_event


if __name__ == "__main__":
    run()
