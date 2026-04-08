"""
Find children from attendance records in population registers.
Identify all registered parents over time.
NOTE: Absence records need to exist in `temp/absence.parquet`.
Save resulting data to `temp/families.parquet`.
"""

import pandas as pd

from absence_and_crime.utils.stata_io import gather
from absence_and_crime.config import PATHS


def run() -> None:
    """
    Find children from attendance records in population registers.
    Identify all registered parents over time.
    NOTE: Absence records need to exist in `temp/absence.parquet`.
    Save resulting data to `temp/families.parquet`.
    """

    paths = PATHS
    absence = pd.read_parquet(paths.temp / "absence.parquet")

    # List of children and their first month of school
    ids = absence.sort_values("month").drop_duplicates("pnr", keep="first")[
        ["pnr", "month"]
    ]

    # Find children and their parents
    families = gather(
        paths.data.dst_raw,
        names=range(2003, 2023),
        file_pattern="bef12_{name}.dta",
        add_name="year",
        concatenate=True,
        filters={"pnr": ids["pnr"]},
        columns=["pnr", "koen", "foed_dag", "ie_type", "mor_id", "far_id"],
    )

    # Set time-invariant variables to latest recorded value
    vl = ["koen", "foed_dag", "ie_type"]
    families = families.sort_values(["pnr", "year"])
    families[vl] = families.groupby("pnr")[vl].transform("last")

    # ---- Crop to appropriate subset of children

    # Only years prior to year of 17th birthday
    families = families.loc[families["year"] - families["foed_dag"].dt.year <= 16]

    # Must have started school at ages 5-7
    families = families.merge(
        ids, on="pnr", how="left"
    ).loc[  # add first month of school
        lambda d: (d.month.dt.year - d.foed_dag.dt.year).between(5, 7)
    ]

    # Reshaping: make long and drop missing parents
    families = families.melt(
        id_vars=["pnr", "koen", "foed_dag", "ie_type", "year", "month"],
        value_vars=["mor_id", "far_id"],
        var_name="mom",
        value_name="parent",
    ).loc[lambda d: d["parent"] != ""]

    # Destring parent IDs
    families["parent"] = pd.to_numeric(families["parent"]).astype("int64")

    # Mom indicator
    families["mom"] = (families["mom"] == "mor_id").astype("uint8")

    # Time of parenthood
    families["parent_start"] = families.groupby(["pnr", "parent"])["year"].transform(
        "min"
    )
    families["parent_end"] = families.groupby(["pnr", "parent"])["year"].transform(
        "max"
    )

    # Remove time dimension and make child-parent pair unique
    families.drop(columns=["year"], inplace=True)
    families.drop_duplicates(inplace=True)

    # Minor edits
    families = families.assign(
        koen=lambda d: d.koen.eq(2).astype(int),  # female dummy
        ie_type=lambda d: d.ie_type.ne(1).astype(int),  # immigrant/descendant dummy
    ).rename(
        columns={
            "month": "start_of_school",
            "foed_dag": "date_of_birth",
            "koen": "female",
            "ie_type": "non_danish",
        }
    )

    # ---- Find parents

    ids = set(families["parent"])

    ps = gather(
        paths.data.dst_raw,
        names=range(2003, 2023),
        file_pattern="bef12_{name}.dta",
        add_name="year",
        concatenate=True,
        filters={"pnr": ids},
        columns=["pnr", "koen"],
    )

    # Latest registered sex
    ps = (
        ps.sort_values(["pnr", "year"])
        .drop_duplicates("pnr", keep="last")[["pnr", "koen"]]
        .rename(columns={"pnr": "parent"})
        .reset_index(drop=True)
    )

    # Add to child records
    families = (
        families.merge(ps, on="parent", how="inner", validate="m:1")
        .assign(mom=lambda d: d.koen.eq(2).where(d.koen.notna()))
        .drop(columns=["koen"])
        .drop_duplicates()
    )

    # Save
    families.to_parquet(paths.temp / "families.parquet", index=False)


if __name__ == "__main__":
    run()
