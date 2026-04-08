"""
Gather covariates on children from their first year of school.
NOTE: Data on parent-child links need to exist in `temp/families.parquet`.
Save child covariates in `temp/covariates.parquet`.
"""

import pandas as pd

from absence_and_crime.config import PATHS
from absence_and_crime.utils.stata_io import fetch
from absence_and_crime.transforms import age_from_months


def run() -> None:
    """
    Gather covariates on children from their first year of school.
    NOTE: Data on parent-child links need to exist in `temp/families.parquet`.
    Save child covariates in `temp/covariates.parquet`.
    """

    paths = PATHS

    # Load data
    covariates = pd.read_parquet(paths.temp / "families.parquet")

    covariates = (
        covariates.assign(
            year=lambda d: d.start_of_school.dt.year,
            age_at_start=lambda d: age_from_months(d.date_of_birth, d.start_of_school),
        )
        .loc[ # parents from child's first year of school
            lambda d: d["year"].between(d.parent_start, d.parent_end)
        ]
        .drop(columns=["parent_start", "parent_end"])
    )

    ids = (
        covariates[["parent", "year"]]
        .rename(columns={"parent": "pnr"})
        .drop_duplicates()
    )

    lmo = {}
    for t in range(2010, 2022):

        py = ids.loc[covariates.year.eq(t)]  # This year's parents

        # ---- BFL: Wages and hours worked

        vl = ["pnr", "ajo_smalt_loenbeloeb", "ajo_fuldtid_beskaeftiget"]
        tmp = (
            fetch(paths.data.dst_raw / f"bfl{t}.dta", filters={"pnr": py["pnr"]}, columns=vl)
            .groupby("pnr", as_index=False)
            .agg(  # Collapse by parent
                wages=(
                    "ajo_smalt_loenbeloeb",
                    lambda x: x.sum() / 1000,
                ),  # Total wages, 1.000 DKK
                fulltime=(
                    "ajo_fuldtid_beskaeftiget",
                    lambda x: x.sum() / 12,
                ),  # Share of fulltime (monthly)
            )
        )

        # Merge onto parent list
        py = py.merge(tmp, on="pnr", how="left")

        # ---- IND: Assets

        vl = ["pnr", "formrest_ny05"]
        tmp = (
            fetch(paths.data.dst_raw / f"ind{t}.dta", filters={"pnr": py["pnr"]}, columns=vl)
            .groupby("pnr", as_index=False)
            .agg(
                assets=(
                    "formrest_ny05",
                    lambda x: x.sum() / 1000,
                ),  # Net assets, 1.000 DKK
            )
        )

        # Merge onto parent list
        py = py.merge(tmp, on="pnr", how="left")

        # Assuming missing is 0
        py = py.fillna(0)

        # ---- UDDA: Education

        vl = ["pnr", "hfaudd"]
        tmp = fetch(
            paths.data.dst_raw / f"udda09_{t}.dta", filters={"pnr": py["pnr"]}, columns=vl
        )

        # Merge onto parent list
        py = py.merge(tmp, on="pnr", how="left")

        lmo[t] = py

    # Combine all years into one
    lmo = pd.concat(lmo.values(), ignore_index=True)

    # Merge onto children
    covariates = covariates.merge(
        lmo.rename(columns={"pnr": "parent"}),
        on=["parent", "year"],
        how="left",
        validate="m:1",
    )

    # Truncate
    for i in ["wages", "fulltime", "assets"]:
        covariates[i].clip(upper=covariates[i].quantile(0.99), inplace=True)
        if i in ["wages", "fulltime"]:
            covariates[i].clip(lower=0, inplace=True)
        else:
            covariates[i].clip(lower=covariates[i].quantile(0.01), inplace=True)

    # Recode educations levels
    cats = {  # 4 rough ISCED levels
        "Primary": 1,
        "Lower secondary": 1,  # 1: Grundskole (Pre-High School)
        "Upper secondary": 2,  # 2: Gymnasie (High School)
        "Short cycle tertiary": 3,  # 3: KVU (post-High School, non-college)
        "Bachelor or equivalent": 4,
        "Master or equivalent": 4,
        "Doctoral or equivalent": 4,  # 4: MVU/LVU (Some college degree)
    }

    mapping = (
        pd.read_stata(paths.data.disced / "c_audd_niveau_e_l1l2_t.dta")
        .assign(
            hfaudd=lambda df: df["start"].astype(int),
            isced=lambda df: df["AUDD_NIVEAU_E_L1L2_T"].map(cats),
        )[["hfaudd", "isced"]]
        .dropna()
        .set_index("hfaudd")["isced"]
        .to_dict()
    )

    covariates = covariates.assign(
        education=lambda d: d["hfaudd"].map(mapping)
    ).drop(columns=["hfaudd"])

    # ---- Collapse by child

    cons = ["age_at_start", "non_danish", "female"]
    dkk = ["wages", "assets"]

    covariates = covariates.groupby("pnr", as_index=False).agg(
        {
            **{c: "last" for c in cons},  # child-specific variables
            **{c: "sum" for c in dkk},  # total wages, transfers and net asset
            "fulltime": "mean",  # mean share of fulltime
            "education": "max",  # highest level of education among parents
        }
    )

    # Save
    covariates.to_parquet(paths.temp / "covariates.parquet")


if __name__ == "__main__":
    run()
