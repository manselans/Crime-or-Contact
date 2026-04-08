"""
Gather household instability indicators.
NOTE: Requires
1. dict of DataFrames of event panels to exist in `temp/panels.pkl`.
2. DataFrame of parent-child links to exist in `temp/families.parquet`.
Save a dict of DataFrames containing instability indicators in `temp/indicators.pkl`.
"""

import pickle

import pandas as pd

from absence_and_crime.config import PATHS
from absence_and_crime.utils.stata_io import fetch, gather


def run() -> None:
    """
    Gather household instability indicators.
    NOTE: Requires
    1. dict of DataFrames of event panels to exist in `temp/panels.pkl`.
    2. DataFrame of parent-child links to exist in `temp/families.parquet`.
    Save a dict of DataFrames containing instability indicators in `temp/indicators.pkl`.
    """

    paths = PATHS

    # Load data
    with open(paths.temp / "panels.pkl", "rb") as f:
        panels = pickle.load(f)
    families = pd.read_parquet(paths.temp / "families.parquet")

    time_of_crime = panels["crime"][["pnr", "t0"]].drop_duplicates()

    child_ids = set(panels["crime"].pnr)
    parent_ids = set(
        families[["pnr", "parent", "parent_start", "parent_end"]]
        .merge(time_of_crime, on="pnr", how="inner")
        .loc[lambda d: d.t0.dt.year.between(d.parent_start, d.parent_end), "parent"]
    )

    del panels, time_of_crime, families

    instability = {}

    # ---- Address changes

    instability["address"] = (
        fetch(
            paths.data.dst_raw / "befbop12_2021.dta",
            columns=["pnr", "bop_vfra"],
            filters={"pnr": child_ids},
        )
        .assign(
            month=lambda d: pd.to_datetime(d.bop_vfra, errors="coerce").dt.to_period("M")
        )
        .dropna(subset=["month"])[["pnr", "month"]]
        .drop_duplicates()
    )

    # ---- Job Loss

    # BFL: Wages and hours worked
    job = gather(
        paths.data.dst_raw,
        names=range(2008, 2022),
        file_pattern="bfl{name}.dta",
        concatenate=True,
        columns=[
            "pnr",
            "ajo_smalt_loenbeloeb",
            "ajo_fuldtid_beskaeftiget",
            "ajo_job_slut_dato",
        ],
        filters={"pnr": parent_ids},
    )

    # Make monthly
    job = (
        job.assign(month=job["ajo_job_slut_dato"].dt.to_period("M"))
        .groupby(["pnr", "month"], as_index=False)
        .agg(
            wages=("ajo_smalt_loenbeloeb", lambda x: x.sum()),
            fulltime=("ajo_fuldtid_beskaeftiget", lambda x: x.sum()),
        )
    )

    job = job.query("fulltime > 0")[["pnr", "month"]]

    obsend = pd.Period("2021-12", freq="M")

    # Compute next month and check presence
    instability["jobloss"] = (
        job[["pnr"]]
        # Next month: Month after recorded employment
        .assign(month=job["month"] + 1)
        # Still employed?
        .merge(job, on=["pnr", "month"], how="left", indicator=True)
        # Keep only job-losses
        .loc[lambda d: d["month"].le(obsend) & d["_merge"].eq("left_only")]
        # Sort and clean
        .drop(columns=["_merge"])
        .sort_values(["pnr", "month"])
        .reset_index(drop=True)
    )

    del job

    # ---- Parent substance abuse (via diagnoses and prescriptions)

    # Diagnoses, somatic hospitals
    adm = gather(  # Administrative data w. patient IDs
        paths.data.dst_raw,
        names=range(2010, 2019),
        file_pattern="lpr_adm{name}.dta",
        concatenate=True,
        columns=["pnr", "recnum", "c_kontaars", "d_inddto"],
        filters={"pnr": parent_ids},
    )

    codes = gather(  # Link between patient IDs and diagnoses
        paths.data.dst_raw,
        names=range(2010, 2019),
        file_pattern="lpr_diag{name}.dta",
        concatenate=True,
        columns=["recnum", "c_diag", "c_diagtype"],
        filters={"recnum": adm["recnum"]},
    )

    diag = adm.merge(codes, on="recnum", how="left").drop(columns=["recnum"])

    # Add psychiatry
    diag = pd.concat(
        [
            # LPR_DIAG
            diag.assign(psyk=0),
            # LPR_PSYK
            fetch(
                paths.data.dst_raw / "psyk_adm2018.dta",
                columns=["pnr", "c_adiag", "d_inddto", "c_kontaars"],
                filters={"pnr": parent_ids},
            )
            .assign(psyk=1)
            .rename(columns={"c_adiag": "c_diag"}),
        ],
        ignore_index=True,
    )

    del adm, codes

    # Prescriptions
    lmdb = {
        y: (
            pd.read_sas(
                paths.data.dst_raw / f"lmdb{y}.sas7bdat", encoding="latin1"
            )[["PNR", "eksd", "atc3"]]
            .assign(pnr=lambda d: pd.to_numeric(d["PNR"], errors="coerce"))
            .drop(columns=["PNR"])
            .loc[
                lambda d: d["pnr"].isin(parent_ids)
                & d["atc3"].str.startswith("N", na=False)
            ]
        )
        for y in range(2008, 2022)
    }

    lmdb = pd.concat(lmdb.values(), ignore_index=True)

    # From diagnoses
    s1 = diag.loc[lambda d: d["c_diag"].str.startswith("DF1", na=False)].assign(
        month=lambda d: pd.to_datetime(d["d_inddto"], errors="coerce").dt.to_period("M")
    )[["pnr", "month"]]

    # From prescriptions
    s2 = lmdb.loc[lambda d: d["atc3"].str.startswith("N07B", na=False)].assign(
        month=lambda d: pd.to_datetime(d["eksd"], errors="coerce").dt.to_period("M")
    )[["pnr", "month"]]

    instability["substance"] = (
        pd.concat([s1, s2], ignore_index=True).drop_duplicates().reset_index(drop=True)
    )

    del s1, s2, lmdb

    # ---- Parent psych. diagnosis

    selfharm = [f"DX{i}" for i in range(60, 85)]
    damage = ["DS51", "DS55", "DS59", "DS61", "DS65", "DS69"]
    damage = damage + [f"DT{i}" for i in range(36, 61) if i != 51]

    instability["mental"] = (
        diag.assign(
            # In somatic hospital w. psychiatric diagnosis
            psychiatric=lambda d: d["c_diag"].str.startswith("DF", na=False)
            & d["c_diagtype"].eq("A")
            | d["psyk"],
            # Attempted suicide or self harm
            selfharm=lambda d: d["c_kontaars"].eq("4")
            | d["c_diag"].str[0:4].isin(selfharm),
            # Damage or poisoning as supplemental diagnosis
            damage=lambda d: d["c_diagtype"].ne("A")
            & d["c_diag"].str[0:4].isin(damage)
            & ~d["psyk"],
        )
        .assign(
            contact=lambda d: d["psychiatric"] | d["selfharm"],
            month=lambda d: d["d_inddto"].dt.to_period("M"),
        )
        .loc[lambda d: d["contact"], ["pnr", "month"]]
        .drop_duplicates()
    )

    # Save
    with open(paths.temp / "instability.pkl", "wb") as f:
        pickle.dump(instability, f)


if __name__ == "__main__":
    run()
