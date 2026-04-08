"""
Gather parent criminal records.
NOTE: Data on parent-child links need to exist in `temp/families.parquet`.
Save criminal records in a dictionary of DataFrames in `temp/records.pkl`.
"""

import pickle

import pandas as pd

from absence_and_crime.config import PATHS
from absence_and_crime.utils.stata_io import gather
from absence_and_crime.transforms import make_monthly, merge_spells


def run() -> None:
    """
    Gather parent criminal records.
    NOTE: Data on parent-child links need to exist in `temp/families.parquet`.
    Save criminal records in a dictionary of DataFrames in `temp/records.pkl`.
    """

    paths = PATHS
    ids = set(pd.read_parquet(paths.temp / "families.parquet").parent)

    # ---- Crimes and charges

    charges = gather(
        paths.data.dst_raw,
        names=range(1980, 2022),
        file_pattern="krsi{name}.dta",
        concatenate=True,
        columns=["pnr", "sig_sigtdto", "sig_ger1dto"],
        filters={"pnr": ids, "sig_ger7": lambda s: s.between(1000000, 1999999)},
    )

    crimes = make_monthly(charges, date_col="sig_ger1dto", name="crimes")
    charges = make_monthly(charges, date_col="sig_sigtdto", name="charges")

    # ---- Convictions

    conv = gather(
        paths.data.dst_raw,
        names=range(1980, 2022),
        file_pattern="kraf{name}.dta",
        concatenate=True,
        columns=["pnr", "afg_afgoedto"],
        filters={"pnr": ids, "afg_ger7": lambda s: s.between(1000000, 1999999)},
    )

    conv = make_monthly(conv, date_col="afg_afgoedto", name="convictions")

    # ---- Incarcerations

    incar = gather(
        paths.data.crime / "krin_placering.dta",
        columns=["pnr", "handelse", "fgsldto", "losldto"],
        filters={"pnr": ids},
        concatenate=True,
    )

    incar = incar.rename(
        columns={"handelse": "type", "fgsldto": "entry", "losldto": "exit"}
    )

    # ---- Arrests
    arrests = incar.query("type <= 2").pipe(
        make_monthly, date_col="entry", name="arrests"
    )

    # ---- Non-arrest incarcerations

    # Pre-trial custody, serving time, and transfers
    incar = incar.query("type >= 3")

    # Drop older records with missing release date
    incar = incar.loc[lambda d: ~(d["exit"].isna() & d["entry"].dt.year.le(1990))]

    # Impute missing transfer dates
    mask = incar["exit"].isna() & incar["type"].eq(6)
    incar["exit"] = incar["exit"].mask(mask, incar["entry"])

    # Recent incarcerations with missing release dates -> assume ongoing
    mask = incar["exit"].isna() & incar["entry"].ge(pd.Timestamp("2019-01-01"))
    incar["exit"] = incar["exit"].mask(mask, pd.Timestamp("2021-08-01"))

    # Drop double registrations
    incar = incar.sort_values(
        ["pnr", "entry", "exit"], na_position="first"
    ).drop_duplicates(["pnr", "entry"], keep="last")

    # Set remaining missing release dates to day of incarceration and mark as imputed
    incar["imputed"] = incar["exit"].isna()
    incar["exit"] = incar["exit"].where(incar["exit"].notna(), incar["entry"])

    # ---- Collapse overlapping spells

    spells = (
        incar.groupby("pnr", group_keys=False)
        .apply(merge_spells)
        .reset_index(drop=True)
    )

    # Mark imputed
    imputees = incar.query("imputed").pipe(lambda d: set(zip(d["pnr"], d["exit"])))
    spells["imputed"] = pd.Series(zip(spells["pnr"], spells["exit"])).isin(imputees)

    # Make monthly
    spells = spells.assign(
        **{c: lambda d, c=c: d[c].dt.to_period("M") for c in ["entry", "exit"]}
    )

    # Save entry into incarceration as event
    entries = (
        (spells[["pnr", "entry"]].rename(columns={"entry": "month"}).assign(entries=1))
        .groupby(["pnr", "month"], as_index=False)
        .agg({"entries": "sum"})
    )

    # Make incarceration data long
    spells = (
        spells.set_index("pnr")
        .apply(lambda r: pd.period_range(r["entry"], r["exit"], freq="M"), axis=1)
        .explode()
        .rename("month")
        .reset_index()
        .drop_duplicates()
    )

    spells["incarcerated"] = 1

    # Gather all records in dictionary
    records = {
        "crime": crimes,
        "charge": charges,
        "conviction": conv,
        "arrest": arrests,
        "incarceration": entries,
        "in_prison": spells,
    }

    # Save
    with open(paths.temp / "records.pkl", "wb") as f:
        pickle.dump(records, f)


if __name__ == "__main__":
    run()
