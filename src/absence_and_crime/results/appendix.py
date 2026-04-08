"""
Functions to build figures from appendix.
"""

import pickle

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from absence_and_crime.config import PATHS, setup_matplotlib
from absence_and_crime.plotting import twfe, coef_plot


def figure_a1() -> None:
    """
    Figure A1: Timing of CJC relative to crime
    NOTE: Requires `temp/panels.pkl` and `temp/records.pkl`.
    Save figure to `output/figures/Figure A1.png`.
    """

    paths = PATHS
    setup_matplotlib()

    # Load data
    with open(paths.temp / "panels.pkl", "rb") as f:
        panels = pickle.load(f)
    with open(paths.temp / "records.pkl", "rb") as f:
        records = pickle.load(f)

    # Set up data
    df = panels["crime"].loc[lambda d: d.event_time.between(0, 12)].copy()

    contact_steps = ["arrest", "charge", "conviction", "incarceration"]
    contact_names = ["Arrested", "Charged", "Convicted", "Incarcerated"]

    # Indicate criminal record in month t
    for k in contact_steps:
        df = df.merge(
            records[k].rename(columns={"pnr": "parent"}),
            on=["parent", "month"],
            how="left",
            validate="m:1",
            indicator=k,
        )
        df[k] = df[k].eq("both")

    # Period means
    agg_dict = {k: (k, "mean") for k in contact_steps}
    agg_dict["N"] = ("pnr", "size")
    df = df.groupby("event_time", as_index=False).agg(**agg_dict)

    # Plot
    fig, ax = plt.subplots(figsize=(8, 6), constrained_layout=True)
    colors = ["0", ".2", ".4", ".6"]
    styles = ["-", "--", "-.", ":"]

    for i, k in enumerate(contact_steps):
        # discretion
        ones = df[k] * df["N"]
        zeros = (1 - df[k]) * df["N"]
        df.loc[ones <= 3, k] = 0
        df.loc[zeros <= 3, k] = 1

        # plot
        ax.plot(
            df.event_time,
            df[k],
            linestyle=styles[i],
            color=colors[i],
            label=contact_names[i],
        )

    # Edit
    ax.legend(bbox_to_anchor=(0.5, -0.12), ncol=2, title="Parent:")

    ax.set_xlabel("Months since first parent crime")
    ax.set_ylabel("Share of children")
    ax.tick_params(axis="y", rotation=90)
    ax.set_yticks(np.arange(0, 5, 1) / 10)
    ax.yaxis.set_major_formatter(
        FuncFormatter(
            lambda x, _: "0" if abs(x) < 1e-12 else f"{x:.2f}".replace("0.", ".")
        )
    )

    fig.savefig(paths.figures / "Figure A1.png")


def figure_a2() -> None:
    """
    Figure A2: Main results stratified by dimensions of
    parent and household instability
    NOTE: Requires `temp/panels.pkl` and `temp/instability.pkl`.
    Save figure to `output/figures/Figure A2.png`.
    """

    paths = PATHS
    setup_matplotlib()

    # Load data
    with open(paths.temp / "panels.pkl", "rb") as f:
        panels = pickle.load(f)
    with open(paths.temp / "instability.pkl", "rb") as f:
        instability = pickle.load(f)

    # Set up data
    df = panels["crime"].copy()
    id_shift = {"pnr": "parent"}  # for parent indicators

    for k, v in instability.items():
        df = df.merge(
            v.rename(columns=id_shift if k not in ["address"] else {}),
            on=(["parent", "month"] if k not in ["address"] else ["pnr", "month"]),
            how="left",
            validate="m:1",
            indicator=k,
        )
        # has registered instability (x100 to get from shares to pct.)
        df[k] = df[k].eq("both").astype(int) * 100

    # Plot
    fig, axes = plt.subplots(
        2, 2, figsize=(16, 12), constrained_layout=True, sharey=True, sharex=True
    )
    axes = axes.ravel()

    indicator_names = {
        "address": "Child address change",
        "mental": "Parent psych. diagnosis",
        "jobloss": "Parent job loss",
        "substance": "Parent substance abuse",
    }

    for i, k in enumerate(instability):

        # ---- Set up data

        # Split by household instability
        df["after"] = df[k].astype(bool) & df["event_time"].between(0, 3)
        df["after"] = (
            df.groupby("pnr")["after"].transform(lambda g: (g == 1).any()).astype(bool)
        )

        # Plot
        _, coefs = twfe(df[df.after])
        coef_plot(
            coefs,
            ax=axes[i],
            label="Yes",
            color=".4",
            errorbar_kwargs={"markersize": 8},
        )

        _, coefs = twfe(df[~df.after])
        coef_plot(
            coefs,
            event="first parent crime",
            ylim=None,
            ax=axes[i],
            label="No",
            vline=None,
            errorbar_kwargs={"markersize": 8},
        )

        # Edits
        handles, labels = axes[i].get_legend_handles_labels()
        leg = axes[i].legend(
            handles,
            labels,
            loc="upper left",
            ncol=2,
            frameon=False,
            title=indicator_names[k],
        )
        leg.get_title().set_fontweight("bold")
        axes[i].set_ylim((-3, 3.5))
        if i % 2 != 0:
            axes[i].set_ylabel("")
        if i < 2:
            axes[i].set_xlabel("")

    fig.savefig(paths.figures / "Figure A2.png")


def figure_a3() -> None:
    """
    Figure A3: Separating crime and arrest
    NOTE: Requires `temp/panels.pkl` and `temp/records.pkl`.
    Save figure to `output/figures/Figure A3.png`.
    """

    paths = PATHS
    setup_matplotlib()

    # Load data
    with open(paths.temp / "panels.pkl", "rb") as f:
        panels = pickle.load(f)
    with open(paths.temp / "records.pkl", "rb") as f:
        records = pickle.load(f)

    # Set up data
    df = _separate_events(
        panels["crime"], records["arrest"].rename(columns={"pnr": "parent"}), delta=3
    )

    # Regress
    _, coefs = twfe(df)

    # Plot
    fig, _ = coef_plot(coefs, event="first parent crime")
    fig.savefig(paths.figures / "Figure A3.png")


def figure_a4() -> None:
    """
    Figure A4: Main results on not-yet-treated only
    NOTE: Requires `temp/panels.pkl`.
    Save figure to `output/figures/Figure A4.png`.
    """

    paths = PATHS
    setup_matplotlib()

    # Load data
    with open(paths.temp / "panels.pkl", "rb") as f:
        panels = pickle.load(f)

    # Run regression
    _, coefs = twfe(
        panels["crime"].loc[lambda d: d.event_time.lt(0)],
        window=range(-12, 0), 
    )

    # Plot
    fig, _ = coef_plot(coefs, event="first parent crime", xjump=2)
    fig.savefig(paths.figures / "Figure A4.png")


def _separate_events(
    df: pd.DataFrame,
    exclusion_events: pd.DataFrame,
    *,
    id_col: str = "parent",
    t0_col: str = "t0",
    month_col: str = "month",
    delta: int = 1,
):
    """
    Remove individuals from event panel who have another event
    occuring within `delta` months.

    Parameters
    ----------
    df
        Event panel DataFrame.
    exclusion_events
        DataFrame of events to base exclusion on (id, calendar_month_col).
    id_col
        Name of the id column in `df` and `exclusion_events` (these have to be the same).
    t0_col
        Name of column in `df` containing month of event (must be dtype Period[M]).
    month_col
        Name of column in `exclusion_events` containing month of event
        (must be dtype Period[M]).
    delta
        Minimum required distance in months between events.
        E.g., delta = 0 -> exclusion events may not occur in t0 itself.

    Returns
    -------
    df_filtered
        Filtered panel.
    """

    # unique (id, t0) pairs
    id_t0 = df[[id_col, t0_col]].drop_duplicates()

    # merge on exclusion events
    exclude = (
        id_t0.merge(exclusion_events, on=id_col, how="left")
        .loc[
            lambda d: (d[month_col].astype("int64") - d[t0_col].astype("int64")).abs()
            <= delta,
            [id_col, t0_col],
        ]
        .drop_duplicates()
    )

    df_filtered = (
        df.merge(
            exclude, on=[id_col, t0_col], how="left", validate="m:1", indicator=True
        )
        .loc[lambda d: d["_merge"].eq("left_only")]
        .drop(columns=["_merge"])
    )

    return df_filtered
