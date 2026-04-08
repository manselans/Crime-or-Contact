"""
Functions to build figures from main analysis.
"""

import pickle

import matplotlib.pyplot as plt

from absence_and_crime.config import PATHS, setup_matplotlib
from absence_and_crime.plotting import period_mean, twfe, coef_plot


def figure_1() -> None:
    """
    Figure 1: Descriptive time trends of school absence rate relative to parental crime
    NOTE: Requires `temp/panels.pkl`.
    Save figure to `output/figures/Figure 1.png`.
    """

    paths = PATHS
    setup_matplotlib()

    # Load data
    with open(paths.temp / "panels.pkl", "rb") as f:
        panels = pickle.load(f)

    fig, _ = period_mean(panels["crime"])

    fig.savefig(paths.figures / "Figure 1.png")


def figure_2() -> None:
    """
    Figure 2: Event-study regression results of parent crime predicting school absence
    NOTE: Requires `temp/panels.pkl`.
    Save figure to `output/figures/Figure 2.png`.
    """

    paths = PATHS
    setup_matplotlib()

    # Load data
    with open(paths.temp / "panels.pkl", "rb") as f:
        panels = pickle.load(f)

    _, coefs = twfe(panels["crime"])
    fig, _ = coef_plot(coefs, event="first parent crime")

    fig.savefig(paths.figures / "Figure 2.png")


def figure_3() -> None:
    """
    Figure 3: Descriptive timing of parent and household instability relative to parent crime
    NOTE: Requires `temp/panels.pkl` and `temp/instability.pkl`.
    Save figure to `output/figures/Figure 3.png`.
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
        2, 2, figsize=(16, 12), sharex=True, constrained_layout=True
    )
    axes = axes.ravel()

    indicator_names = {
        "address": "Child address change",
        "mental": "Parent psych. diagnosis",
        "jobloss": "Parent job loss",
        "substance": "Parent substance abuse",
    }

    for i, k in enumerate(instability.keys()):
        period_mean(
            df,
            y=k,
            ax=axes[i],
            ylim=None,
            ytitle=indicator_names[k] + " (pct.)",
        )
        if i < 2:
            axes[i].set_xlabel("")

    fig.savefig(paths.figures / "Figure 3.png")


def figure_4() -> None:
    """
    Figure 4: Main results stratified by dimensions of parent and household instability
    NOTE: Requires `temp/panels.pkl` and `temp/instability.pkl`.
    Save figure to `output/figures/Figure 4.png`.
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

    for i, k in enumerate(instability.keys()):

        # Split by household instability
        df["after"] = df[k].astype(bool) & df["event_time"].between(0, 3)
        df["after"] = (
            df.groupby("pnr")["after"].transform(lambda g: (g == 1).any()).astype(bool)
        )

        # Plot
        period_mean(
            df[df.after],
            lfit=False,
            label="Yes",
            color=".4",
            ax=axes[i],
            plot_kwargs={"marker": "^", "markersize": 8},
        )

        period_mean(
            df[~df.after],
            lfit=False,
            label="No",
            event="first parent crime",
            vline=None,
            ax=axes[i],
            plot_kwargs={"markersize": 8},
        )

        # Edits
        handles, labels = axes[i].get_legend_handles_labels()
        leg = axes[i].legend(
            handles,
            labels,
            loc="upper left",
            ncol=2,
            frameon=False,
            title=indicator_names[k] + ":",
        )
        leg.get_title().set_fontweight("bold")
        axes[i].set_ylim(6, 12)
        if i % 2 != 0:
            axes[i].set_ylabel("")
        if i < 2:
            axes[i].set_xlabel("")

    fig.savefig(paths.figures / "Figure 4.png")


def figure_5() -> None:
    """
    Figure 5: Descriptive time trends of school absence rates around stages of contact
    NOTE: Requires `temp/panels.pkl`.
    Save figure to `output/figures/Figure 5.png`.
    """

    paths = PATHS
    setup_matplotlib()

    # Load data
    with open(paths.temp / "panels.pkl", "rb") as f:
        panels = pickle.load(f)

    # Selected points of contact
    stages = ["arrest", "charge", "conviction", "incarceration"]
    panels = {s: panels[s] for s in stages}

    # Plot
    fig, axes = plt.subplots(
        2, 2, figsize=(16, 12), sharey=True, constrained_layout=True
    )
    axes = axes.ravel()

    for i, k in enumerate(panels):
        period_mean(panels[k], ax=axes[i], event=f"first parent {stages[i]}")
        if i % 2 != 0:
            axes[i].set_ylabel("")

    fig.savefig(paths.figures / "Figure 5.png")


def figure_6() -> None:
    """
    Figure 6: Event-study regression results around stages of contact
    NOTE: Requires `temp/panels.pkl`.
    Save figure to `output/figures/Figure 6.png`.
    """

    paths = PATHS
    setup_matplotlib()

    # Load data
    with open(paths.temp / "panels.pkl", "rb") as f:
        panels = pickle.load(f)

    # Selected points of contact
    stages = ["arrest", "charge", "conviction", "incarceration"]
    panels = {s: panels[s] for s in stages}

    # Plot
    fig, axes = plt.subplots(
        2, 2, figsize=(16, 12), sharey=True, constrained_layout=True
    )
    axes = axes.ravel()

    for i, k in enumerate(panels):
        _, coefs = twfe(panels[k])
        coef_plot(coefs, ax=axes[i], event="first parent " + stages[i], ylim=(-1, 1.5))

        if i % 2 != 0:
            axes[i].set_ylabel("")

    fig.savefig(paths.figures / "Figure 6.png")
