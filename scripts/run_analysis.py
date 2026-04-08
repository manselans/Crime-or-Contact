"""
Create figures and tables from the main text.
"""

import absence_and_crime.results.tables as tbs
import absence_and_crime.results.figures as fgs
import absence_and_crime.results.appendix as apx


def run_analysis():
    """
    Generate figures and tables from the main text.
    """

    # ---- Tables
    print("Generating tables", end="...", flush=True)

    print("1")
    tbs.table_1()

    # ---- Figures
    print("Generating figures", end="...", flush=True)

    print("1", end="...", flush=True)
    fgs.figure_1()

    print("2", end="...", flush=True)
    fgs.figure_2()

    print("3", end="...", flush=True)
    fgs.figure_3()

    print("4", end="...", flush=True)
    fgs.figure_4()

    print("5", end="...", flush=True)
    fgs.figure_5()

    print("6")
    fgs.figure_6()

    # ---- Appendix
    print("Generating appendix figures", end="...", flush=True)

    print("1", end="...", flush=True)
    apx.figure_a1()

    print("2", end="...", flush=True)
    apx.figure_a2()

    print("3", end="...", flush=True)
    apx.figure_a3()

    print("4")
    apx.figure_a4()

    print("Done.")


if __name__ == "__main__":
    run_analysis()
