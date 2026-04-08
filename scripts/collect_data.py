"""
Collect data for analysis.
"""


from absence_and_crime.etl.clean_attendance import run as clean_attendance
from absence_and_crime.etl.define_families import run as define_families
from absence_and_crime.etl.criminal_records import run as criminal_records
from absence_and_crime.etl.setup_panels import run as setup_panels
from absence_and_crime.etl.covariates import run as gather_covariates
from absence_and_crime.etl.household_instability import run as gather_instability


def collect_data():
    """Collect data for analysis."""

    print("Collecting and cleaning absence records...")
    clean_attendance()

    print("Identifying child-parent pairs...")
    define_families()

    print("Gathering parent criminal records...")
    criminal_records()

    print("Building event time panels...")
    setup_panels()

    print("Gathering socio-economic covariates...")
    gather_covariates()

    print("Gathering indicators of household instability...")
    gather_instability()

    print("Data collection complete.")


if __name__ == "__main__":
    collect_data()
