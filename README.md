# Replication Package — Project Title

Authors: Noa Hendel     
Affiliation: ROCKWOOL Foundation         
Contact: nhe@rff.dk  
Repository: (link)

This repository contains the replication code for the project *PROJECT TITLE* by AUTHORS.  

The repository is structured to allow full replication of all results, conditional on access to the underlying data.

Source code: The Python package source follows the "src" layout and is located in the `/src` directory. Install with `pip install -e .` to work with the editable package.

## Contents
- pyproject.toml
- /src — Python package source
- /notebooks — exploratory notebooks 
- /scripts — helper scripts and orchestration (e.g., run_all.sh)
- /output — figures and tables
- LICENSE
- README.md (this file)

## Requirements
- Python: >=3.8
- Dependencies: see pyproject.toml or requirements.txt

## Installation
1. Clone the repository:

```bash
git clone <repo-url>
```

2. Create and activate a virtual environment:

```bash
python -m venv .venv
# macOS / Linux (bash, zsh)
source .venv/bin/activate
# Windows (PowerShell)
.venv\Scripts\Activate.ps1
# Windows (cmd)
.venv\Scripts\activate.bat
```

3. Install package and dependencies:

```bash
pip install -e .
# or, if using requirements.txt
pip install -r requirements.txt
```

4. Confirm installation:

```bash
python -c "import importlib.metadata as m; print(m.version('school-absence-and-parental-crime'))"
```

## Data
The underlying data are microdata from **Statistics Denmark** and **cannot be shared** in this repository.

To run the code, you must edit:

src/tools/paths.py

and point it to your **local copies** of the restricted data.

Example:

```python
# src/tools/paths.py

dst = Path(r"/absolute/path/to/raw/data")
crime = Path(r"/absolute/path/to/crime/registers")

```

## Reproducing Results
1. Prepare environment (see Installation).
2. Configure data paths (see Data).
3. Collect analytic data:

```bash
python scripts/collect_data.py
```

By default this saves prepared analytical datasets under ./data (script will create the directory if missing).

4. Run analysis and generate figures/tables:

```bash
python scripts/run_analysis.py
```

Outputs (figures and tables) are written to ./output (script will create the directory if missing).

## Licensing 
- License: MIT
