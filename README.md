# tableau-connections-update
Tool to automatically update passwords / tokens for all available objects in Tableau Server that have a specific server name / username.

## Prerequisites
- Access to Tableau Server with an account that has permissions to update data source connections
- Python >= 3.9
- `uv` package installer (recommended)

## Setup
1. Clone this repository
2. Create and activate virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# or
.venv\Scripts\activate  # Windows
```

3. Install uv:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh   # Linux/macOS
# or
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"    # Windows
```

4. Install dependencies:
```bash
uv pip install -r pyproject.toml
```

5. Rename and configure settings:
```bash
cp -r configs-example configs
# Edit configs.yml with required connection details
```

## Usage
With virtual environment activated, run (example):
```bash
python ./main.py -i oracle -e dev
```

## How It Works

This script performs the following operations:

1. Connects to Tableau Server using your personal access token.
2. Identifies all data sources, workbooks, flows and other objects defined in `configs.yml` that match specified connection details.
3. Modifies credentials for all matching connections.
4. Records all changes in a detailed log file (`<log_file_name>.txt`).