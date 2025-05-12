# Frostbyte

> Cold Data Archiving for Pandas Workflows

Frostbyte is a lightweight, local-first cold data archiving tool for data engineers, scientists, and ML practitioners. It enables efficient compression, versioning, and management of large, infrequently accessed datasets (CSV, Parquet) without relying on cloud infrastructure.

## Features

- **Efficient Compression**: Minimize disk usage for stale or infrequently used datasets
- **Data Versioning**: Maintain reproducible data versions with schema and content metadata
- **Simple Commands**: Archive, restore, inspect, and diff data files with intuitive commands
- **Extensible**: Built to be extended with different backends and UIs

## Installation

```bash
# Install from source
git clone https://github.com/utkuyucel/Frostbyte.git
cd frostbyte
pip install -e .

# Or from PyPI (once published)
# pip install frostbyte
```

## Quick Start

```bash
# Initialize Frostbyte in your project
frostbyte init

# Archive a CSV file
frostbyte archive data/cleaned.csv

# List archived files
frostbyte ls

# Get stats about your archives
frostbyte stats

# Restore an archived file
frostbyte restore data/cleaned.csv

# Compare two versions
frostbyte diff data/cleaned.csv@1 data/cleaned.csv@2
```

## Usage Examples

### Typical Workflow

1. **Initialize a new project**:
   ```bash
   frostbyte init
   ```

2. **Archive a file after processing**:
   ```bash
   frostbyte archive data/experiment.csv
   ```

3. **List available archives**:
   ```bash
   frostbyte ls --all
   ```

4. **Restore a specific version**:
   ```bash
   frostbyte restore data/experiment.csv@1
   ```

5. **View statistics**:
   ```bash
   frostbyte stats data/experiment.csv
   ```

6. **Compare versions**:
   ```bash
   frostbyte diff data/experiment.csv@1 data/experiment.csv@2
   ```

## Command Reference

| Command                     | Description                                          |
|-----------------------------|------------------------------------------------------|
| `frostbyte init`            | Initialize project, create `.frostbyte/` directory   |
| `frostbyte archive <path>`  | Compress file, record metadata                       |
| `frostbyte restore <path>`  | Decompress and restore original file                 |
| `frostbyte ls [--all]`      | List archived files and versions                     |
| `frostbyte stats [<file>]`  | Show size savings, last access, total versions       |
| `frostbyte diff <a> <b>`    | Show detailed row/column-level diffs between versions |
| `frostbyte purge <file>`    | Remove archive versions or entire file from storage  |
| `frostbyte gui`             | Launch optional Streamlit GUI (coming soon)          |

## Enhanced Diff Functionality

Frostbyte provides powerful diff capabilities for comparing DataFrames and archived files:

- **Key-Based Row Comparison**: Intelligently match rows using specified key columns
- **Detailed Change Reports**: See exactly what rows were added, removed, or modified
- **Schema Differences**: Track column additions, removals, and type changes
- **Value-Level Changes**: Identify specific cell values that changed
- **Smart Sampling**: View representative samples of changed rows

Example:
```python
# Direct DataFrame comparison
from frostbyte.utils.diff import diff_dataframes
result = diff_dataframes(df1, df2, key_columns=['id'])

# Archived file comparison
from frostbyte import diff
changes = diff('data.csv@1', 'data.csv@2')
```

Check the [examples directory](examples/) for more detailed usage examples.

## Development

### Prerequisites

- Python 3.8+
- Required packages: click, pandas, numpy, duckdb, zstandard

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/yourusername/frostbyte.git
cd frostbyte

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"
```

## License

This project is licensed under the [MIT License](LICENSE).