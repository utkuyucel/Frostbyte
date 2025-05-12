# Frostbyte

> Cold Data Archiving for Pandas Workflows

Frostbyte is a lightweight, local-first cold data archiving tool for data engineers, scientists, and ML practitioners. It enables efficient compression, versioning, and management of large, infrequently accessed datasets (CSV, Parquet) without relying on cloud infrastructure.

## Features

- **Efficient Compression**: Minimize disk usage for stale or infrequently used datasets
- **Data Versioning**: Maintain reproducible data versions with schema and content metadata
- **Simple Commands**: Archive, restore, and inspect data files with intuitive commands
- **Extensible**: Built to be extended with different backends and UIs

## Installation

```bash
# Install from source
git clone https://github.com/utkuyucel/Frostbyte.git
cd frostbyte

# Create virtual environment
python -m venv frostbyte_venv
source frostbyte_venv/bin/activate  # On Windows: frostbyte_venv\Scripts\activate

# Install the package
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



## Command Reference

| Command                     | Description                                          |
|-----------------------------|------------------------------------------------------|
| `frostbyte init`            | Initialize project, create `.frostbyte/` directory (recreates DB if exists) |
| `frostbyte archive <path>`  | Compress file, record metadata                       |
| `frostbyte restore <path>`  | Decompress and restore original file                 |
| `frostbyte ls`              | List summary of archived files with latest versions  |
| `frostbyte ls --all`        | List all versions with detailed info (date, size, filename) |
| `frostbyte stats [<file>]`  | Show size savings, last access, total versions       |
| `frostbyte purge <file>`    | Remove archive versions or entire file from storage  |

Check the [examples directory](examples/) for more detailed usage examples.

## Development

### Prerequisites

- Python 3.8+
- Required packages: click, pandas, numpy, duckdb, zstandard

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/utkuyucel/frostbyte.git
cd frostbyte

# Create and activate virtual environment
python -m venv frostbyte_venv
source frostbyte_venv/bin/activate  # On Windows: frostbyte_venv\Scripts\activate

# Install development dependencies
pip install -e .
pip install pytest black flake8 mypy
```

## License

This project is licensed under the [MIT License](LICENSE).