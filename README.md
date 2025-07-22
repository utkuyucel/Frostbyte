# Frostbyte

![Frostbyte Logo](imgs/frostbyte_logo_small.png)

> Cold Data Archiving for Data Workflows

Frostbyte is a lightweight tool that compresses, versions, and manages your large data files (CSV, Parquet, Excel). Perfect for data scientists and analysts who need to save disk space while keeping data organized & versioned.

## Version Control

```mermaid
flowchart LR
    V1["Version 1"] -->|"fb archive"| V2["Version 2"]
    V2 -->|"fb archive"| V3["Version 3"]
    V3 -.->|"fb restore -v 1"| V1
```

## Features

- **Space-Saving Compression**: Reduce storage needs for large datasets
- **Simple Versioning**: Track changes in your data files
- **Easy Commands**: Intuitive CLI with short aliases
- **Local First**: No cloud dependencies, works completely offline

## Quick Installation

```bash
# From source
git clone https://github.com/utkuyucel/Frostbyte.git
cd Frostbyte

# Setup environment
python -m venv frostbyte_venv
source frostbyte_venv/bin/activate

# Install
pip install -e .
```

### Work with Multiple Versions

```bash
# Archive your file
fb archive dataset.csv

# Later, after changes, archive again
fb archive dataset.csv  # Creates version 2

# List all versions of a specific file
fb ls dataset.csv

# Restore a specific version
fb restore dataset.csv -v 1
```

## Command Reference

| Command | Description | Example |
|---------|-------------|---------|
| `fb init` | Setup Frostbyte in your project | `fb init` |
| `fb archive <file>` | Compress and store a file | `fb archive data.csv` |
| `fb ls [file_name]` | List archived files. Optionally specify a file name to see all its versions. | `fb ls` or `fb ls my_data.csv` |
| `fb stats [file]` | Show compression statistics | `fb stats` or `fb stats data.csv` |
| `fb restore <file>` | Restore a file from archive | `fb restore data.csv` or `fb restore data.csv -v 2` |
| `fb purge <file>` | Remove archive versions | `fb purge old_data.csv` |

## License

[MIT License](LICENSE)
