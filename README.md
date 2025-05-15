# Frostbyte

> Cold Data Archiving for Data Workflows

Frostbyte is a lightweight tool that compresses, versions, and manages your large data files (CSV, Parquet, Excel). Perfect for data scientists and analysts who need to save disk space while keeping data organized.

##  Workflow

```mermaid
%% Detailed User Workflow
flowchart TD
    subgraph "Setup"
        Init[Initialize Repository] -->|fb init| Config[Configure Settings]
    end
    
    subgraph "Data Management"
        Raw[Raw Data File] -->|Process & Clean| Processed[Processed Data]
        Processed -->|fb archive| Archive[Archive File]
        Archive -->|fb ls| List[List Archives]
        List -->|fb stats| Stats[View Statistics]
        Stats -->|Space Savings| Decision{Need File?}
        Decision -->|Yes| Restore[Restore File]
        Decision -->|No| Keep[Keep Archived]
        Restore -->|fb restore| Working[Working Data]
        Working -->|Modify| NewVersion[New Version]
        NewVersion -->|fb archive| Archive
    end
    
    subgraph "Version Control"
        V1[Version 1] -->|fb archive| V2[Version 2]
        V2 -->|fb archive| V3[Version 3]
        V3 -.->|fb restore -v 1| V1
    end
    
    Init -.-> Raw
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

## Simple Usage Scenarios

### Scenario 1: Archive Dataset After Processing

```bash
# Create an archive of your processed data 
fb archive processed_data.csv
```

### Scenario 2: Free Up Disk Space

```bash
# See how much space you'll save
fb stats large_dataset.csv

# Archive the file
fb archive large_dataset.csv

# Verify it's archived
fb ls
```

### Scenario 3: Restore Data for Analysis

```bash
# List what's available
fb ls

# Restore the file you need
fb restore large_dataset.csv
```

### Scenario 4: Work with Multiple Versions

```bash
# Archive your file
fb archive dataset.csv

# Later, after changes, archive again
fb archive dataset.csv  # Creates version 2

# List all versions
fb ls --all

# Restore a specific version
fb restore dataset.csv -v 1
```

## Command Reference

| Command | Description | Example |
|---------|-------------|---------|
| `fb init` | Setup Frostbyte in your project | `fb init` |
| `fb archive <file>` | Compress and store a file | `fb archive data.csv` |
| `fb ls` | List archived files | `fb ls` or `fb ls --all` |
| `fb stats [file]` | Show compression statistics | `fb stats` or `fb stats data.csv` |
| `fb restore <file>` | Restore a file from archive | `fb restore data.csv` or `fb restore data.csv -v 2` |
| `fb purge <file>` | Remove archive versions | `fb purge old_data.csv` |

## Prerequisites

- Python 3.8+
- Packages: pandas, duckdb, pyarrow, click

## License

[MIT License](LICENSE)