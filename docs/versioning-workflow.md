# Data Versioning with Frostbyte

This guide demonstrates how to use Frostbyte to manage multiple versions of a dataset through its evolution. You'll learn how to archive versions, list them, review statistics, and restore specific versions when needed.

## Basic Versioning Workflow

### 1. Initialize a Repository

Start by initializing a Frostbyte repository in your project:

```bash
$ frostbyte init
✅ Initialized Frostbyte repository in .frostbyte/
```

> **⚠️ CAUTION:** If Frostbyte is already initialized in your project, running `init` again will delete all archives and reset the database. You'll be prompted for confirmation before proceeding.
>
> ```bash
> $ frostbyte init
> ⚠️ WARNING: Frostbyte is already initialized. This will delete all archives and reset the database. Continue? [y/N]: n
> Initialization aborted
> ```
>
> Use this command when you want to start fresh and clear all archive history.

### 2. Archive Your Dataset

When you have a dataset ready to archive:

```bash
$ head -n 3 customer_data.csv
id,name,email,signup_date,purchase_count
1,John Doe,john@example.com,2023-01-15,5
2,Jane Smith,jane@example.com,2023-02-20,3

$ frostbyte archive customer_data.csv
✅ Archived customer_data.csv (v1)
   Original size: 42.5 MB
   Compressed size: 12.8 MB
   Compression ratio: 70.1%
```

### 3. Update and Create New Versions

As your dataset evolves, create new versions:

```bash
# Make modifications (adding new customers, fixing errors)
$ python update_data.py customer_data.csv
✅ Updated 156 records, added 42 new customers

# Archive the updated file as a new version
$ frostbyte archive customer_data.csv
✅ Archived customer_data.csv (v2)
   Original size: 43.2 MB
   Compressed size: 13.1 MB
   Compression ratio: 69.7%

# Make another round of updates
$ python enrich_data.py customer_data.csv
✅ Added purchase history details to all customers

# Archive the third version
$ frostbyte archive customer_data.csv
✅ Archived customer_data.csv (v3)
   Original size: 48.7 MB
   Compressed size: 14.5 MB
   Compression ratio: 70.2%
```

### 4. Review Your Versions

You can list all versions to see the history:

```bash
$ frostbyte ls --all
│ Original Path    │ Version │ Date                │ Size    │ Compressed │ Ratio │
│--------------------------------------------------------------------------------│
│ customer_data.csv│ 1       │ 2025-05-10 14:32:15 │ 42.5 MB │ 12.8 MB    │ 70.1% │
│ customer_data.csv│ 2       │ 2025-05-11 09:15:47 │ 43.2 MB │ 13.1 MB    │ 69.7% │
│ customer_data.csv│ 3       │ 2025-05-13 10:22:33 │ 48.7 MB │ 14.5 MB    │ 70.2% │
```

> **Tip:** Use the "Original Path" and "Version" from this table to construct your restore commands. For example, to restore version 2 of customer_data.csv, you would use: `frostbyte restore customer_data.csv -v 2`

Get detailed statistics about your archived files:

```bash
$ frostbyte stats customer_data.csv
✅ Statistics for customer_data.csv:
   Versions: 3
   Latest version: 3
   Last modified: 2025-05-13 10:22:33
   Total size saved: 94.0 MB
```

### 5. Restore a Specific Version

If you discover an issue with your current version, you can restore a previous one:

#### Restore Command Syntax

The `restore` command has the following syntax:

```bash
frostbyte restore <path_spec> [--version VERSION]
# or
frostbyte restore <path_spec> [-v VERSION]
```

Where `<path_spec>` can be in one of these formats:

- **Path only** (restores the latest version):  
  `frostbyte restore customer_data.csv`

- **Path with version parameter** (restores a specific version):  
  `frostbyte restore customer_data.csv -v 2`

- **Archive filename** (restores the specific archive):  
  `frostbyte restore customer_data_v2.parquet`

- **Partial filename** (searches for matching files):  
  `frostbyte restore customer_data`

> **Note:** You can use either the original file path or the archive filename. Frostbyte is flexible and will find the correct version based on your input.

#### Restore Examples

```bash
# Restore the latest version using path
$ frostbyte restore customer_data.csv
✅ Restored customer_data.csv (latest) to customer_data.csv

# Restore version 2 specifically using version parameter
$ frostbyte restore customer_data.csv -v 2
✅ Restored customer_data.csv (v2) to customer_data.csv

# Restore using the archive filename
$ frostbyte restore customer_data_v1.parquet
✅ Restored customer_data.csv (v1) to customer_data.csv

# Restore using just the partial name (if it uniquely identifies one file)
$ frostbyte restore customer_data
✅ Restored customer_data.csv (latest) to customer_data.csv

# For files in subdirectories, include the relative or absolute path with version
$ frostbyte restore data/raw/customer_data.csv --version 1
✅ Restored data/raw/customer_data.csv (v1) to data/raw/customer_data.csv

# Verify it's the right version
$ head -n 3 customer_data.csv
id,name,email,signup_date,purchase_count
1,John Doe,john@example.com,2023-01-15,5
2,Jane Smith,jane@example.com,2023-02-20,3
```

> **Note:** When using partial names, if multiple files match, Frostbyte will show you the list of matching files and ask you to be more specific.

### 6. Continue Working from a Previous Version

After restoring, you can continue working from that version:

```bash
# Continue working with version 2 as our baseline
$ python fix_data.py customer_data.csv
✅ Fixed 12 records with incorrect purchase counts

# Archive this new version
$ frostbyte archive customer_data.csv
✅ Archived customer_data.csv (v4)
   Original size: 43.2 MB
   Compressed size: 13.0 MB
   Compression ratio: 69.9%
```

### 7. Review the Updated Version History

Now your version history will show the branching workflow:

```bash
$ frostbyte ls --all
│ Original Path    │ Version │ Date                │ Size    │ Compressed │ Ratio │
│--------------------------------------------------------------------------------│
│ customer_data.csv│ 1       │ 2025-05-10 14:32:15 │ 42.5 MB │ 12.8 MB    │ 70.1% │
│ customer_data.csv│ 2       │ 2025-05-11 09:15:47 │ 43.2 MB │ 13.1 MB    │ 69.7% │
│ customer_data.csv│ 3       │ 2025-05-13 10:22:33 │ 48.7 MB │ 14.5 MB    │ 70.2% │
│ customer_data.csv│ 4       │ 2025-05-13 14:05:12 │ 43.2 MB │ 13.0 MB    │ 69.9% │
```

## Best Practices

1. **Create meaningful versions**: Archive datasets after significant changes
2. **Use version annotations**: Document what changed in each version
3. **Regularly check statistics**: Monitor compression ratios and disk savings
4. **Clean up old versions**: Use `frostbyte purge` to remove unneeded versions
5. **Version your scripts too**: Keep processing scripts aligned with dataset versions

## Common Scenarios

### Reverting to a Stable Version

If a data processing pipeline introduces errors:

```bash
# Identify the last good version
$ frostbyte ls --all
│ Original Path    │ Version │ Date                │ Size    │ Compressed │ Ratio │
│--------------------------------------------------------------------------------│
│ customer_data.csv│ 1       │ 2025-05-10 14:32:15 │ 42.5 MB │ 12.8 MB    │ 70.1% │
│ customer_data.csv│ 2       │ 2025-05-11 09:15:47 │ 43.2 MB │ 13.1 MB    │ 69.7% │ <- Last known good version
│ customer_data.csv│ 3       │ 2025-05-13 10:22:33 │ 48.7 MB │ 14.5 MB    │ 70.2% │ <- Problematic version

# Restore the specific version using the -v parameter
$ frostbyte restore customer_data.csv -v 2
✅ Restored customer_data.csv (v2) to customer_data.csv
```

### Creating a Dataset Branch

To experiment without affecting the main dataset:

```bash
# Restore a specific version to a different output file
# (Note: Optional --output flag not yet implemented in current version)
$ frostbyte restore customer_data.csv@2
$ cp customer_data.csv experiment.csv

# Make changes and archive as a new file
$ python transform.py experiment.csv
$ frostbyte archive experiment.csv
```

### Comparing Versions

To understand differences between versions:

```bash
# Restore both versions to temporary files
$ frostbyte restore customer_data.csv -v 2 --output v2.csv
$ frostbyte restore customer_data.csv -v 3 --output v3.csv
# Use comparison tools
$ python compare_datasets.py v2.csv v3.csv
```

## Resetting Your Repository

In some cases, you may need to completely reset your Frostbyte repository, removing all archives and metadata:

### When to Reset

- When starting a new project phase that requires a clean history
- When archives contain incorrect or problematic data
- When you want to reclaim disk space and start fresh
- For testing or development purposes

### How to Reset

To reset your Frostbyte repository:

```bash
$ frostbyte init
⚠️ WARNING: Frostbyte is already initialized. This will delete all archives and reset the database. Continue? [y/N]: y
✓ Frostbyte initialized successfully
  Database reset to empty state
```

> **⚠️ CAUTION:** This operation permanently deletes all archive data and cannot be undone. Make sure you have backups of any important files before proceeding.
