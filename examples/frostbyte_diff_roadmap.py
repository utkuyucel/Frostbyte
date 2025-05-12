#!/usr/bin/env python
"""
Frostbyte Diff Testing Roadmap

This script provides a comprehensive roadmap for testing the Frostbyte
diff functionality. It demonstrates both basic diff_dataframes usage
and the full archive-based workflow.
"""

import os
import sys
import tempfile
import pandas as pd

# Print to stderr for visibility
def log(message):
    print(message, file=sys.stderr)

# Make a nice section header
def header(title):
    log("\n" + "=" * 80)
    log(f" {title}")
    log("=" * 80)

# Step 1: Test diff_dataframes directly
header("1. TESTING diff_dataframes FUNCTION DIRECTLY")

# Import the function
from frostbyte.utils.diff import diff_dataframes

# Create sample dataframes
log("Creating sample dataframes...")
df1 = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'name': ['A', 'B', 'C', 'D', 'E'],
    'value': [10, 20, 30, 40, 50]
})

df2 = pd.DataFrame({
    'id': [2, 3, 4, 5, 6],  # id=1 removed, id=6 added
    'name': ['B', 'C', 'X', 'E', 'F'],  # Changed D to X
    'value': [20, 35, 40, 50, 60],  # Changed value for id 3
    'status': ['Active'] * 5  # Added new column
})

log("\nSourceDataFrame 1:")
log(str(df1))
log("\nSourceDataFrame 2:")
log(str(df2))

# Run diff with ID as key
log("\nRunning diff with 'id' as key column...")
result = diff_dataframes(df1, df2, key_columns=['id'])

# Print the results
log("\nDiff Results:")
log(f"- Rows added: {result['rows_added']}")
log(f"- Rows removed: {result['rows_removed']}")
log(f"- Rows modified: {result['rows_modified']}")
log(f"- Total cells changed: {result['total_cells_changed']}")

log("\nSchema changes:")
for change in result['schema_changes']:
    log(f"  {change}")

log("\nColumn changes:")
for col, count in result['column_diff_counts'].items():
    log(f"  {col}: {count} changes")

log("\nSample row changes:")
if result['row_sample']['added']:
    log("  Added rows:")
    for row in result['row_sample']['added']:
        log(f"    {row}")

if result['row_sample']['removed']:
    log("  Removed rows:")
    for row in result['row_sample']['removed']:
        log(f"    {row}")

if result['row_sample']['modified']:
    log("  Modified rows:")
    for mod in result['row_sample']['modified']:
        log(f"    Key: {mod['key']}")
        log("    Changes:")
        for col, values in mod['changes'].items():
            log(f"      {col}: {values['old']} → {values['new']}")

# Step 2: Test the full archive workflow
header("2. TESTING COMPLETE ARCHIVE WORKFLOW")

# Import the necessary functions
from frostbyte import archive, diff, init, restore

# Set up a temporary environment
with tempfile.TemporaryDirectory() as temp_dir:
    os.environ["FROSTBYTE_ROOT"] = temp_dir
    
    # Initialize Frostbyte
    log("Initializing Frostbyte repository...")
    init_result = init()
    log(f"Init result: {init_result}")
    
    # Create CSV files
    file1_path = os.path.join(temp_dir, "data1.csv") 
    file2_path = os.path.join(temp_dir, "data2.csv")
    
    log(f"\nSaving DataFrame 1 to {file1_path}")
    df1.to_csv(file1_path, index=False)
    
    log(f"Saving DataFrame 2 to {file2_path}")
    df2.to_csv(file2_path, index=False)
    
    # Archive the files
    log("\nArchiving files...")
    archive1_info = archive(file1_path)
    log(f"Archive 1 info: {archive1_info}")
    
    archive2_info = archive(file2_path) 
    log(f"Archive 2 info: {archive2_info}")
    
    # Run diff on the archived files
    log("\nRunning diff between archived files...")
    diff_result = diff(file1_path, file2_path)
    
    log("\nArchive Diff Results:")
    log(f"- Rows added: {diff_result['rows_added']}")
    log(f"- Rows removed: {diff_result['rows_removed']}")
    log(f"- Rows modified: {diff_result['rows_modified']}")
    log(f"- Total cells changed: {diff_result['total_cells_changed']}")
    
    log("\nSchema changes:")
    for change in diff_result['schema_changes']:
        log(f"  {change}")
    
    # Try restoring and comparing again
    log("\nRestoring files...")
    restore_result1 = restore(file1_path)
    log(f"Restore 1 result: {restore_result1}")
    
    restore_result2 = restore(file2_path)
    log(f"Restore 2 result: {restore_result2}")

log("\nTest completed successfully!")
