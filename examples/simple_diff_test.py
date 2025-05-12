#!/usr/bin/env python
"""
Basic test script for Frostbyte diff functionality.
This script just tests the basic diff_dataframes function directly.
"""

import sys
import pandas as pd

# Print debug information
print(f"Python version: {sys.version}")
print(f"Pandas version: {pd.__version__}")

# Import the function we want to test
from frostbyte.utils.diff import diff_dataframes

# Create two simple dataframes
print("\nCreating test dataframes...")
df1 = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'name': ['A', 'B', 'C', 'D', 'E'],
    'value': [10, 20, 30, 40, 50]
})
print(df1)

df2 = pd.DataFrame({
    'id': [2, 3, 4, 5, 6],  # Removed 1, added 6
    'name': ['B', 'C', 'X', 'E', 'F'],  # Changed D to X
    'value': [20, 35, 40, 50, 60],  # Changed value for id 3
    'status': ['Active'] * 5  # Added new column
})
print(df2)

# Run the diff with ids as keys
print("\nRunning diff_dataframes with 'id' as key column...")
result = diff_dataframes(df1, df2, key_columns=['id'])

# Print the results
print("\nDiff Results:")
print(f"Rows added: {result['rows_added']}")
print(f"Rows removed: {result['rows_removed']}")
print(f"Rows modified: {result['rows_modified']}")
print(f"Total cells changed: {result['total_cells_changed']}")

print("\nSchema changes:")
for change in result['schema_changes']:
    print(f"  {change}")

print("\nColumn diff counts:")
for col, count in result['column_diff_counts'].items():
    print(f"  {col}: {count}")

print("\nRow Samples:")
print("  Added rows:")
for row in result['row_sample']['added']:
    print(f"    {row}")

print("  Removed rows:")
for row in result['row_sample']['removed']:
    print(f"    {row}")

print("  Modified rows:")
for mod in result['row_sample']['modified']:
    print(f"    Key: {mod['key']}")
    print("    Changes:")
    for col, values in mod['changes'].items():
        print(f"      {col}: {values['old']} → {values['new']}")
