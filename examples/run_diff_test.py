#!/usr/bin/env python
"""
Test script that runs the diff_dataframes function directly.
"""

import sys
import pandas as pd
from frostbyte.utils.diff import diff_dataframes

# Create sample dataframes just like in the test
print("Creating sample dataframes...", file=sys.stderr)
df1 = pd.DataFrame({
    'id': [1, 2, 3, 4],
    'name': ['A', 'B', 'C', 'D'],
    'value': [10, 20, 30, 40]
})

df2 = pd.DataFrame({
    'id': [2, 3, 4, 5],  # id=1 removed, id=5 added
    'value': [20, 35, 40, 50],  # Value for id=3 changed
    'category': ['X', 'Y', 'Z', 'W']  # New column
})

print("\nRunning diff_dataframes with 'id' as key column...", file=sys.stderr)
result = diff_dataframes(df1, df2, key_columns=['id'])

# Print the results in a structured way
print("\nDIFF RESULTS:", file=sys.stderr)
print(f"Rows added:    {result['rows_added']}", file=sys.stderr)
print(f"Rows removed:  {result['rows_removed']}", file=sys.stderr)
print(f"Rows modified: {result['rows_modified']}", file=sys.stderr)

print("\nSchema changes:", file=sys.stderr)
for change in result['schema_changes']:
    print(f"  {change}", file=sys.stderr)

print("\nColumn-level changes:", file=sys.stderr)
for col, count in result['column_diff_counts'].items():
    print(f"  {col}: {count} changes", file=sys.stderr)

print("\nSample row changes:", file=sys.stderr)
if result['row_sample']['added']:
    print("\n  Added rows:", file=sys.stderr)
    for row in result['row_sample']['added']:
        print(f"    {row}", file=sys.stderr)

if result['row_sample']['removed']:
    print("\n  Removed rows:", file=sys.stderr)
    for row in result['row_sample']['removed']:
        print(f"    {row}", file=sys.stderr)

if result['row_sample']['modified']:
    print("\n  Modified rows:", file=sys.stderr)
    for mod in result['row_sample']['modified']:
        print(f"    Key: {mod['key']}", file=sys.stderr)
        print("    Changes:", file=sys.stderr)
        for col, values in mod['changes'].items():
            print(f"      {col}: {values['old']} → {values['new']}", file=sys.stderr)

print("\nTest completed successfully!", file=sys.stderr)
