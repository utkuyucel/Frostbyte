#!/usr/bin/env python
"""
Test script that runs the diff_dataframes function directly
in a similar way as the successful test_diff_dataframes_with_key test.
"""

import pandas as pd
from frostbyte.utils.diff import diff_dataframes

def main():
    """Run a basic test of diff_dataframes."""
    # Create sample dataframes just like in the test
    print("Creating sample dataframes...")
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
    
    print("Running diff_dataframes with 'id' as key column...")
    result = diff_dataframes(df1, df2, key_columns=['id'])
    
    # Print the results in a structured way
    print("\nDIFF RESULTS:")
    print(f"Rows added:    {result['rows_added']}")
    print(f"Rows removed:  {result['rows_removed']}")
    print(f"Rows modified: {result['rows_modified']}")
    
    print("\nSchema changes:")
    for change in result['schema_changes']:
        print(f"  {change}")
    
    print("\nColumn-level changes:")
    for col, count in result['column_diff_counts'].items():
        print(f"  {col}: {count} changes")
    
    print("\nSample row changes:")
    if result['row_sample']['added']:
        print("\n  Added rows:")
        for row in result['row_sample']['added']:
            print(f"    {row}")
    
    if result['row_sample']['removed']:
        print("\n  Removed rows:")
        for row in result['row_sample']['removed']:
            print(f"    {row}")
    
    if result['row_sample']['modified']:
        print("\n  Modified rows:")
        for mod in result['row_sample']['modified']:
            print(f"    Key: {mod['key']}")
            print("    Changes:")
            for col, values in mod['changes'].items():
                print(f"      {col}: {values['old']} → {values['new']}")

if __name__ == "__main__":
    main()
