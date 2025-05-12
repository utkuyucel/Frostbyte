#!/usr/bin/env python
"""
Demo script for Frostbyte diff functionality with custom keys.
"""

import pandas as pd
import tempfile
from pathlib import Path

from frostbyte.utils.diff import diff_dataframes

def main():
    print("FROSTBYTE DIFF WITH CUSTOM KEYS DEMO")
    print("=" * 50)
    
    # Create sample dataframes with multiple potential keys
    print("Creating sample dataframes...")
    
    # Dataset with customer purchase history
    df1 = pd.DataFrame({
        'customer_id': [101, 102, 103, 104, 105],
        'order_id': ['A001', 'B002', 'C003', 'D004', 'E005'],
        'product': ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard'],
        'quantity': [1, 2, 1, 3, 2],
        'price': [1200, 800, 500, 350, 100],
        'date': ['2025-01-01', '2025-01-15', '2025-02-01', '2025-02-15', '2025-03-01']
    })
    
    # Updated dataset with some changes
    df2 = pd.DataFrame({
        'customer_id': [102, 103, 104, 105, 106],  # Customer 101 removed, 106 added
        'order_id': ['B002', 'C003', 'D004', 'E005', 'F006'],
        'product': ['Phone', 'Tablet', 'Monitor XL', 'Keyboard', 'Mouse'],  # Product name changed
        'quantity': [3, 1, 3, 2, 5],  # Quantity changed for 102
        'price': [800, 450, 400, 100, 50],  # Price changed for 103 and 104
        'date': ['2025-01-15', '2025-02-01', '2025-02-15', '2025-03-01', '2025-03-15']
    })
    
    print("\n1. Using customer_id as key:")
    diff1 = diff_dataframes(df1, df2, key_columns=['customer_id'])
    print_diff_results(diff1)
    
    print("\n2. Using order_id as key:")
    diff2 = diff_dataframes(df1, df2, key_columns=['order_id'])
    print_diff_results(diff2)
    
    print("\n3. Using both customer_id and order_id as composite key:")
    diff3 = diff_dataframes(df1, df2, key_columns=['customer_id', 'order_id'])
    print_diff_results(diff3)
    
    print("\n4. Without specifying key (automatic detection):")
    diff4 = diff_dataframes(df1, df2)
    print_diff_results(diff4)
    
    # Example with a time series dataset where date is a natural key
    print("\n\nTIME SERIES DATA EXAMPLE")
    print("=" * 50)
    
    # Create time series data
    ts1 = pd.DataFrame({
        'date': pd.date_range(start='2025-01-01', periods=10, freq='D'),
        'temperature': [20, 22, 19, 18, 17, 20, 22, 23, 25, 24],
        'humidity': [45, 48, 52, 55, 58, 54, 50, 48, 46, 44],
        'pressure': [1013, 1012, 1010, 1009, 1008, 1010, 1012, 1014, 1015, 1013]
    })
    
    # Second time series with some changes
    ts2 = ts1.copy()
    ts2.loc[2:4, 'temperature'] += 2  # Change some temperature readings
    ts2.loc[5:8, 'humidity'] -= 5     # Change some humidity readings
    ts2 = ts2.iloc[2:12].reset_index(drop=True)  # Shift the window by 2 days
    
    print("\n5. Time series diff using date as key:")
    diff5 = diff_dataframes(ts1, ts2, key_columns=['date'])
    print_diff_results(diff5)

def print_diff_results(result):
    """Print the results of a diff operation in a readable format."""
    print(f"\nRow changes:")
    print(f" - Rows added:    {result['rows_added']}")
    print(f" - Rows removed:  {result['rows_removed']}")
    print(f" - Rows modified: {result['rows_modified']}")
    print(f" - Total cells changed: {result['total_cells_changed']}")
    
    print("\nSchema changes:")
    for change in result['schema_changes']:
        print(f" - {change}")
    
    print("\nColumn difference counts:")
    for col, count in result['column_diff_counts'].items():
        print(f" - {col}: {count} changes")
    
    if result['row_sample']['added']:
        print("\nSample added rows:")
        for row in result['row_sample']['added'][:2]:  # Show up to 2 samples
            print(f" - {row}")
    
    if result['row_sample']['removed']:
        print("\nSample removed rows:")
        for row in result['row_sample']['removed'][:2]:  # Show up to 2 samples
            print(f" - {row}")
    
    if result['row_sample']['modified']:
        print("\nSample modified rows:")
        for mod in result['row_sample']['modified'][:2]:  # Show up to 2 samples
            print(f" - Key: {mod['key']}")
            print("   Changes:")
            for col, values in mod['changes'].items():
                print(f"     {col}: {values['old']} → {values['new']}")

if __name__ == "__main__":
    main()
