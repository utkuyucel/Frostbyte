#!/usr/bin/env python
"""
Demo script for Frostbyte diff functionality.
"""

import os
import pandas as pd
import tempfile
from pathlib import Path

import frostbyte

def print_section(title):
    """Print a section header."""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)

def main():
    print_section("FROSTBYTE DIFF DEMO")
    
    # Create a temporary working directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Set up Frostbyte to use this directory
        print("Initializing Frostbyte repository...")
        os.environ["FROSTBYTE_ROOT"] = temp_dir
        frostbyte.init()
        
        # Create sample dataframes
        print("\nCreating sample data files...")
        df1 = pd.DataFrame({
            'id': range(1, 101),
            'name': [f"Item-{i}" for i in range(1, 101)],
            'value': [i * 10 for i in range(1, 101)],
            'category': ['A' if i % 3 == 0 else 'B' if i % 3 == 1 else 'C' for i in range(1, 101)]
        })
        
        # Second dataframe with various changes
        df2 = df1.copy()
        # Modify some values
        df2.loc[df2['id'].between(20, 30), 'value'] += 5
        # Add new rows
        new_rows = pd.DataFrame({
            'id': range(101, 111),
            'name': [f"Item-{i}" for i in range(101, 111)],
            'value': [i * 10 for i in range(101, 111)],
            'category': ['D' for _ in range(10)]
        })
        df2 = pd.concat([df2, new_rows], ignore_index=True)
        # Remove some rows
        df2 = df2[~df2['id'].between(5, 15)]
        # Add a new column
        df2['status'] = ['Active' if i % 5 != 0 else 'Inactive' for i in range(len(df2))]
        # Remove a column
        df2 = df2.drop(columns=['category'])
        
        # Save dataframes to CSV files
        file1_path = os.path.join(temp_dir, "original.csv")
        file2_path = os.path.join(temp_dir, "modified.csv")
        df1.to_csv(file1_path, index=False)
        df2.to_csv(file2_path, index=False)
        
        print(f"Created: {file1_path}")
        print(f"Created: {file2_path}")
        
        # Archive the files
        print("\nArchiving files...")
        archive_1 = frostbyte.archive(file1_path)
        archive_2 = frostbyte.archive(file2_path)
        print(f"Archived original.csv as version {archive_1.get('version')}")
        print(f"Archived modified.csv as version {archive_2.get('version')}")
        
        # List archives
        print("\nListing archives:")
        archives = frostbyte.ls(show_all=True)
        for archive in archives:
            print(f" - {archive.get('original_path')} (v{archive.get('version')})")
        
        # Compare files directly
        print_section("COMPARING FILES DIRECTLY")
        diff_result = frostbyte.diff(file1_path, file2_path)
        print_diff_results(diff_result)
        
        # Compare specific versions
        print_section("COMPARING SPECIFIC VERSIONS")
        version_diff = frostbyte.diff(f"{file1_path}@1", f"{file2_path}@1")
        print_diff_results(version_diff)
        
        print("\nDiff demo completed successfully!")

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
        for row in result['row_sample']['added'][:3]:  # Show up to 3 samples
            print(f" - {row}")
    
    if result['row_sample']['removed']:
        print("\nSample removed rows:")
        for row in result['row_sample']['removed'][:3]:  # Show up to 3 samples
            print(f" - {row}")
    
    if result['row_sample']['modified']:
        print("\nSample modified rows:")
        for mod in result['row_sample']['modified'][:3]:  # Show up to 3 samples
            print(f" - Key: {mod['key']}")
            print("   Changes:")
            for col, values in mod['changes'].items():
                print(f"     {col}: {values['old']} → {values['new']}")

if __name__ == "__main__":
    main()
