#!/usr/bin/env python
"""
Simple debug script for Frostbyte diff functionality.
"""

print("Starting debug script...")

# Basic imports
try:
    import pandas as pd
    print("Pandas imported successfully")
except ImportError as e:
    print(f"Error importing pandas: {e}")

# Try importing from frostbyte
try:
    from frostbyte.utils.diff import diff_dataframes
    print("diff_dataframes imported successfully")
except ImportError as e:
    print(f"Error importing diff_dataframes: {e}")

# Create simple test data
try:
    df1 = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
    df2 = pd.DataFrame({'A': [2, 3, 4], 'B': ['b', 'c', 'd']})
    print("Test DataFrames created successfully")
except Exception as e:
    print(f"Error creating DataFrames: {e}")

# Try running a simple diff
try:
    print("Running diff_dataframes...")
    result = diff_dataframes(df1, df2)
    print("diff_dataframes executed successfully")
    print(f"Result: {result}")
except Exception as e:
    print(f"Error running diff_dataframes: {e}")

print("Debug script completed")
