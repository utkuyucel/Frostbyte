#!/usr/bin/env python
"""
Debug script with explicit error handling for Frostbyte diff functionality.
"""

import sys
import traceback

print("Script started")

try:
    print("Importing pandas...")
    import pandas as pd
    print(f"Pandas version: {pd.__version__}")
except Exception as e:
    print(f"Error importing pandas: {e}")
    traceback.print_exc()
    sys.exit(1)

try:
    print("Importing diff_dataframes...")
    from frostbyte.utils.diff import diff_dataframes
    print("Successfully imported diff_dataframes")
except Exception as e:
    print(f"Error importing diff_dataframes: {e}")
    traceback.print_exc()
    sys.exit(1)

try:
    print("Creating test dataframes...")
    df1 = pd.DataFrame({
        'id': [1, 2, 3],
        'value': [10, 20, 30]
    })
    df2 = pd.DataFrame({
        'id': [2, 3, 4],
        'value': [20, 35, 40]
    })
    print("Successfully created test dataframes")
    print(df1)
    print(df2)
except Exception as e:
    print(f"Error creating dataframes: {e}")
    traceback.print_exc()
    sys.exit(1)

try:
    print("Running diff_dataframes...")
    result = diff_dataframes(df1, df2, key_columns=['id'])
    print("Successfully ran diff_dataframes")
    print(f"Result: {result}")
except Exception as e:
    print(f"Error running diff_dataframes: {e}")
    traceback.print_exc()
    sys.exit(1)

print("Script completed successfully")
