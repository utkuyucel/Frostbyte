#!/usr/bin/env python
"""
Debug script that writes output to a file.
"""

import sys
import traceback
import os

# Open a file to write output
with open('/home/utku/Frostbyte/debug_output.txt', 'w') as f:
    f.write("Script started\n")
    
    try:
        f.write("Importing pandas...\n")
        import pandas as pd
        f.write(f"Pandas version: {pd.__version__}\n")
    except Exception as e:
        f.write(f"Error importing pandas: {str(e)}\n")
        traceback.print_exc(file=f)
        sys.exit(1)
    
    try:
        f.write("Importing diff_dataframes...\n")
        from frostbyte.utils.diff import diff_dataframes
        f.write("Successfully imported diff_dataframes\n")
    except Exception as e:
        f.write(f"Error importing diff_dataframes: {str(e)}\n")
        traceback.print_exc(file=f)
        sys.exit(1)
    
    try:
        f.write("Creating test dataframes...\n")
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [10, 20, 30]
        })
        df2 = pd.DataFrame({
            'id': [2, 3, 4],
            'value': [20, 35, 40]
        })
        f.write("Successfully created test dataframes\n")
        f.write(f"{df1}\n")
        f.write(f"{df2}\n")
    except Exception as e:
        f.write(f"Error creating dataframes: {str(e)}\n")
        traceback.print_exc(file=f)
        sys.exit(1)
    
    try:
        f.write("Running diff_dataframes...\n")
        result = diff_dataframes(df1, df2, key_columns=['id'])
        f.write("Successfully ran diff_dataframes\n")
        f.write(f"Result: {result}\n")
    except Exception as e:
        f.write(f"Error running diff_dataframes: {str(e)}\n")
        traceback.print_exc(file=f)
        sys.exit(1)
    
    f.write("Script completed successfully\n")
