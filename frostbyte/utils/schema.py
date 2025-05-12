"""
Schema extraction functionality for Frostbyte.
"""

import os
from pathlib import Path
from typing import Dict, Union

import pandas as pd


def extract_schema(file_path: Union[str, Path]) -> Dict:
    """
    Extract schema information from a CSV or Parquet file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Dict: Schema information including column types, stats, etc.
    """
    file_path = Path(file_path)
    
    # Determine file type from extension
    file_ext = file_path.suffix.lower()
    
    try:
        # Read first few rows to infer schema
        if file_ext == '.csv':
            df = pd.read_csv(file_path, nrows=100)
        elif file_ext in ('.parquet', '.pq'):
            df = pd.read_parquet(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_ext}")
        
        # Build schema information
        schema = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': {},
            'avg_row_bytes': df.memory_usage(deep=True).sum() / max(1, len(df))
        }
        
        # Add column metadata
        for col in df.columns:
            col_info = {
                'type': str(df[col].dtype),
                'nullable': df[col].isna().any()
            }
            
            # Add basic statistics for numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info['stats'] = {
                    'min': float(df[col].min()) if not df[col].isna().all() else None,
                    'max': float(df[col].max()) if not df[col].isna().all() else None,
                    'mean': float(df[col].mean()) if not df[col].isna().all() else None,
                    'stddev': float(df[col].std()) if not df[col].isna().all() else None,
                }
            
            schema['columns'][col] = col_info
        
        return schema
    except Exception as e:
        # Fallback for unsupported files or errors
        return {
            'row_count': 0,
            'column_count': 0,
            'columns': {},
            'error': str(e)
        }
