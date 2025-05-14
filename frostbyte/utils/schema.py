"""
Schema extraction functionality for Frostbyte.
"""

import os
from pathlib import Path
from typing import Any, Dict, Union

import pandas as pd


def extract_schema(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Extract schema information from a CSV or Parquet file.

    Args:
        file_path: Path to the file

    Returns:
        Dict[str, Any]: Schema information including column types, stats, etc.
    """
    file_path = Path(file_path)

    # Determine file type from extension
    file_ext = file_path.suffix.lower()

    try:
        # Get the actual file size in bytes
        actual_file_size = os.path.getsize(file_path)

        # Read first few rows to infer schema
        if file_ext == ".csv":
            # For CSV, we'll count total rows first
            with open(file_path) as f:
                row_count = sum(1 for _ in f)

            # Then sample to determine column types
            df = pd.read_csv(file_path, nrows=100)
        elif file_ext in (".parquet", ".pq"):
            df = pd.read_parquet(file_path)
            row_count = len(df)
        else:
            raise ValueError(f"Unsupported file type: {file_ext}")

        # Build schema information
        schema: Dict[str, Any] = {
            "row_count": row_count,
            "column_count": len(df.columns),
            "columns": {},
            "file_size_bytes": actual_file_size,
            "avg_row_bytes": actual_file_size / max(1, row_count),
        }

        # Add column metadata
        for col in df.columns:
            col_info = {"type": str(df[col].dtype), "nullable": df[col].isna().any()}

            # Add basic statistics for numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info["stats"] = {
                    "min": float(df[col].min()) if not df[col].isna().all() else None,
                    "max": float(df[col].max()) if not df[col].isna().all() else None,
                    "mean": float(df[col].mean()) if not df[col].isna().all() else None,
                    "stddev": float(df[col].std()) if not df[col].isna().all() else None,
                }

            schema["columns"][col] = col_info

        return schema
    except Exception as e:
        # Fallback for unsupported files or errors
        return {
            "row_count": 0,
            "column_count": 0,
            "columns": {},
            "file_size_bytes": 0,
            "avg_row_bytes": 0,
            "error": str(e),
        }
