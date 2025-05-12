"""
Implementation of the diff command for Frostbyte.

Provides functionality to compare two versions of a file.
"""

import os
from typing import Dict, Tuple, Union, Any

import pandas as pd

from frostbyte.utils.diff import diff_dataframes


def diff_files(file_a: str, file_b: str, manager=None) -> Dict:
    """
    Compare two versions of a file.
    
    Args:
        file_a: First file specification (path@version)
        file_b: Second file specification (path@version)
        manager: Optional archive manager instance
        
    Returns:
        Dict: Information about the differences
    """
    # Parse version specifications
    path_a, version_a = _parse_path_spec(file_a)
    path_b, version_b = _parse_path_spec(file_b)
    
    # Use the provided manager or get a new one
    if manager is None:
        # Import here to avoid circular import
        from frostbyte.core.manager import ArchiveManager
        manager = ArchiveManager()
    
    if version_a:
        restore_a_path = f"{path_a}@{version_a}"
    else:
        restore_a_path = path_a
        
    if version_b:
        restore_b_path = f"{path_b}@{version_b}"
    else:
        restore_b_path = path_b
    
    # Read the files into DataFrames
    df_a, df_b = _load_dataframes(restore_a_path, restore_b_path, manager)
    
    # Compare the DataFrames
    return diff_dataframes(df_a, df_b)


def _parse_path_spec(path_spec: str) -> Tuple[str, Union[int, float, None]]:
    """
    Parse a path@version specification.
    
    Args:
        path_spec: Path with optional version (e.g., 'data/file.csv@2')
        
    Returns:
        Tuple[str, Union[int, float, None]]: Path and version
    """
    if '@' in path_spec:
        path, version_str = path_spec.split('@', 1)
        try:
            if '.' in version_str:
                version = float(version_str)
            else:
                version = int(version_str)
        except ValueError:
            version = None
    else:
        path, version = path_spec, None
    
    return path, version


def _load_dataframes(file_a: str, file_b: str, manager: Any) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load two DataFrames from file paths or archive specifications.
    
    Args:
        file_a: First file or archive spec
        file_b: Second file or archive spec
        manager: Archive manager for restoration
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Loaded DataFrames
    """
    # Handle the first file
    if os.path.exists(file_a):
        path_a = file_a
    else:
        # Restore from archive
        result_a = manager.restore(file_a)
        path_a = result_a['original_path']
        
    # Handle the second file
    if os.path.exists(file_b):
        path_b = file_b
    else:
        # Restore from archive
        result_b = manager.restore(file_b)
        path_b = result_b['original_path']
    
    # Load the DataFrames
    df_a = _load_file(path_a)
    df_b = _load_file(path_b)
    
    return df_a, df_b


def _load_file(file_path: str) -> pd.DataFrame:
    """
    Load a file into a pandas DataFrame.
    
    Args:
        file_path: Path to the file
        
    Returns:
        pd.DataFrame: Loaded DataFrame
    """
    ext = os.path.splitext(file_path)[1].lower()
    
    if ext == '.csv':
        return pd.read_csv(file_path)
    elif ext in ('.parquet', '.pq'):
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")
