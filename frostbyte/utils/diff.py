"""
Utilities for comparing data files.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import numpy as np


def diff_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, key_columns: Optional[List[str]] = None) -> Dict:
    """
    Compare two pandas DataFrames and return detailed differences.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        key_columns: Optional list of columns to use as keys for row matching
                    (if None, will use index or attempt to identify a suitable key)
        
    Returns:
        Dict: Detailed differences between the DataFrames
    """
    result = {
        'rows_added': 0,
        'rows_removed': 0,
        'rows_modified': 0,
        'schema_changes': [],
        'total_cells_changed': 0,
        'column_diff_counts': {},
        'row_sample': {
            'added': [],
            'removed': [],
            'modified': []
        }
    }
    
    # Check schema changes
    cols_added = [col for col in df2.columns if col not in df1.columns]
    cols_removed = [col for col in df1.columns if col not in df2.columns]
    cols_common = [col for col in df1.columns if col in df2.columns]
    
    # Record schema changes
    for col in cols_added:
        result['schema_changes'].append(f"Added column: {col} ({df2[col].dtype})")
    
    for col in cols_removed:
        result['schema_changes'].append(f"Removed column: {col} ({df1[col].dtype})")
    
    # Check for type changes in common columns
    for col in cols_common:
        if df1[col].dtype != df2[col].dtype:
            result['schema_changes'].append(
                f"Changed type of column {col}: {df1[col].dtype} → {df2[col].dtype}"
            )
    
    # Perform row-level comparison
    if key_columns is None:
        # Determine suitable key columns if not specified
        if df1.index.name is not None and df1.index.name == df2.index.name:
            # Both DataFrames have the same named index
            key_columns = [df1.index.name]
            df1 = df1.reset_index()
            df2 = df2.reset_index()
        elif df1.index.equals(df2.index) and not df1.index.equals(pd.RangeIndex(len(df1))):
            # Both DataFrames have the same non-default index
            key_columns = ['index']
            df1 = df1.reset_index()
            df2 = df2.reset_index()
        else:
            # Look for potential key columns (those with unique values)
            potential_keys = [
                col for col in cols_common 
                if df1[col].nunique() == len(df1) and df2[col].nunique() == len(df2)
            ]
            if potential_keys:
                key_columns = [potential_keys[0]]  # Use the first unique column as key
    
    if key_columns:
        # Compare using key columns
        df1_keyed = df1.set_index(key_columns)
        df2_keyed = df2.set_index(key_columns)
        
        # Find rows only in df1 (removed)
        removed_keys = set(df1_keyed.index) - set(df2_keyed.index)
        result['rows_removed'] = len(removed_keys)
        
        # Find rows only in df2 (added)
        added_keys = set(df2_keyed.index) - set(df1_keyed.index)
        result['rows_added'] = len(added_keys)
        
        # Compare common rows
        common_keys = set(df1_keyed.index) & set(df2_keyed.index)
        
        # Gather changes per column for common rows
        modified_rows = set()
        
        for col in cols_common:
            # Skip key columns in comparing values
            if col in key_columns:
                continue
                
            # Get values for current column
            s1 = df1_keyed.loc[list(common_keys), col] if common_keys else pd.Series()
            s2 = df2_keyed.loc[list(common_keys), col] if common_keys else pd.Series()
            
            # Handle NaN comparison properly
            diff_mask = ~((s1 == s2) | (s1.isna() & s2.isna()))
            changed_keys = set(s1[diff_mask].index)
            
            # Update column diff counts
            if len(changed_keys) > 0:
                result['column_diff_counts'][col] = len(changed_keys)
                result['total_cells_changed'] += len(changed_keys)
                modified_rows.update(changed_keys)
        
        result['rows_modified'] = len(modified_rows)
        
        # Get sample rows for display
        sample_limit = 5  # Max samples to include in each category
        
        # Sample added rows
        sample_added = list(added_keys)[:sample_limit]
        for key in sample_added:
            row = df2_keyed.loc[key].to_dict()
            if isinstance(key, tuple):
                key_dict = {k: v for k, v in zip(key_columns, key)}
            else:
                key_dict = {key_columns[0]: key}
            result['row_sample']['added'].append({**key_dict, **row})
        
        # Sample removed rows
        sample_removed = list(removed_keys)[:sample_limit]
        for key in sample_removed:
            row = df1_keyed.loc[key].to_dict()
            if isinstance(key, tuple):
                key_dict = {k: v for k, v in zip(key_columns, key)}
            else:
                key_dict = {key_columns[0]: key}
            result['row_sample']['removed'].append({**key_dict, **row})
        
        # Sample modified rows
        sample_modified = list(modified_rows)[:sample_limit]
        for key in sample_modified:
            old_row = df1_keyed.loc[key].to_dict()
            new_row = df2_keyed.loc[key].to_dict()
            changes = {}
            
            # Calculate what changed in this row
            for col in cols_common:
                if col in key_columns:
                    continue
                
                old_val = old_row[col]
                new_val = new_row[col]
                
                # Check for NaN equality
                if pd.isna(old_val) and pd.isna(new_val):
                    continue
                
                if old_val != new_val:
                    changes[col] = {
                        'old': _format_value_for_display(old_val),
                        'new': _format_value_for_display(new_val)
                    }
            
            # Create the sample entry
            if isinstance(key, tuple):
                key_dict = {k: v for k, v in zip(key_columns, key)}
            else:
                key_dict = {key_columns[0]: key}
            
            result['row_sample']['modified'].append({
                'key': key_dict,
                'changes': changes
            })
    else:
        # No reliable key columns, use basic comparison
        result['rows_removed'] = len(df1) - len(df2)
        result['rows_added'] = len(df2) - len(df1)
        
        # Compare common columns from both dataframes
        common_size = min(len(df1), len(df2))
        
        # Compare common columns from both dataframes
        if cols_common and common_size > 0:
            # Compare only rows that exist in both DataFrames
            df1_common = df1[cols_common].iloc[:common_size]
            df2_common = df2[cols_common].iloc[:common_size]
            
            # Count number of different cells
            comparison = (df1_common != df2_common) | (df1_common.isna() != df2_common.isna())
            result['total_cells_changed'] = comparison.sum().sum()
            
            # Rows modified = rows with at least one difference
            result['rows_modified'] = comparison.any(axis=1).sum()
            
            # Count differences by column
            for col in cols_common:
                diff_count = comparison[col].sum()
                if diff_count > 0:
                    result['column_diff_counts'][col] = int(diff_count)
    
    return result


def _format_value_for_display(val):
    """Format a value for display in diff results."""
    if pd.isna(val):
        return None
    if isinstance(val, (np.integer, int)):
        return int(val)
    if isinstance(val, (np.floating, float)):
        return float(val)
    if isinstance(val, (np.bool_, bool)):
        return bool(val)
    return str(val)
