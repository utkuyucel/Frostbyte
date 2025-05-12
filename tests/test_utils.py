"""
Tests for Frostbyte utility functions.
"""

import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from frostbyte.utils.file_utils import get_file_hash, get_file_size
from frostbyte.utils.schema import extract_schema
from frostbyte.utils.diff import diff_dataframes


def test_file_hash():
    """Test generating file hash."""
    # Create a temporary test file
    test_content = b"This is a test file for hashing."
    
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(test_content)
        file_path = temp_file.name
    
    try:
        # Get hash
        file_hash = get_file_hash(file_path)
        
        # Hash should be a hex string of correct length
        assert isinstance(file_hash, str)
        assert len(file_hash) == 64  # SHA-256 produces 64-character hexadecimal strings
        
        # Test consistency
        file_hash2 = get_file_hash(file_path)
        assert file_hash == file_hash2
        
    finally:
        # Clean up
        os.remove(file_path)


def test_file_size():
    """Test getting file size."""
    # Create a temporary test file
    test_content = b"This is a test file for size measurement."
    
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(test_content)
        file_path = temp_file.name
    
    try:
        # Get file size
        size = get_file_size(file_path)
        
        # Size should match the content length
        assert size == len(test_content)
        
    finally:
        # Clean up
        os.remove(file_path)


def test_extract_schema_csv():
    """Test schema extraction from CSV."""
    # Create a temporary CSV file
    data = {
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [1.1, 2.2, 3.3]
    }
    df = pd.DataFrame(data)
    
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file:
        df.to_csv(temp_file.name, index=False)
        file_path = temp_file.name
    
    try:
        # Extract schema
        schema = extract_schema(file_path)
        
        # Check schema contents
        assert 'row_count' in schema
        assert schema['row_count'] == 3
        
        assert 'column_count' in schema
        assert schema['column_count'] == 3
        
        assert 'columns' in schema
        assert 'id' in schema['columns']
        assert 'name' in schema['columns']
        assert 'value' in schema['columns']
        
        # Check column types
        assert 'type' in schema['columns']['id']
        assert 'type' in schema['columns']['name']
        assert 'type' in schema['columns']['value']
        
        # Check stats for numeric column
        assert 'stats' in schema['columns']['value']
        assert 'min' in schema['columns']['value']['stats']
        assert 'max' in schema['columns']['value']['stats']
        assert schema['columns']['value']['stats']['min'] == 1.1
        assert schema['columns']['value']['stats']['max'] == 3.3
        
    finally:
        # Clean up
        os.remove(file_path)


def test_diff_dataframes():
    """Test DataFrame diff functionality."""
    # Create two DataFrames with differences
    df1 = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'name': ['A', 'B', 'C', 'D'],
        'value': [10, 20, 30, 40]
    })
    
    # Second DataFrame has:
    # - New column (category)
    # - Missing column (name)
    # - Modified rows (value changed)
    # - Added row (id=5)
    # - Removed row (id=1)
    df2 = pd.DataFrame({
        'id': [2, 3, 4, 5],
        'value': [20, 35, 40, 50],  # Value for id=3 changed
        'category': ['X', 'Y', 'Z', 'W']  # New column
    })
    
    # Calculate differences using id as the key column
    diff_result = diff_dataframes(df1, df2, key_columns=['id'])
    
    # Check schema changes
    assert 'schema_changes' in diff_result
    schema_changes = diff_result['schema_changes']
    
    # Should detect added and removed columns
    assert any('Added column: category' in change for change in schema_changes)
    assert any('Removed column: name' in change for change in schema_changes)
    
    # Check row differences
    assert diff_result['rows_added'] == 1  # id=5
    assert diff_result['rows_removed'] == 1  # id=1
    assert diff_result['rows_modified'] == 1  # id=3 (value changed)
    
    # Check total cells changed
    assert diff_result['total_cells_changed'] > 0
    assert 'value' in diff_result['column_diff_counts']
    
    # Check row samples
    assert 'row_sample' in diff_result
    assert len(diff_result['row_sample']['added']) > 0
    assert len(diff_result['row_sample']['removed']) > 0
    assert len(diff_result['row_sample']['modified']) > 0
    
    # Test automatic key column detection
    df1_with_index = df1.set_index('id')
    df2_with_index = df2.set_index('id')
    diff_result_auto = diff_dataframes(df1_with_index, df2_with_index)
    
    # Should have similar results to the explicit key version
    assert diff_result_auto['rows_added'] == diff_result['rows_added']
    assert diff_result_auto['rows_removed'] == diff_result['rows_removed']


def test_diff_dataframes_no_keys():
    """Test DataFrame diff functionality without key columns."""
    # Create two DataFrames without reliable keys
    df1 = pd.DataFrame({
        'col1': [1, 2, 3, 4],
        'col2': ['A', 'B', 'C', 'D']
    })
    
    df2 = pd.DataFrame({
        'col1': [1, 3, 4, 5],  # First row matches, others shifted
        'col2': ['X', 'C', 'D', 'E']  # First row changed, others shifted
    })
    
    # Calculate differences without specifying key columns
    diff_result = diff_dataframes(df1, df2)
    
    # Basic checks on the results
    assert 'rows_added' in diff_result
    assert 'rows_removed' in diff_result
    assert 'total_cells_changed' in diff_result
    
    # Should detect some changes
    assert diff_result['total_cells_changed'] > 0
