"""
Tests for the Frostbyte diff functionality.
"""

import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from frostbyte import archive, diff, init, get_manager
from frostbyte.core.diff import diff_files, _parse_path_spec, _load_dataframes
from frostbyte.utils.diff import diff_dataframes


@pytest.fixture
def temp_archive():
    """Create a temporary archive for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["FROSTBYTE_ROOT"] = temp_dir
        init()
        yield temp_dir


@pytest.fixture
def sample_dataframes():
    """Create sample dataframes for testing."""
    df1 = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'name': ['A', 'B', 'C', 'D'],
        'value': [10, 20, 30, 40]
    })
    
    df2 = pd.DataFrame({
        'id': [2, 3, 4, 5],
        'value': [20, 35, 40, 50],
        'category': ['X', 'Y', 'Z', 'W']
    })
    
    return df1, df2


def test_parse_path_spec():
    """Test parsing path specifications."""
    # Test with version
    path, version = _parse_path_spec("data/file.csv@2")
    assert path == "data/file.csv"
    assert version == 2
    
    # Test with float version
    path, version = _parse_path_spec("data/file.csv@1.5")
    assert path == "data/file.csv"
    assert version == 1.5
    
    # Test without version
    path, version = _parse_path_spec("data/file.csv")
    assert path == "data/file.csv"
    assert version is None


def test_diff_dataframes_with_key(sample_dataframes):
    """Test diff_dataframes with key columns."""
    df1, df2 = sample_dataframes
    
    # Use id as key column
    result = diff_dataframes(df1, df2, key_columns=['id'])
    
    # Check basic counts
    assert result['rows_added'] == 1  # id=5 added
    assert result['rows_removed'] == 1  # id=1 removed
    assert result['rows_modified'] == 1  # id=3 value changed
    
    # Check schema changes
    assert len(result['schema_changes']) == 2
    assert any('Added column: category' in change for change in result['schema_changes'])
    assert any('Removed column: name' in change for change in result['schema_changes'])
    
    # Check samples
    assert len(result['row_sample']['added']) == 1
    assert len(result['row_sample']['removed']) == 1
    assert len(result['row_sample']['modified']) == 1
    
    # Check the modified sample details
    modified = result['row_sample']['modified'][0]
    assert modified['key']['id'] == 3
    assert 'value' in modified['changes']
    assert modified['changes']['value']['old'] == 30
    assert modified['changes']['value']['new'] == 35


def test_diff_dataframes_auto_key(sample_dataframes):
    """Test diff_dataframes with automatic key detection."""
    df1, df2 = sample_dataframes
    
    # Set index on both dataframes
    df1.set_index('id', inplace=True)
    df2.set_index('id', inplace=True)
    
    result = diff_dataframes(df1, df2)
    
    # Should get similar results as with explicit key
    assert result['rows_added'] == 1
    assert result['rows_removed'] == 1
    assert result['rows_modified'] > 0


def test_diff_files_with_archive(temp_archive, sample_dataframes):
    """Test diff_files with archived files."""
    df1, df2 = sample_dataframes
    
    # Save dataframes to temporary files
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file1:
        df1.to_csv(temp_file1.name, index=False)
        file1_path = temp_file1.name
        
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file2:
        df2.to_csv(temp_file2.name, index=False)
        file2_path = temp_file2.name
    
    try:
        # Archive the files
        archive(file1_path)
        archive(file2_path)
        
        # Compare the files
        result = diff_files(file1_path, file2_path)
        
        # Basic checks
        assert 'rows_added' in result
        assert 'rows_removed' in result
        assert 'schema_changes' in result
        
        # Specific checks
        assert result['rows_added'] > 0
        assert result['rows_removed'] > 0
        assert len(result['schema_changes']) > 0
        
    finally:
        # Clean up
        if os.path.exists(file1_path):
            os.remove(file1_path)
        if os.path.exists(file2_path):
            os.remove(file2_path)


def test_diff_api(temp_archive, sample_dataframes):
    """Test the public diff API."""
    df1, df2 = sample_dataframes
    
    # Save dataframes to temporary files
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file1:
        df1.to_csv(temp_file1.name, index=False)
        file1_path = temp_file1.name
        
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file2:
        df2.to_csv(temp_file2.name, index=False)
        file2_path = temp_file2.name
    
    try:
        # Archive the files
        v1_info = archive(file1_path)
        v2_info = archive(file2_path)
        
        # Test diff API
        result = diff(file1_path, file2_path)
        
        # Basic checks
        assert 'rows_added' in result
        assert 'rows_removed' in result
        assert 'schema_changes' in result
        assert 'total_cells_changed' in result
        
        # Test with version specifiers
        v1 = v1_info.get('version', 1)
        result_with_version = diff(f"{file1_path}@{v1}", file2_path)
        
        assert result_with_version['rows_added'] == result['rows_added']
        assert result_with_version['rows_removed'] == result['rows_removed']
        
    finally:
        # Clean up
        if os.path.exists(file1_path):
            os.remove(file1_path)
        if os.path.exists(file2_path):
            os.remove(file2_path)
