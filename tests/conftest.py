"""
Test configuration for Frostbyte.
"""

import os
import tempfile

import pandas as pd
import pytest


@pytest.fixture
def temp_workspace():
    """Create a temporary workspace for testing."""
    old_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as temp_dir:
        os.chdir(temp_dir)
        yield temp_dir
        os.chdir(old_cwd)


@pytest.fixture
def sample_csv(temp_workspace):
    """Create a sample CSV file for testing."""
    data = {
        'id': range(100),
        'value': [i * 2 for i in range(100)],
        'name': [f'item-{i}' for i in range(100)]
    }
    df = pd.DataFrame(data)
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    # Save the DataFrame to a CSV file
    file_path = os.path.join('data', 'sample.csv')
    df.to_csv(file_path, index=False)
    
    return file_path


@pytest.fixture
def sample_parquet(temp_workspace):
    """Create a sample Parquet file for testing."""
    data = {
        'id': range(100),
        'value': [i * 2 for i in range(100)],
        'name': [f'item-{i}' for i in range(100)]
    }
    df = pd.DataFrame(data)
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    # Save the DataFrame to a Parquet file
    file_path = os.path.join('data', 'sample.parquet')
    df.to_parquet(file_path, index=False)
    
    return file_path
