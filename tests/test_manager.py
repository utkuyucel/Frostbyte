"""
Tests for the Frostbyte ArchiveManager class.
"""

import os
import tempfile
from typing import Generator

import pandas as pd
import pytest

from frostbyte.core.manager import ArchiveManager


@pytest.fixture
def temp_workspace() -> Generator[str, None, None]:
    """Create a temporary workspace for testing."""
    old_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as temp_dir:
        os.chdir(temp_dir)
        yield temp_dir
        os.chdir(old_cwd)


@pytest.fixture
def sample_csv(temp_workspace: str) -> str:
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


def test_initialize(temp_workspace: str) -> None:
    """Test initializing a new Frostbyte repository."""
    manager = ArchiveManager()
    result = manager.initialize()
    
    assert result is True
    assert os.path.exists('.frostbyte')
    assert os.path.exists('.frostbyte/archives')
    assert os.path.exists('.frostbyte/manifest.db')


def test_archive_restore(temp_workspace: str, sample_csv: str) -> None:
    """Test archiving and restoring a file."""
    manager = ArchiveManager()
    manager.initialize()
    
    # Archive the file
    archive_result = manager.archive(sample_csv)
    
    assert archive_result['original_path'] == sample_csv
    assert archive_result['version'] == 1
    assert os.path.exists(os.path.join('.frostbyte', 'archives', 'sample_v1.csv.fbyt'))
    
    # Delete the original file
    os.remove(sample_csv)
    assert not os.path.exists(sample_csv)
    
    # Restore the file
    restore_result = manager.restore(sample_csv)
    
    assert restore_result['original_path'] == sample_csv
    assert restore_result['version'] == 1
    assert os.path.exists(sample_csv)
    
    # Read the restored file and check its content
    df = pd.read_csv(sample_csv)
    assert len(df) == 100
    assert 'id' in df.columns
    assert 'value' in df.columns
    assert 'name' in df.columns


def test_list_archives(temp_workspace: str, sample_csv: str) -> None:
    """Test listing archive files."""
    manager = ArchiveManager()
    manager.initialize()
    
    # Archive a file
    manager.archive(sample_csv)
    
    # List archives
    archives = manager.list_archives()
    
    assert len(archives) == 1
    assert archives[0]['original_path'] == sample_csv
    assert archives[0]['latest_version'] == 1
    
    # Archive it again to create a new version
    manager.archive(sample_csv)
    
    # List all versions
    archives = manager.list_archives(show_all=True)
    
    assert len(archives) == 2
    assert archives[0]['original_path'] == sample_csv
    assert archives[0]['version'] == 1
    assert archives[1]['original_path'] == sample_csv
    assert archives[1]['version'] == 2


def test_purge(temp_workspace: str, sample_csv: str) -> None:
    """Test purging archive files."""
    manager = ArchiveManager()
    manager.initialize()
    
    # Create multiple versions
    manager.archive(sample_csv)
    manager.archive(sample_csv)
    manager.archive(sample_csv)
    
    # Purge specific version
    purge_result = manager.purge(f"{sample_csv}@2")
    
    assert purge_result['original_path'] == sample_csv
    assert purge_result['version'] == 2
    
    # List archives to confirm the purge
    archives = manager.list_archives(show_all=True)
    versions = [a['version'] for a in archives]
    
    assert 2 not in versions
    assert 1 in versions
    assert 3 in versions
    
    # Purge all versions
    purge_result = manager.purge(sample_csv, all_versions=True)
    
    assert purge_result['original_path'] == sample_csv
    assert purge_result['count'] > 0
    
    # Confirm all versions are gone
    archives = manager.list_archives()
    assert len(archives) == 0


def test_manager_initialization() -> None:
    """Test initializing the archive manager."""
    manager = ArchiveManager()
    assert manager.base_dir.exists()


def test_manager_archive_and_restore() -> None:
    """Test archiving and restoring a file using the manager."""
    manager = ArchiveManager()
    manager.initialize()

    # Archive a file
    file_path = "test_file.csv"
    with open(file_path, "w") as f:
        f.write("id,value\n1,100\n2,200")

    archive_info = manager.archive(file_path)
    assert archive_info["version"] == 1

    # Restore the file
    restored_info = manager.restore(f"{file_path}@1")
    assert restored_info["original_path"] == file_path
