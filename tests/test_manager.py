"""
Tests for the Frostbyte ArchiveManager class.
"""

import os
import tempfile
from pathlib import Path
from typing import Generator

import pytest

from frostbyte.core.manager import ArchiveManager

@pytest.fixture
def temp_workspace() -> Generator[str, None, None]:
    """Create a temporary workspace for testing."""
    old_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmp_dir:
        os.chdir(tmp_dir)
        yield tmp_dir
        os.chdir(old_cwd)

@pytest.fixture
def sample_csv(temp_workspace: str) -> str:
    """Create a simple sample CSV file for testing."""
    data = "id,value\n1,10\n2,20\n3,30\n"
    path = Path("data.csv")
    path.write_text(data)
    return str(path)

@pytest.mark.usefixtures("temp_workspace")
def test_initialize_repository() -> None:
    """Test initializing the archive manager."""
    manager = ArchiveManager()
    assert manager.initialize() is True
    assert os.path.isdir(".frostbyte")
    assert os.path.isdir(".frostbyte/archives")

@pytest.mark.usefixtures("temp_workspace")
def test_archive_and_list(sample_csv: str) -> None:
    """Test archiving a file and listing archives."""
    manager = ArchiveManager()
    manager.initialize()
    info = manager.archive(sample_csv)
    assert info["version"] == 1

    listings = manager.list_archives()
    assert len(listings) == 1
    assert listings[0]["latest_version"] == 1

    # Archive again to create version 2
    info2 = manager.archive(sample_csv)
    assert info2["version"] == 2

    all_versions = manager.list_archives(show_all=True)
    versions = sorted([entry["version"] for entry in all_versions])
    assert versions == [1, 2]

@pytest.mark.usefixtures("temp_workspace")
def test_purge_versions(sample_csv: str) -> None:
    """Test purging specific and all versions."""
    manager = ArchiveManager()
    manager.initialize()
    manager.archive(sample_csv)
    manager.archive(sample_csv)
    # Purge version 1
    result1 = manager.purge(sample_csv, version=1)
    assert result1["count"] == 1
    remaining = manager.list_archives(show_all=True)
    assert all(entry["version"] != 1 for entry in remaining)
    # Purge all
    result_all = manager.purge(sample_csv, all_versions=True)
    assert result_all["count"] >= 1
    assert manager.list_archives() == []
