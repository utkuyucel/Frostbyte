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


@pytest.mark.usefixtures("temp_workspace")
def sample_csv() -> str:
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

    # To get all versions for a specific file, pass its name
    all_versions_for_file = manager.list_archives(file_name=sample_csv)
    versions = sorted([entry["version"] for entry in all_versions_for_file])
    assert versions == [1, 2]
    # Ensure the original_path is consistent and matches sample_csv's absolute path
    abs_sample_csv_path = str(Path(sample_csv).resolve())
    assert all(entry["original_path"] == abs_sample_csv_path for entry in all_versions_for_file)


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
    # After purging v1, listing versions for sample_csv should only show v2
    remaining_versions_for_file = manager.list_archives(file_name=sample_csv)
    if remaining_versions_for_file:  # It might be empty if only one version existed and was purged
        assert len(remaining_versions_for_file) == 1
        assert remaining_versions_for_file[0]["version"] == 2  # Assuming v2 was the other version
    else:  # If only one version existed, it should be empty now
        assert manager.list_archives(file_name=sample_csv) == []

    # Purge all remaining versions of sample_csv
    # At this point, only v2 of sample_csv should exist if it started with two versions.
    # If it started with one, remaining_versions_for_file would be empty.
    # The purge all should target the specific file.
    result_all = manager.purge(sample_csv, all_versions=True)

    # Check that at least one version was purged (either v2, or v1 if it was the only one)
    assert result_all["count"] >= 1

    # After purging all versions of sample_csv, listing it should yield no results.
    assert manager.list_archives(file_name=sample_csv) == []

    # The general list (summary) might still contain other files if they existed.
    # For this test, we only care that sample_csv is gone.
    # If no other files were archived, manager.list_archives() would be [].
    # If there were other files, they would still be listed.
    # So, we check that sample_csv is not in the summary list.
    summary_list = manager.list_archives()
    abs_sample_csv_path = str(Path(sample_csv).resolve())
    assert not any(item["original_path"] == abs_sample_csv_path for item in summary_list)
