"""
Tests for the Frostbyte ArchiveManager class.
"""

import os
import tempfile
from pathlib import Path
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
def sample_csv(_: str) -> str:
    """Create a sample CSV file for testing."""
    data = {
        "id": range(100),
        "value": [i * 2 for i in range(100)],
        "name": [f"item-{i}" for i in range(100)],
    }
    df = pd.DataFrame(data)

    # Create data directory
    os.makedirs("data", exist_ok=True)

    # Save the DataFrame to a CSV file
    file_path = os.path.join("data", "sample.csv")
    df.to_csv(file_path, index=False)

    return file_path


def test_initialize(_: str) -> None:
    """Test initializing a new Frostbyte repository."""
    manager = ArchiveManager()
    result = manager.initialize()

    assert result is True
    assert os.path.exists(".frostbyte")
    assert os.path.exists(".frostbyte/archives")
    assert os.path.exists(".frostbyte/manifest.db")


def test_archive_restore(_: str, sample_csv: str) -> None:
    """Test archiving and restoring a file."""
    manager = ArchiveManager()
    manager.initialize()

    # Archive the file
    archive_result = manager.archive(sample_csv)

    assert archive_result["original_path"] == sample_csv
    assert archive_result["version"] == 1
    assert os.path.exists(os.path.join(".frostbyte", "archives", "sample_v1.parquet"))

    # Delete the original file
    os.remove(sample_csv)
    assert not os.path.exists(sample_csv)

    # Restore the file (using default latest version)
    restore_result = manager.restore(sample_csv)

    assert Path(restore_result["original_path"]).name == Path(sample_csv).name
    assert restore_result["version"] == 1
    assert os.path.exists(sample_csv)

    # Read the restored file and check its content
    df = pd.read_csv(sample_csv)
    assert len(df) == 100
    assert "id" in df.columns
    assert "value" in df.columns
    assert "name" in df.columns


def test_list_archives(_: str, sample_csv: str) -> None:
    """Test listing archive files."""
    manager = ArchiveManager()
    manager.initialize()

    # Archive a file
    manager.archive(sample_csv)

    # List archives
    archives = manager.list_archives()

    assert len(archives) == 1
    # We use Path to normalize paths for comparison since one might be absolute and one relative
    assert Path(archives[0]["original_path"]).name == Path(sample_csv).name
    assert archives[0]["latest_version"] == 1

    # Archive it again to create a new version
    manager.archive(sample_csv)

    # List all versions
    archives = manager.list_archives(show_all=True)

    assert len(archives) == 2
    assert Path(archives[0]["original_path"]).name == Path(sample_csv).name
    assert archives[0]["version"] == 1
    assert Path(archives[1]["original_path"]).name == Path(sample_csv).name
    assert archives[1]["version"] == 2


def test_purge(_: str, sample_csv: str) -> None:
    """Test purging archive files."""
    manager = ArchiveManager()
    manager.initialize()

    # Create multiple versions
    manager.archive(sample_csv)
    manager.archive(sample_csv)
    manager.archive(sample_csv)

    # Check what versions we have
    initial_archives = manager.list_archives(show_all=True)
    versions_before = [a["version"] for a in initial_archives]

    # Make sure we have all three versions
    assert 1 in versions_before
    assert 2 in versions_before
    assert 3 in versions_before

    # Purge specific version
    purge_result = manager.purge(sample_csv, 2)

    assert Path(purge_result["original_path"]).name == Path(sample_csv).name
    assert purge_result["version"] == 2

    # List archives to confirm the purge
    archives = manager.list_archives(show_all=True)
    versions = [
        a["version"] for a in archives if Path(a["original_path"]).name == Path(sample_csv).name
    ]

    # These assertions should pass if purge worked correctly
    assert 2 not in versions, f"Version 2 still found after purge: {versions}"
    assert 1 in versions
    assert 3 in versions

    # Purge all versions
    purge_result = manager.purge(sample_csv, all_versions=True)

    assert Path(purge_result["original_path"]).name == Path(sample_csv).name
    assert purge_result["count"] > 0

    # Verify all versions are gone
    final_archives = manager.list_archives(show_all=True)
    final_versions = [
        a["version"]
        for a in final_archives
        if Path(a["original_path"]).name == Path(sample_csv).name
    ]
    assert len(final_versions) == 0, f"Still found versions after purge all: {final_versions}"

    # Confirm all versions are gone
    archives = manager.list_archives()
    assert len(archives) == 0


def test_manager_initialization() -> None:
    """Test initializing the archive manager."""
    manager = ArchiveManager()
    assert manager.base_dir.exists()


def test_manager_archive_and_restore() -> None:
    """Test archiving and restoring a file using the manager."""
    # Use a specific directory to ensure isolation between test runs
    with tempfile.TemporaryDirectory() as temp_dir:
        # Initialize the manager in this directory
        os.chdir(temp_dir)
        manager = ArchiveManager()
        manager.initialize()

        # Create the file in the temporary directory with absolute path
        file_path = os.path.join(temp_dir, "test_file.csv")
        with open(file_path, "w") as f:
            f.write("id,value\n1,100\n2,200")

        # Archive the file
        archive_info = manager.archive(file_path)
        assert archive_info["version"] == 1

        # Delete the original file to test restore
        os.remove(file_path)
        assert not os.path.exists(file_path)

        # Restore the file with explicit version
        restored_info = manager.restore(file_path, 1)
        assert Path(restored_info["original_path"]).name == "test_file.csv"
        assert os.path.exists(file_path)
