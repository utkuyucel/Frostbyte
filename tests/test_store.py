"""
Tests for the Frostbyte MetadataStore class.
"""

import tempfile
from datetime import datetime
from typing import Generator

import pytest

from frostbyte.core.store import MetadataStore


@pytest.fixture
def temp_db() -> Generator[str, None, None]:
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
        db_path = temp_file.name
    try:
        yield db_path
    finally:
        try:
            import os

            os.remove(db_path)
        except Exception:
            pass


def test_get_next_version(temp_db: str) -> None:
    """Test getting the next version number for a file."""
    store = MetadataStore(temp_db)
    store.initialize()

    file_path = "/data/test.csv"
    assert store.get_next_version(file_path) == 1

    store.add_archive("id1", file_path, 1, datetime.now(), "h1", 100, {}, 50.0, "p1")
    assert store.get_next_version(file_path) == 2


def test_list_and_remove_archives(temp_db: str) -> None:
    """Test listing and removing archive entries."""
    store = MetadataStore(temp_db)
    store.initialize()

    file = "/data/test.csv"
    store.add_archive("id1", file, 1, datetime.now(), "h1", 100, {}, 50.0, "p1")
    store.add_archive("id2", file, 2, datetime.now(), "h2", 110, {}, 55.0, "p2")

    # Test detailed view for the specific file
    all_versions_of_file = store.list_archives(file_name=file)
    assert len(all_versions_of_file) == 2
    versions = sorted([a["version"] for a in all_versions_of_file])
    assert versions == [1, 2]

    # Test summary view (should show one entry for 'file' with latest_version 2)
    summary_list = store.list_archives()  # No file_name means summary
    assert len(summary_list) == 1
    assert summary_list[0]["original_path"] == file
    assert summary_list[0]["latest_version"] == 2
    assert summary_list[0]["version_count"] == 2

    # Test removing a specific version (version 1)
    res = store.remove_archives(file, version=1)
    assert res["count"] == 1

    # After removing v1, detailed view for 'file' should only show v2
    remaining_versions_of_file = store.list_archives(file_name=file)
    assert len(remaining_versions_of_file) == 1
    assert remaining_versions_of_file[0]["version"] == 2

    # Summary view should now show latest_version 2 and version_count 1
    summary_list_after_purge_v1 = store.list_archives()
    assert len(summary_list_after_purge_v1) == 1
    assert summary_list_after_purge_v1[0]["latest_version"] == 2
    assert summary_list_after_purge_v1[0]["version_count"] == 1

    # Test removing all remaining versions of 'file' (which is just v2)
    res_all = store.remove_archives(file, all_versions=True)
    assert res_all["count"] == 1  # Only v2 was left

    # After purging all, detailed view for 'file' should be empty
    assert store.list_archives(file_name=file) == []
    # Summary view should also be empty as no other files were archived
    assert store.list_archives() == []
