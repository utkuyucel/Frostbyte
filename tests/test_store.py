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

    all_arch = store.list_archives(show_all=True)
    assert len(all_arch) == 2

    latest_arch = store.list_archives(show_all=False)
    assert len(latest_arch) == 1 and latest_arch[0]["latest_version"] == 2

    res = store.remove_archives(file, version=1)
    assert res["count"] == 1

    res_all = store.remove_archives(file, all_versions=True)
    assert res_all["count"] == 1
