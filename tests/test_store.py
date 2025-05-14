"""
Tests for the Frostbyte MetadataStore class.
"""

import os
import tempfile
from datetime import datetime
from typing import Generator

import duckdb
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
        # Clean up the database file
        try:
            os.remove(db_path)
        except Exception as e:
            print(f"Error: {e}")


def test_initialize_store(temp_db: str) -> None:
    """Test initializing the metadata store."""
    store = MetadataStore(temp_db)
    store.initialize()

    # There's no easy way to verify the tables were created without querying,
    # so we'll check if the file exists and has some content
    assert os.path.exists(temp_db)
    assert os.path.getsize(temp_db) > 0


def test_store_initialization() -> None:
    """Test initializing the metadata store."""
    store = MetadataStore(":memory:")
    store.initialize()

    # Ensure the database schema is created
    conn = duckdb.connect(store.db_path)
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    assert len(tables) > 0


def test_add_get_archive(temp_db: str) -> None:
    """Test adding and retrieving archive metadata."""
    store = MetadataStore(temp_db)
    store.initialize()

    # Add an archive entry
    archive_id = "test-id-123"
    original_path = "/data/test.csv"
    version = 1
    timestamp = datetime.now()
    file_hash = "abcdef123456"
    row_count = 1000
    schema = {
        "columns": {
            "id": {"type": "int64", "nullable": False},
            "value": {"type": "float64", "nullable": True},
        }
    }
    compression_ratio = 75.5
    storage_path = "/.frostbyte/archives/test_v1.csv.fbyt"

    store.add_archive(
        id=archive_id,
        original_path=original_path,
        version=version,
        timestamp=timestamp,
        hash=file_hash,
        row_count=row_count,
        schema=schema,
        compression_ratio=compression_ratio,
        storage_path=storage_path,
    )

    # Retrieve the archive
    archive = store.get_archive(original_path, version)

    assert archive is not None
    assert archive["id"] == archive_id
    assert archive["original_path"] == original_path
    assert archive["version"] == version
    assert isinstance(archive["timestamp"], (str, datetime))
    assert archive["hash"] == file_hash
    assert archive["row_count"] == row_count
    assert archive["compression_ratio"] == compression_ratio
    assert archive["storage_path"] == storage_path


def test_get_next_version(temp_db: str) -> None:
    """Test getting the next version number for a file."""
    store = MetadataStore(temp_db)
    store.initialize()

    file_path = "/data/test.csv"

    # First version should be 1
    next_version = store.get_next_version(file_path)
    assert next_version == 1

    # Add an archive entry
    store.add_archive(
        id="test-id-1",
        original_path=file_path,
        version=1,
        timestamp=datetime.now(),
        hash="hash1",
        row_count=100,
        schema={},
        compression_ratio=50.0,
        storage_path="path1",
    )

    # Next version should be 2
    next_version = store.get_next_version(file_path)
    assert next_version == 2

    # Add another version
    store.add_archive(
        id="test-id-2",
        original_path=file_path,
        version=2,
        timestamp=datetime.now(),
        hash="hash2",
        row_count=110,
        schema={},
        compression_ratio=55.0,
        storage_path="path2",
    )

    # Next version should be 3
    next_version = store.get_next_version(file_path)
    assert next_version == 3


def test_list_archives(temp_db: str) -> None:
    """Test listing archive entries."""
    store = MetadataStore(temp_db)
    store.initialize()

    file1_path = "/data/test1.csv"
    file2_path = "/data/test2.csv"

    # Add multiple archive entries
    store.add_archive(
        id="test-id-1",
        original_path=file1_path,
        version=1,
        timestamp=datetime.now(),
        hash="hash1",
        row_count=100,
        schema={},
        compression_ratio=50.0,
        storage_path="path1",
    )

    store.add_archive(
        id="test-id-2",
        original_path=file1_path,
        version=2,
        timestamp=datetime.now(),
        hash="hash2",
        row_count=110,
        schema={},
        compression_ratio=55.0,
        storage_path="path2",
    )

    store.add_archive(
        id="test-id-3",
        original_path=file2_path,
        version=1,
        timestamp=datetime.now(),
        hash="hash3",
        row_count=200,
        schema={},
        compression_ratio=60.0,
        storage_path="path3",
    )

    # List all archives
    archives = store.list_archives(show_all=True)
    assert len(archives) == 3

    # List latest versions only
    archives = store.list_archives(show_all=False)
    assert len(archives) == 2

    # Check the versions are correct
    file1_archive = next((a for a in archives if a["original_path"] == file1_path), None)
    file2_archive = next((a for a in archives if a["original_path"] == file2_path), None)

    assert file1_archive is not None
    assert file2_archive is not None
    assert file1_archive["latest_version"] == 2
    assert file2_archive["latest_version"] == 1


def test_remove_archives(temp_db: str) -> None:
    """Test removing archive entries."""
    store = MetadataStore(temp_db)
    store.initialize()

    file_path = "/data/test.csv"

    # Add multiple versions
    store.add_archive(
        id="test-id-1",
        original_path=file_path,
        version=1,
        timestamp=datetime.now(),
        hash="hash1",
        row_count=100,
        schema={},
        compression_ratio=50.0,
        storage_path="path1",
    )

    store.add_archive(
        id="test-id-2",
        original_path=file_path,
        version=2,
        timestamp=datetime.now(),
        hash="hash2",
        row_count=110,
        schema={},
        compression_ratio=55.0,
        storage_path="path2",
    )

    # Remove version 1
    result = store.remove_archives(file_path, version=1)
    assert result["count"] > 0
    assert len(result["storage_paths"]) == 1
    assert result["storage_paths"][0] == "path1"

    # Check it was removed
    archives = store.list_archives(show_all=True)
    versions = [a["version"] for a in archives]
    assert 1 not in versions
    assert 2 in versions

    # Remove all versions
    result = store.remove_archives(file_path, all_versions=True)
    assert result["count"] > 0
    assert len(result["storage_paths"]) == 1
    assert result["storage_paths"][0] == "path2"

    # Check all were removed
    archives = store.list_archives()
    assert len(archives) == 0


def test_store_add_archive() -> None:
    """Test adding an archive to the metadata store."""
    store = MetadataStore(":memory:")
    store.initialize()

    store.add_archive(
        id="test-id",
        original_path="/path/to/file",
        version=1,
        timestamp=datetime.now(),
        hash="test-hash",
        row_count=100,
        schema={},
        compression_ratio=0.5,
        storage_path="/path/to/archive",
    )

    # Verify the archive was added
    conn = duckdb.connect(store.db_path)
    result = conn.execute("SELECT * FROM archives WHERE id = 'test-id';").fetchone()
    assert result is not None
