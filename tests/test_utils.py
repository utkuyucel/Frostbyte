"""
Tests for Frostbyte utility functions.
"""

import os
import tempfile

import pandas as pd

from frostbyte.utils.file_utils import get_file_hash, get_file_size
from frostbyte.utils.schema import extract_schema


def test_file_hash() -> None:
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


def test_file_size() -> None:
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


def test_extract_schema_csv() -> None:
    """Test schema extraction from CSV."""
    # Create a temporary CSV file
    data = {"id": [1, 2, 3], "name": ["A", "B", "C"], "value": [1.1, 2.2, 3.3]}
    df = pd.DataFrame(data)

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
        df.to_csv(temp_file.name, index=False)
        file_path = temp_file.name

    try:
        # Extract schema
        schema = extract_schema(file_path)

        # Check schema contents
        assert "row_count" in schema
        assert schema["row_count"] == 3

        assert "column_count" in schema
        assert schema["column_count"] == 3

        assert "columns" in schema
        assert "id" in schema["columns"]
        assert "name" in schema["columns"]
        assert "value" in schema["columns"]

        # Check column types
        assert "type" in schema["columns"]["id"]
        assert "type" in schema["columns"]["name"]
        assert "type" in schema["columns"]["value"]

        # Check stats for numeric column
        assert "stats" in schema["columns"]["value"]
        assert "min" in schema["columns"]["value"]["stats"]
        assert "max" in schema["columns"]["value"]["stats"]
        assert schema["columns"]["value"]["stats"]["min"] == 1.1
        assert schema["columns"]["value"]["stats"]["max"] == 3.3

    finally:
        # Clean up
        os.remove(file_path)
