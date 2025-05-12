"""
Tests for the Frostbyte compressor module.
"""

import os
import tempfile
from pathlib import Path

import pytest

from frostbyte.core.compressor import Compressor


def test_compress_decompress():
    """Test that a file can be compressed and decompressed correctly."""
    # Create a temporary test file
    test_content = b"This is a test file content for compression testing."
    
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(test_content)
        source_path = temp_file.name
    
    try:
        # Create a temporary output file
        with tempfile.NamedTemporaryFile(delete=False) as compressed_file:
            compressed_path = compressed_file.name
        
        with tempfile.NamedTemporaryFile(delete=False) as restored_file:
            restored_path = restored_file.name
        
        # Compress the file
        compressor = Compressor()
        compressed_size = compressor.compress(source_path, compressed_path)
        
        # Verify compression worked
        assert os.path.exists(compressed_path)
        assert compressed_size > 0
        assert compressed_size < len(test_content)  # Should be smaller
        
        # Decompress the file
        decompressed_size = compressor.decompress(compressed_path, restored_path)
        
        # Verify decompression worked
        assert os.path.exists(restored_path)
        assert decompressed_size == len(test_content)
        
        # Check content is the same
        with open(restored_path, 'rb') as f:
            restored_content = f.read()
        
        assert restored_content == test_content
        
    finally:
        # Clean up temp files
        for path in [source_path, compressed_path, restored_path]:
            try:
                os.remove(path)
            except:
                pass
