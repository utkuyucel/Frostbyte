"""
File utilities for Frostbyte.
"""

import hashlib
import os
from pathlib import Path
from typing import Union


def get_file_hash(file_path: Union[str, Path]) -> str:
    """Calculate SHA-256 hash of a file."""
    file_path = Path(file_path)
    sha256 = hashlib.sha256()
    
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            sha256.update(chunk)
    
    return sha256.hexdigest()


def get_file_size(file_path: Union[str, Path]) -> int:
    """Get the size of a file in bytes."""
    file_path = Path(file_path)
    return file_path.stat().st_size
