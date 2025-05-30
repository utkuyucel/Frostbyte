import hashlib
from functools import lru_cache
from pathlib import Path
from typing import Union


@lru_cache(maxsize=256)
def get_file_hash(file_path: Union[str, Path]) -> str:
    """Compute SHA256 hash of file with caching for performance."""
    file_path = Path(file_path)
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def get_file_size(file_path: Union[str, Path]) -> int:
    """Get file size in bytes."""
    return Path(file_path).stat().st_size
