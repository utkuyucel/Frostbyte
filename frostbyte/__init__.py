"""
Frostbyte: Cold Data Archiving for Pandas Workflows

A lightweight, local-first cold data archiving tool for data engineers, scientists, and ML practitioners.
"""

__version__ = "0.1.0"

from pathlib import Path
from typing import Dict, List, Optional

from frostbyte.core.manager import ArchiveManager
from frostbyte.utils.config import Config

# Initialize the global archive manager
_manager: Optional[ArchiveManager] = None


def get_manager() -> ArchiveManager:
    """Get the global archive manager instance."""
    global _manager
    if _manager is None:
        _manager = ArchiveManager()
    return _manager


def init(path: Optional[str] = None) -> bool:
    """Initialize a new Frostbyte repository."""
    return get_manager().initialize()


def archive(file_path: str) -> Dict:
    """Archive a file and store its metadata."""
    return get_manager().archive(file_path)


def restore(path_spec: str) -> Dict:
    """Restore an archived file using path, version, archive filename, or partial name."""
    return get_manager().restore(path_spec)


def ls(show_all: bool = False) -> List[Dict]:
    """List archived files, optionally showing all versions."""
    return get_manager().list_archives(show_all)


def stats(file_path: Optional[str] = None) -> Dict:
    """Get statistics about archived files, for specific file or all archives."""
    return get_manager().get_stats(file_path)


def purge(file_path: str, all_versions: bool = False) -> Dict:
    """Remove specific archive versions or all versions of a file."""
    return get_manager().purge(file_path, all_versions)


def find_by_name(name_part: str) -> List[Dict]:
    """Find archives by partial filename match."""
    return get_manager().find_by_name(name_part)
