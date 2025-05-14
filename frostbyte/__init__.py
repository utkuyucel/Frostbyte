"""
Frostbyte: Cold Data Archiving for Pandas Workflows

A lightweight, local-first tool for efficient compression, versioning, and management of data.
"""

__version__ = "0.1.0"

from typing import Dict, List, Optional

from frostbyte.core.manager import ArchiveManager


# Use a class to manage singleton instance instead of global variables
class _ManagerProvider:
    """Singleton provider for archive manager instance."""

    _instance: Optional[ArchiveManager] = None

    @classmethod
    def get(cls) -> ArchiveManager:
        """Get or create the archive manager instance."""
        if cls._instance is None:
            cls._instance = ArchiveManager()
        return cls._instance


def get_manager() -> ArchiveManager:
    """Get the archive manager instance."""
    return _ManagerProvider.get()


def init() -> bool:
    """Initialize a new Frostbyte repository."""
    return get_manager().initialize()


def archive(file_path: str) -> Dict:
    """Archive a file and store its metadata."""
    return get_manager().archive(file_path)


def restore(path_spec: str, version: Optional[int] = None) -> Dict:
    """Restore an archived file using path, version, archive filename, or partial name.

    Args:
        path_spec: Path or name of the file to restore
        version: Specific version to restore (if None, latest version is used)
    """
    return get_manager().restore(path_spec, version)


def ls(show_all: bool = False) -> List[Dict]:
    """List archived files, optionally showing all versions."""
    return get_manager().list_archives(show_all)


def stats(file_path: Optional[str] = None) -> Dict:
    """Get statistics about archived files, for specific file or all archives."""
    return get_manager().get_stats(file_path)


def purge(file_path: str, version: Optional[int] = None, all_versions: bool = False) -> Dict:
    """Remove specific archive versions or all versions of a file.

    Args:
        file_path: Path of the file to purge
        version: Specific version to purge (latest is used if None and all_versions is False)
        all_versions: If True, purge all versions of the file
    """
    return get_manager().purge(file_path, version, all_versions)


def find_by_name(name_part: str) -> List[Dict]:
    """Find archives by partial filename match."""
    return get_manager().find_by_name(name_part)
