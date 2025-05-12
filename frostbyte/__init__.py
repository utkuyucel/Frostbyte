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
    """
    Get the global archive manager instance.
    
    Returns:
        ArchiveManager: The global archive manager instance.
    """
    global _manager
    if _manager is None:
        _manager = ArchiveManager()
    return _manager


def init(path: Optional[str] = None) -> bool:
    """
    Initialize a new Frostbyte repository.
    
    Args:
        path: Path to initialize. If None, uses current directory.
        
    Returns:
        bool: True if successful, False otherwise.
    """
    return get_manager().initialize()


def archive(file_path: str) -> Dict:
    """
    Archive a file.
    
    Args:
        file_path: Path to the file to archive.
        
    Returns:
        Dict: Information about the archived file.
    """
    return get_manager().archive(file_path)


def restore(path_spec: str) -> Dict:
    """
    Restore an archived file to its original location.
    
    Args:
        path_spec: Path with optional version (e.g., 'data/file.csv@2')
        
    Returns:
        Dict: Information about the restored file.
    """
    return get_manager().restore(path_spec)


def ls(show_all: bool = False) -> List[Dict]:
    """
    List archived files.
    
    Args:
        show_all: If True, show all versions; otherwise, show latest only.
        
    Returns:
        List[Dict]: Archive information.
    """
    return get_manager().list_archives(show_all)


def stats(file_path: Optional[str] = None) -> Dict:
    """
    Get statistics about archived files.
    
    Args:
        file_path: Path to specific file, or None for all.
        
    Returns:
        Dict: Statistics about the archived file(s).
    """
    return get_manager().get_stats(file_path)


def purge(file_path: str, all_versions: bool = False) -> Dict:
    """
    Remove archive versions.
    
    Args:
        file_path: Path to file, with optional version.
        all_versions: If True, remove all versions.
        
    Returns:
        Dict: Information about the purged file(s).
    """
    return get_manager().purge(file_path, all_versions)
