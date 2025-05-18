"""
Frostbyte: Cold Data Archiving for Pandas Workflows

A lightweight, local-first tool for efficient compression, versioning, and management of data.
"""

__version__ = "0.1.0"

from typing import Callable, Dict, List, Optional

from frostbyte.core.manager import ArchiveManager


class _ManagerProvider:
    _instance: Optional[ArchiveManager] = None

    @classmethod
    def get(cls) -> ArchiveManager:
        if cls._instance is None:
            cls._instance = ArchiveManager()
        return cls._instance


def get_manager() -> ArchiveManager:
    return _ManagerProvider.get()


def init() -> bool:
    return get_manager().initialize()


def archive(
    file_path: str, quiet: bool = False, progress_callback: Optional[Callable[[float], None]] = None
) -> Dict:
    return get_manager().archive(file_path, quiet=quiet, progress_callback=progress_callback)


def restore(
    path_spec: str,
    version: Optional[int] = None,
    progress_callback: Optional[Callable[[float], None]] = None,
) -> Dict:
    return get_manager().restore(path_spec, version, progress_callback)


def ls(file_name: Optional[str] = None) -> List[Dict]:  # Ensure this line is correct
    return get_manager().list_archives(file_name=file_name)  # Pass file_name correctly


def stats(file_path: Optional[str] = None) -> Dict:
    return get_manager().get_stats(file_path)


def purge(file_path: str, version: Optional[int] = None, all_versions: bool = False) -> Dict:
    return get_manager().purge(file_path, version, all_versions)


def find_by_name(name_part: str) -> List[Dict]:
    return get_manager().find_by_name(name_part)
