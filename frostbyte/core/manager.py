"""
Core archive manager for Frostbyte.

Orchestrates the archiving, restoring, and management of data files.
"""

import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

from frostbyte.core.compressor import Compressor
from frostbyte.core.store import MetadataStore
from frostbyte.utils.file_utils import get_file_hash, get_file_size
from frostbyte.utils.schema import extract_schema


class ArchiveManager:
    """Manages archiving, restoring, and queries for Frostbyte."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the archive manager.
        
        Args:
            config_path: Path to configuration file. If None, uses default.
        """
        self.base_dir = Path(os.getcwd())
        self.frostbyte_dir = self.base_dir / '.frostbyte'
        self.archives_dir = self.frostbyte_dir / 'archives'
        
        self.store = MetadataStore(self.frostbyte_dir / 'manifest.db')
        self.compressor = Compressor()
        
    def initialize(self) -> bool:
        """
        Initialize a new Frostbyte repository in the current directory.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Create directories if they don't exist
            self.frostbyte_dir.mkdir(exist_ok=True)
            self.archives_dir.mkdir(exist_ok=True)
            
            # Initialize the metadata store
            self.store.initialize()
            
            return True
        except Exception as e:
            print(f"Error initializing Frostbyte: {e}")
            return False
    
    def archive(self, file_path: str) -> Dict:
        """
        Archive a file.
        
        Args:
            file_path: Path to the file to archive.
            
        Returns:
            Dict: Information about the archived file.
        """
        file_path_obj = Path(file_path).resolve()
        file_path_str = str(file_path_obj)
        
        # Get file information
        file_hash = get_file_hash(file_path_obj)
        original_size = get_file_size(file_path_obj)
        
        # Extract schema and statistics
        schema = extract_schema(file_path_obj)
        row_count = schema.get('row_count', 0)
        
        # Get the next version number
        version = self.store.get_next_version(file_path_str)
        
        # Create archive name
        archive_name = f"{file_path_obj.stem}_v{version}{file_path_obj.suffix}.fbyt"
        archive_path = self.archives_dir / archive_name
        
        # Compress the file
        compressed_size = self.compressor.compress(file_path, archive_path)
        compression_ratio = 100 * (1 - (compressed_size / original_size)) if original_size > 0 else 0
        
        # Record metadata
        archive_id = str(uuid.uuid4())
        self.store.add_archive(
            id=archive_id,
            original_path=file_path_str,
            version=version,
            timestamp=datetime.now(),
            hash=file_hash,
            row_count=row_count,
            schema=schema,
            compression_ratio=compression_ratio,
            storage_path=str(archive_path)
        )
        
        return {
            'archive_id': archive_id,
            'original_path': str(file_path),
            'version': version,
            'archive_name': archive_name,
            'compression_ratio': compression_ratio
        }
    
    def restore(self, path_spec: str) -> Dict:
        """
        Restore an archived file to its original location.
        
        Args:
            path_spec: Path with optional version (e.g., 'data/file.csv@2')
            
        Returns:
            Dict: Information about the restored file.
        """
        # Parse path and version
        if '@' in path_spec:
            path, version_str = path_spec.split('@', 1)
            version = self._parse_version(version_str)
        else:
            path = path_spec
            version = None  # Use latest
        
        # Get archive info
        archive_info = self.store.get_archive(path, version)
        if not archive_info:
            raise ValueError(f"Archive not found: {path_spec}")
        
        # Decompress file
        storage_path = Path(archive_info['storage_path'])
        original_path = Path(archive_info['original_path'])
        
        self.compressor.decompress(storage_path, original_path)
        
        return {
            'original_path': str(original_path),
            'version': archive_info['version'],
            'timestamp': archive_info['timestamp']
        }
    
    def list_archives(self, show_all: bool = False) -> List[Dict]:
        """
        List archived files.
        
        Args:
            show_all: If True, show all versions; otherwise, show latest only.
            
        Returns:
            List[Dict]: Archive information.
        """
        return self.store.list_archives(show_all)
    
    def get_stats(self, file_path: Optional[str] = None) -> Dict:
        """
        Get statistics about archived files.
        
        Args:
            file_path: Path to specific file, or None for all.
            
        Returns:
            Dict: Statistics about the archived file(s).
        """
        return self.store.get_stats(file_path)
    
    def purge(self, file_path: str, all_versions: bool = False) -> Dict:
        """
        Remove archive versions.
        
        Args:
            file_path: Path to file, with optional version.
            all_versions: If True, remove all versions.
            
        Returns:
            Dict: Information about the purged file(s).
        """
        # Parse path and version
        if '@' in file_path and not all_versions:
            path, version_str = file_path.split('@', 1)
            version = self._parse_version(version_str)
        else:
            path = file_path
            version = None  # Use latest if not all_versions
            
        result = self.store.remove_archives(path, int(version) if version is not None else None, all_versions)
        
        # Remove physical files
        for archive_path in result.get('storage_paths', []):
            try:
                Path(archive_path).unlink(missing_ok=True)
            except Exception:
                pass
        
        return {
            'original_path': path,
            'version': version,
            'count': result.get('count', 0)
        }
    
    def _parse_version(self, version_str: str) -> Union[int, float]:
        """
        Parse a version string into a numeric version.
        
        Args:
            version_str: The version string (e.g., '1', '1.2')
            
        Returns:
            Union[int, float]: The numeric version.
        """
        try:
            if '.' in version_str:
                return float(version_str)
            return int(version_str)
        except ValueError:
            raise ValueError(f"Invalid version format: {version_str}")
