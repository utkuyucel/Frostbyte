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
        """Initialize the archive manager with optional config path."""
        self.base_dir = Path(os.getcwd())
        self.frostbyte_dir = self.base_dir / '.frostbyte'
        self.archives_dir = self.frostbyte_dir / 'archives'
        
        self.store = MetadataStore(self.frostbyte_dir / 'manifest.db')
        self.compressor = Compressor()
        
    def initialize(self) -> bool:
        """Initialize a new Frostbyte repository in the current directory."""
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
        """Archive a file and return information about the archived file."""
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
        archive_name = f"{file_path_obj.stem}_v{version}.parquet"
        archive_path = self.archives_dir / archive_name
        
        # Compress the file
        _, compressed_size = self.compressor.compress(file_path, archive_path)
        compression_ratio = 100 * (1 - (compressed_size / original_size)) if original_size > 0 else 0
        
        # Record metadata
        archive_id = str(uuid.uuid4())
        original_extension = file_path_obj.suffix
        self.store.add_archive(
            id=archive_id,
            original_path=file_path_str,
            version=version,
            timestamp=datetime.now(),
            hash=file_hash,
            row_count=row_count,
            schema=schema,
            compression_ratio=compression_ratio,
            storage_path=str(archive_path),
            original_extension=original_extension
        )
        
        return {
            'archive_id': archive_id,
            'original_path': str(file_path),
            'version': version,
            'archive_name': archive_name,
            'original_size': original_size,
            'compressed_size': compressed_size,
            'compression_ratio': compression_ratio
        }
    
    def restore(self, path_spec: str) -> Dict:
        """Restore an archived file using path, version, archive filename, or partial name."""
        # Check for archive filename pattern (_v#.parquet)
        if "_v" in path_spec and path_spec.endswith(".parquet"):
            # Extract version from the archive filename if present
            try:
                # Parse potential archive filename (e.g., 'customer_data_v1.parquet')
                matches = self.find_by_name(path_spec)
                if matches:
                    if len(matches) == 1:
                        # For archive filenames, we already have the version in the match
                        archive_info = self.store.get_archive(
                            matches[0]['original_path'], 
                            matches[0]['latest_version']
                        )
                    else:
                        # Multiple matching archive filenames
                        match_paths = [f"{m['original_path']} (v{m['latest_version']})" for m in matches]
                        matches_str = '\n  '.join(match_paths)
                        raise ValueError(f"Multiple archives match '{path_spec}':\n  {matches_str}\nPlease be more specific.")
                else:
                    raise ValueError(f"No archives found matching: {path_spec}")
            except ValueError:
                # If parsing fails, continue to regular path handling
                archive_info = None
        # Parse path with @version
        elif '@' in path_spec:
            path, version_str = path_spec.split('@', 1)
            version = self._parse_version(version_str)
            
            # Get archive info by exact path and version
            archive_info = self.store.get_archive(path, version)
            if not archive_info:
                raise ValueError(f"Archive not found: {path_spec}")
        else:
            # First try as exact path with latest version
            archive_info = self.store.get_archive(path_spec)
            
            # If not found, try as a partial name search
            if not archive_info:
                matches = self.find_by_name(path_spec)
                if not matches:
                    raise ValueError(f"No archives found matching: {path_spec}")
                
                if len(matches) > 1:
                    # If multiple matches, show options to the user
                    match_paths = [f"{m['original_path']} (v{m['latest_version']})" for m in matches]
                    matches_str = '\n  '.join(match_paths)
                    raise ValueError(f"Multiple archives match '{path_spec}':\n  {matches_str}\nPlease be more specific or use the full path.")
                
                # Get the archive info for the only match
                archive_info = self.store.get_archive(matches[0]['original_path'], matches[0]['latest_version'])
        
        if not archive_info:
            raise ValueError(f"Could not locate archive: {path_spec}")
            
        # Decompress file
        storage_path = Path(archive_info['storage_path'])
        original_path = Path(archive_info['original_path'])
        original_extension = archive_info.get('original_extension')

        if not original_extension:
            # Fallback or raise error if original_extension is missing
            # For now, let's assume it might be an older archive and try to infer or default
            # A more robust solution would be to ensure all archives have this
            # or handle the case explicitly (e.g. by asking user or defaulting to .csv)
            print(f"Warning: Original extension not found for {storage_path}. Attempting to restore as Parquet's original format if possible, or to a default.")
            # Defaulting to .csv if not found, or could raise error
            original_extension = original_path.suffix or '.csv'


        self.compressor.decompress(storage_path, original_path, original_extension)
        
        # Calculate or extract file sizes
        schema = archive_info.get('schema', {})
        if isinstance(schema, str):
            import json
            schema = json.loads(schema)
            
        # Get the compression ratio
        compression_ratio = archive_info.get('compression_ratio', 0)
        
        # Calculate original file size - either from schema or from the actual file
        original_size = 0
        if schema:
            # Try to get size from schema
            if 'file_size_bytes' in schema:
                original_size = schema.get('file_size_bytes', 0)
            elif 'row_count' in schema and 'avg_row_bytes' in schema:
                original_size = schema['row_count'] * schema['avg_row_bytes']
        
        # If we couldn't get the original size from schema, try getting it from disk
        if not original_size:
            # Get the storage path and try to calculate size from it
            storage_path = Path(archive_info['storage_path'])
            if storage_path.exists():
                storage_size = storage_path.stat().st_size
                # If we have compression ratio, estimate original size
                if compression_ratio:
                    if compression_ratio != 100:  # avoid division by zero
                        original_size = storage_size / (1 - compression_ratio / 100)
                    else:
                        original_size = storage_size  # placeholder for 100% compression
                else:
                    original_size = storage_size
        
        # Calculate compressed size
        compressed_size = 0
        if original_size:
            if compression_ratio >= 0:
                # Normal case - positive compression ratio
                compressed_size = original_size * (1 - compression_ratio / 100)
            else:
                # Negative compression ratio case (file got bigger)
                compressed_size = original_size * (1 + abs(compression_ratio) / 100)
        
        return {
            'original_path': str(original_path),
            'version': archive_info['version'],
            'timestamp': archive_info['timestamp'],
            'original_size': original_size,
            'compressed_size': compressed_size,
            'compression_ratio': compression_ratio
        }
    
    def list_archives(self, show_all: bool = False) -> List[Dict]:
        """List archived files, optionally showing all versions."""
        return self.store.list_archives(show_all)
    
    def get_stats(self, file_path: Optional[str] = None) -> Dict:
        """Get statistics about archived files, for specific file or all archives."""
        return self.store.get_stats(file_path)
    
    def purge(self, file_path: str, all_versions: bool = False) -> Dict:
        """Remove specific archive versions or all versions of a file."""
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
        """Parse a version string into a numeric version."""
        try:
            if '.' in version_str:
                return float(version_str)
            return int(version_str)
        except ValueError:
            raise ValueError(f"Invalid version format: {version_str}")
    
    def find_by_name(self, name_part: str) -> List[Dict]:
        """Find archives by partial filename match."""
        return self.store.find_archives_by_name(name_part)
