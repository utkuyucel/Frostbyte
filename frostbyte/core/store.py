"""
Database interactions for Frostbyte.

Manages metadata storage for archived files.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple, Any, cast

import duckdb
from frostbyte.utils.json_utils import json_dumps


class MetadataStore:
    """Handles database interactions for Frostbyte metadata."""
    
    def __init__(self, db_path: Union[str, Path]):
        """Initialize the metadata store with database path."""
        self.db_path = Path(db_path)
        
    def initialize(self) -> None:
        """Create the database schema, deleting any existing database."""
        if self.db_path.exists():
            self.db_path.unlink()
            
        self.db_path.parent.mkdir(exist_ok=True)
        
        conn = duckdb.connect(str(self.db_path))
        
        try:
            # Create archives table
            conn.execute("""
            CREATE TABLE archives (
                id VARCHAR PRIMARY KEY,
                original_path VARCHAR NOT NULL,
                version INT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                hash VARCHAR NOT NULL,
                row_count INT,
                schema JSON,
                compression_ratio REAL,
                storage_path VARCHAR NOT NULL,
                original_extension VARCHAR,
                UNIQUE(original_path, version)
            )
            """)
            
            # Create stats table
            conn.execute("""
            CREATE TABLE stats (
                archive_id VARCHAR NOT NULL,
                column_name VARCHAR NOT NULL,
                min DOUBLE,
                max DOUBLE,
                mean DOUBLE,
                stddev DOUBLE,
                PRIMARY KEY (archive_id, column_name),
                FOREIGN KEY (archive_id) REFERENCES archives(id)
            )
            """)
            
            conn.commit()
        finally:
            conn.close()
    
    def add_archive(self, id: str, original_path: str, version: int,
                  timestamp: datetime, hash: str, row_count: int,
                  schema: Dict, compression_ratio: float, storage_path: str,
                  original_extension: Optional[str] = None) -> None:
        """Add a new archive entry to the database with associated metadata."""
        conn = duckdb.connect(str(self.db_path))
        
        try:
            conn.execute("""
            INSERT INTO archives (
                id, original_path, version, timestamp, hash,
                row_count, schema, compression_ratio, storage_path, original_extension
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                id, original_path, version, timestamp, hash,
                row_count, json_dumps(schema), compression_ratio, storage_path, original_extension
            ))
            
            # Add column stats if available
            if schema and 'columns' in schema:
                for col_name, stats in schema['columns'].items():
                    if 'stats' in stats:
                        conn.execute("""
                        INSERT INTO stats (
                            archive_id, column_name, min, max, mean, stddev
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            id, col_name,
                            stats['stats'].get('min'),
                            stats['stats'].get('max'),
                            stats['stats'].get('mean'),
                            stats['stats'].get('stddev')
                        ))
            
            conn.commit()
        finally:
            conn.close()
    
    def get_next_version(self, file_path: str) -> int:
        """Get the next sequential version number for a file."""
        # Normalize the file path to ensure consistent matching
        normalized_path = str(Path(file_path).resolve())
        conn = duckdb.connect(str(self.db_path))

        try:
            result = conn.execute(
                """
                SELECT COALESCE(MAX(version), 0) + 1 AS next_version
                FROM archives
                WHERE original_path = ?
                """,
                (normalized_path,),
            ).fetchone()

            return int(result[0]) if result else 1
        finally:
            conn.close()
    
    def get_archive(self, file_path: str, version: Optional[Union[int, float]] = None) -> Optional[Dict]:
        """Get information about an archived file, optionally by specific version."""
        # Normalize the file path to ensure consistent matching
        normalized_path = str(Path(file_path).resolve())
        conn = duckdb.connect(str(self.db_path))
        
        try:
            get_params: Union[Tuple[str, ...], Tuple[str, int]]
            
            if version is None:
                get_query = """
                SELECT * FROM archives
                WHERE original_path = ?
                ORDER BY version DESC
                LIMIT 1
                """
                get_params = (normalized_path,)
            else:
                get_query = """
                SELECT * FROM archives
                WHERE original_path = ? AND version = ?
                """
                get_params = (normalized_path, int(version))
            
            cursor = conn.execute(get_query, get_params)
            result = cursor.fetchone()
            
            # If no result with exact path, try to find by basename as a fallback
            if not result and "/" in normalized_path:
                # Extract the basename
                basename = Path(normalized_path).name
                
                if version is None:
                    fallback_query = """
                    SELECT * FROM archives
                    WHERE SPLIT_PART(original_path, '/', -1) = ?
                    ORDER BY version DESC
                    LIMIT 1
                    """
                    fallback_params = (basename,)
                else:
                    fallback_query = """
                    SELECT * FROM archives
                    WHERE SPLIT_PART(original_path, '/', -1) = ? AND version = ?
                    """
                    fallback_params = (basename, int(version))
                
                cursor = conn.execute(fallback_query, fallback_params)
                result = cursor.fetchone()
            
            if result and cursor.description is not None:
                columns = [desc[0] for desc in cursor.description]
                return {columns[i]: result[i] for i in range(len(columns))}
            
            return None
        finally:
            conn.close()
    
    def list_archives(self, show_all: bool = False) -> List[Dict]:
        """List archived files, optionally showing all versions."""
        conn = duckdb.connect(str(self.db_path))
        
        try:
            if show_all:
                query = """
                SELECT a.*, 
                       COALESCE(json_extract(a.schema, '$.file_size_bytes') :: INT, 
                                (json_extract(a.schema, '$.row_count') :: INT) * (json_extract(a.schema, '$.avg_row_bytes') :: FLOAT)) AS original_size_bytes,
                       COALESCE(json_extract(a.schema, '$.file_size_bytes') :: INT, 
                                (json_extract(a.schema, '$.row_count') :: INT) * (json_extract(a.schema, '$.avg_row_bytes') :: FLOAT)) * (1 - a.compression_ratio / 100) AS compressed_size_bytes,
                       SPLIT_PART(a.storage_path, '/', -1) AS archive_filename
                FROM archives a
                ORDER BY a.original_path, a.version
                """
                result_cursor = conn.execute(query)
                results = result_cursor.fetchall() if result_cursor else []
            else:
                query = """
                SELECT a.original_path, 
                       MAX(a.version) AS latest_version,
                       COUNT(*) AS version_count,
                       MAX(a.timestamp) AS last_modified,
                       SUM(COALESCE(json_extract(a.schema, '$.file_size_bytes') :: INT, 
                                   (json_extract(a.schema, '$.row_count') :: INT) * (json_extract(a.schema, '$.avg_row_bytes') :: FLOAT))) AS total_size_bytes,
                       SUM(COALESCE(json_extract(a.schema, '$.file_size_bytes') :: INT, 
                                   (json_extract(a.schema, '$.row_count') :: INT) * (json_extract(a.schema, '$.avg_row_bytes') :: FLOAT)) * (1 - a.compression_ratio / 100)) AS total_compressed_bytes,
                       AVG(a.compression_ratio) AS avg_compression
                FROM archives a
                GROUP BY a.original_path
                ORDER BY a.original_path
                """
                result_cursor = conn.execute(query)
                results = result_cursor.fetchall() if result_cursor else []
            
            if not results:
                return []
            
            # Get column names from the result cursor description
            columns = [desc[0] for desc in result_cursor.description] if result_cursor.description else []
            
            # Convert results to dictionaries 
            return [
                {columns[i]: row[i] for i in range(len(columns))}
                for row in results
            ]
        finally:
            conn.close()
    
    def get_stats(self, file_path: Optional[str] = None) -> Dict:
        """Get statistics about archived files, for specific file or all archives."""
        conn = duckdb.connect(str(self.db_path))
        
        try:
            # Convert to absolute path if file_path is provided
            if file_path:
                file_path = str(Path(file_path).resolve())
                # Stats for specific file
                query = """
                SELECT a.original_path,
                       COUNT(*) AS versions,
                       MAX(a.version) AS latest_version,
                       MAX(a.timestamp) AS last_modified,
                       SUM((1 - a.compression_ratio / 100) * COALESCE(json_extract(a.schema, '$.file_size_bytes') :: INT, 
                           a.row_count * (json_extract(a.schema, '$.avg_row_bytes') :: FLOAT))) AS size_saved
                FROM archives a
                WHERE a.original_path = ?
                GROUP BY a.original_path
                """
                result_cursor = conn.execute(query, (file_path,))
                result = result_cursor.fetchone() if result_cursor else None
                
                if result:
                    columns = [desc[0] for desc in conn.description] if conn.description else []
                    return {columns[i]: result[i] for i in range(len(columns))}
                return {}
            else:
                # Overall stats
                query = """
                SELECT COUNT(*) AS total_archives,
                       SUM((1 - a.compression_ratio / 100) * COALESCE(json_extract(a.schema, '$.file_size_bytes') :: INT, 
                           a.row_count * (json_extract(a.schema, '$.avg_row_bytes') :: FLOAT))) AS total_size_saved,
                       AVG(a.compression_ratio) AS avg_compression_ratio
                FROM archives a
                """
                result_cursor = conn.execute(query)
                result = result_cursor.fetchone() if result_cursor else None
                
                if result:
                    columns = [desc[0] for desc in conn.description] if conn.description else []
                    return {columns[i]: result[i] for i in range(len(columns))}
                return {}
        finally:
            conn.close()
    
    def remove_archives(self, file_path: str, version: Optional[int] = None, all_versions: bool = False) -> Dict:
        """
        Remove archive entries from the database.
        
        Args:
            file_path: Path to the file
            version: Version number, or None for latest
            all_versions: If True, remove all versions
            
        Returns:
            Dict: Information about the removed entries
        """
        # We use file basename for LIKE queries below, so no need to normalize here
        conn = duckdb.connect(str(self.db_path))
        
        try:
            # First, get storage paths and archive IDs to delete files later
            select_query: str
            select_params: Union[Tuple[str, ...], Tuple[str, int]]
            
            if all_versions:
                select_query = """
                SELECT id, storage_path
                FROM archives
                WHERE original_path LIKE ?
                """
                select_params = (f"%{Path(file_path).name}%",)
            elif version is not None:
                select_query = """
                SELECT id, storage_path
                FROM archives
                WHERE original_path LIKE ? AND version = ?
                """
                select_params = (f"%{Path(file_path).name}%", version)
            else:
                select_query = """
                SELECT id, storage_path
                FROM archives
                WHERE original_path LIKE ?
                ORDER BY version DESC
                LIMIT 1
                """
                select_params = (f"%{Path(file_path).name}%",)
            
            result = conn.execute(select_query, select_params).fetchall()
            archive_ids = [row[0] for row in result] if result else []
            storage_paths = [row[1] for row in result] if result else []
            
            # First delete from the stats table to avoid foreign key constraint violations
            for archive_id in archive_ids:
                stats_delete_query = "DELETE FROM stats WHERE archive_id = ?"
                conn.execute(stats_delete_query, (archive_id,))
            
            conn.commit()  # Commit the stats deletion
            
            # Now delete from the archives table
            if all_versions:
                delete_query = """
                DELETE FROM archives
                WHERE original_path LIKE ?
                """
                delete_params = (f"%{Path(file_path).name}%",)
            elif version is not None:
                delete_query = """
                DELETE FROM archives
                WHERE original_path LIKE ? AND version = ?
                """
                delete_params = (f"%{Path(file_path).name}%", version)
            else:
                delete_query = """
                DELETE FROM archives
                WHERE id IN (
                    SELECT id FROM archives
                    WHERE original_path LIKE ?
                    ORDER BY version DESC
                    LIMIT 1
                )
                """
                delete_params = (f"%{Path(file_path).name}%",)
            
            # Execute the deletion query
            conn.execute(delete_query, delete_params)
            conn.commit()  # Make sure to commit the deletion
            
            # For DuckDB we don't have a direct changes() function like in SQLite
            # Just count the number of paths we collected
            count = len(storage_paths)
            
            # Return the result with storage paths for physical file deletion
            return {
                "storage_paths": storage_paths,
                "count": count
            }
            
            conn.commit()
            
            return {
                'count': count,
                'storage_paths': storage_paths
            }
        finally:
            conn.close()
    
    def find_archives_by_name(self, name_part: str) -> List[Dict]:
        """Find archives by part of the original file name or archive filename."""
        conn = duckdb.connect(str(self.db_path))
        
        try:
            # First try to find by exact basename match
            basename = Path(name_part).name  # Extract basename if path is provided
            query = """
            SELECT original_path, MAX(version) as latest_version
            FROM archives
            WHERE SPLIT_PART(original_path, '/', -1) = ?
            GROUP BY original_path
            ORDER BY original_path
            """
            
            get_params = (basename,)
            cursor = conn.execute(query, get_params)
            results = cursor.fetchall() if cursor else []
            
            # If no exact matches, try to find by partial path match
            if not results:
                query = """
                SELECT original_path, MAX(version) as latest_version
                FROM archives
                WHERE CONTAINS(original_path, ?)
                GROUP BY original_path
                ORDER BY original_path
                """
                
                get_params = (name_part,)
                cursor = conn.execute(query, get_params)
                results = cursor.fetchall() if cursor else []
            
            # If still no results, try to find by archive filename
            if not results:
                query = """
                SELECT original_path, version as latest_version, storage_path
                FROM archives
                WHERE CONTAINS(SPLIT_PART(storage_path, '/', -1), ?)
                ORDER BY original_path
                """
                cursor = conn.execute(query, get_params)
                results = cursor.fetchall() if cursor else []
            
            if not results:
                return []
            
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            return [
                {columns[i]: row[i] for i in range(len(columns))}
                for row in results
            ]
        finally:
            conn.close()
