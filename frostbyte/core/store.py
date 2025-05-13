"""
Dafrom datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple, Any, cast

import duckdb
from frostbyte.utils.json_utils import json_dumps interactions for Frostbyte.

Manages metadata storage for archived files.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple, Any

import duckdb
from frostbyte.utils.json_utils import json_dumps


class MetadataStore:
    """Handles database interactions for Frostbyte metadata."""
    
    def __init__(self, db_path: Union[str, Path]):
        """
        Initialize the metadata store.
        
        Args:
            db_path: Path to the metadata database
        """
        self.db_path = Path(db_path)
        
    def initialize(self) -> None:
        """Create the database schema, deleting any existing database."""
        # Delete existing database file if it exists
        if self.db_path.exists():
            self.db_path.unlink()
            
        # Make sure the parent directory exists
        self.db_path.parent.mkdir(exist_ok=True)
        
        # Connect to DuckDB
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
                  schema: Dict, compression_ratio: float, storage_path: str) -> None:
        """
        Add a new archive entry to the database.
        
        Args:
            id: UUID for the archive
            original_path: Path to the original file
            version: Version number
            timestamp: When the archive was created
            hash: Hash of the original file
            row_count: Number of rows in the file
            schema: Schema of the file as a JSON object
            compression_ratio: Compression ratio (percentage saved)
            storage_path: Path to the compressed archive
        """
        conn = duckdb.connect(str(self.db_path))
        
        try:
            conn.execute("""
            INSERT INTO archives (
                id, original_path, version, timestamp, hash,
                row_count, schema, compression_ratio, storage_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                id, original_path, version, timestamp, hash,
                row_count, json_dumps(schema), compression_ratio, storage_path
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
        """
        Get the next version number for a file.

        Args:
            file_path: Path to the file

        Returns:
            int: Next version number
        """
        conn = duckdb.connect(str(self.db_path))

        try:
            result = conn.execute(
                """
                SELECT COALESCE(MAX(version), 0) + 1 AS next_version
                FROM archives
                WHERE original_path = ?
                """,
                (file_path,),
            ).fetchone()

            return int(result[0]) if result else 1
        finally:
            conn.close()
    
    def get_archive(self, file_path: str, version: Optional[Union[int, float]] = None) -> Optional[Dict]:
        """
        Get information about an archived file.
        
        Args:
            file_path: Path to the file
            version: Version number, or None for latest
            
        Returns:
            Optional[Dict]: Archive information, or None if not found
        """
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
                get_params = (file_path,)
            else:
                get_query = """
                SELECT * FROM archives
                WHERE original_path = ? AND version = ?
                """
                get_params = (file_path, int(version))
            
            cursor = conn.execute(get_query, get_params)
            result = cursor.fetchone()
            
            if result and cursor.description is not None:
                columns = [desc[0] for desc in cursor.description]
                return {columns[i]: result[i] for i in range(len(columns))}
            
            return None
        finally:
            conn.close()
    
    def list_archives(self, show_all: bool = False) -> List[Dict]:
        """
        List archived files.
        
        Args:
            show_all: If True, show all versions; otherwise, show latest only
            
        Returns:
            List[Dict]: Archive information
        """
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
            
            columns = [desc[0] for desc in conn.description] if conn.description else []
            return [
                {columns[i]: row[i] for i in range(len(columns))}
                for row in results
            ]
        finally:
            conn.close()
    
    def get_stats(self, file_path: Optional[str] = None) -> Dict:
        """
        Get statistics about archived files.
        
        Args:
            file_path: Path to specific file, or None for all
            
        Returns:
            Dict: Statistics about the archived file(s)
        """
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
        conn = duckdb.connect(str(self.db_path))
        
        try:
            # First, get storage paths to delete files later
            select_query: str
            select_params: Union[Tuple[str, ...], Tuple[str, int]]
            
            if all_versions:
                select_query = """
                SELECT storage_path
                FROM archives
                WHERE original_path = ?
                """
                select_params = (file_path,)
            elif version is not None:
                select_query = """
                SELECT storage_path
                FROM archives
                WHERE original_path = ? AND version = ?
                """
                select_params = (file_path, version)
            else:
                select_query = """
                SELECT storage_path
                FROM archives
                WHERE original_path = ?
                ORDER BY version DESC
                LIMIT 1
                """
                select_params = (file_path,)
            
            result = conn.execute(select_query, select_params).fetchall()
            storage_paths = [row[0] for row in result] if result else []
            
            # Now delete from the database
            delete_query: str
            delete_params: Union[Tuple[str, ...], Tuple[str, int]]
            
            if all_versions:
                delete_query = """
                DELETE FROM archives
                WHERE original_path = ?
                """
                delete_params = (file_path,)
            elif version is not None:
                delete_query = """
                DELETE FROM archives
                WHERE original_path = ? AND version = ?
                """
                delete_params = (file_path, version)
            else:
                delete_query = """
                DELETE FROM archives
                WHERE id IN (
                    SELECT id FROM archives
                    WHERE original_path = ?
                    ORDER BY version DESC
                    LIMIT 1
                )
                """
                delete_params = (file_path,)
            
            # Execute the deletion query
            conn.execute(delete_query, delete_params)
            
            # Get the affected row count
            count_query = "SELECT changes()"
            count_result = conn.execute(count_query).fetchone()
            count = int(count_result[0]) if count_result else 0
            
            conn.commit()
            
            return {
                'count': count,
                'storage_paths': storage_paths
            }
        finally:
            conn.close()
