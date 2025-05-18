"""
Database interactions for Frostbyte.

Manages metadata storage for archived files.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

import duckdb

from frostbyte.utils.json_utils import json_dumps

class MetadataStore:
    def __init__(self, db_path: Union[str, Path]):
        self.db_path = Path(db_path)
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        if str(self.db_path) == ":memory:":
            # For in-memory databases, keep one persistent connection
            self._conn = duckdb.connect() # type: ignore

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if self._conn:
            return self._conn
        return duckdb.connect(database=str(self.db_path), read_only=False)

    def initialize(self) -> None:
        # Remove existing file if on disk
        if self._conn is None and self.db_path.exists() and self.db_path.is_file(): # mypy check for self.db_path.is_file()
            self.db_path.unlink()
        # Ensure parent dir exists
        if self._conn is None:
            self.db_path.parent.mkdir(exist_ok=True)

        conn = self._connect()
        try:
            # Create archives table
            conn.execute(
                """
            CREATE TABLE IF NOT EXISTS archives (
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
            """
            )
            # Create stats table
            conn.execute(
                """
            CREATE TABLE IF NOT EXISTS stats (
                archive_id VARCHAR NOT NULL,
                column_name VARCHAR NOT NULL,
                min DOUBLE,
                max DOUBLE,
                mean DOUBLE,
                stddev DOUBLE,
                PRIMARY KEY (archive_id, column_name),
                FOREIGN KEY (archive_id) REFERENCES archives(id)
            )
            """
            )
            conn.commit()
        finally:
            if self._conn is None:
                conn.close()

    def add_archive(
        self,
        id: str,
        original_path: str,
        version: int,
        timestamp: datetime,
        hash: str,
        row_count: int,
        schema: Dict,
        compression_ratio: float,
        storage_path: str,
        original_extension: Optional[str] = None,
    ) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
            INSERT INTO archives (
                id, original_path, version, timestamp, hash,
                row_count, schema, compression_ratio, storage_path, original_extension
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    id,
                    original_path,
                    version,
                    timestamp,
                    hash,
                    row_count,
                    json_dumps(schema),
                    compression_ratio,
                    storage_path,
                    original_extension,
                ),
            )
            if schema and "columns" in schema:
                for col_name, stats_data in schema["columns"].items(): # Renamed stats to stats_data
                    if "stats" in stats_data:
                        conn.execute(
                            """
                        INSERT INTO stats (
                            archive_id, column_name, min, max, mean, stddev
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                            (
                                id,
                                col_name,
                                stats_data["stats"].get("min"),
                                stats_data["stats"].get("max"),
                                stats_data["stats"].get("mean"),
                                stats_data["stats"].get("stddev"),
                            ),
                        )
            conn.commit()
        finally:
            if self._conn is None:
                conn.close()

    def get_next_version(self, file_path: str) -> int:
        normalized_path = str(Path(file_path).resolve())
        conn = self._connect()
        try:
            result_tuple = conn.execute(
                """
                SELECT COALESCE(MAX(version), 0) + 1 AS next_version
                FROM archives
                WHERE original_path = ?
                """,
                (normalized_path,),
            ).fetchone()
            return int(result_tuple[0]) if result_tuple else 1
        finally:
            if self._conn is None:
                conn.close()

    def get_archive(
        self, file_path: str, version: Optional[Union[int, float]] = None
    ) -> Optional[Dict]:
        normalized_path = str(Path(file_path).resolve())
        conn = self._connect()
        try:
            query: str
            params_typed: Union[tuple[str], tuple[str, int]]
            if version is None:
                query = """
                SELECT * FROM archives
                WHERE original_path = ?
                ORDER BY version DESC
                LIMIT 1
                """
                params_typed = (normalized_path,)
            else:
                query = """
                SELECT * FROM archives
                WHERE original_path = ? AND version = ?
                """
                params_typed = (normalized_path, int(version))
            cursor = conn.execute(query, params_typed)
            result_tuple = cursor.fetchone()
            if not result_tuple and "/" in normalized_path: # Check if path contains directory
                basename = Path(normalized_path).name
                fallback_query: str
                fb_params_typed: Union[tuple[str], tuple[str, int]]
                if version is None:
                    fallback_query = """
                    SELECT * FROM archives
                    WHERE SPLIT_PART(original_path, '/', -1) = ?
                    ORDER BY version DESC
                    LIMIT 1
                    """
                    fb_params_typed = (basename,)
                else:
                    fallback_query = """
                    SELECT * FROM archives
                    WHERE SPLIT_PART(original_path, '/', -1) = ? AND version = ?
                    """
                    fb_params_typed = (basename, int(version))
                cursor = conn.execute(fallback_query, fb_params_typed)
                result_tuple = cursor.fetchone()

            if result_tuple and cursor.description:
                cols = [d[0] for d in cursor.description]
                return {cols[i]: result_tuple[i] for i in range(len(cols))}
            return None
        finally:
            if self._conn is None:
                conn.close()

    def list_archives(self, file_name: Optional[str] = None) -> List[Dict]:
        conn = self._connect()
        try:
            query: str
            params: Optional[tuple[str]] = None
            if file_name: # Detailed view for a specific file
                # Resolve the file_name to its absolute path if it exists, otherwise use as is for partial matching
                path_obj = Path(file_name)
                if path_obj.exists():
                    resolved_file_name = str(path_obj.resolve())
                else: # Allow partial name matching if full path doesn't exist
                    resolved_file_name = file_name

                query = """
                SELECT 
                    a.*,
                    COALESCE(
                        json_extract(a.schema, '$.file_size_bytes') :: INT,
                        (json_extract(a.schema, '$.row_count') :: INT) * 
                        (json_extract(a.schema, '$.avg_row_bytes') :: FLOAT)
                    ) AS original_size_bytes,
                    COALESCE(
                        json_extract(a.schema, '$.file_size_bytes') :: INT,
                        (json_extract(a.schema, '$.row_count') :: INT) * 
                        (json_extract(a.schema, '$.avg_row_bytes') :: FLOAT)
                    ) * (1 - a.compression_ratio / 100) AS compressed_size_bytes,
                    SPLIT_PART(a.storage_path, '/', -1) AS archive_filename,
                    a.row_count
                FROM archives a
                WHERE a.original_path LIKE ? OR SPLIT_PART(a.original_path, '/', -1) LIKE ?
                ORDER BY a.original_path, a.version
                """
                like_pattern = f"%{Path(resolved_file_name).name}%" # Match basename for flexibility
                params = (like_pattern, like_pattern)
                cursor = conn.execute(query, params)
            else: # Summary view for all files
                query = """
                SELECT 
                    a.original_path,
                    MAX(a.version) AS latest_version,
                    SUM(a.row_count) AS total_row_count, -- Sum of row_count for all versions of this path
                    COUNT(*) AS version_count,
                    MAX(a.timestamp) AS last_modified,
                    SUM(
                        COALESCE(
                            json_extract(a.schema, '$.file_size_bytes') :: INT,
                            (json_extract(a.schema, '$.row_count') :: INT) * 
                            (json_extract(a.schema, '$.avg_row_bytes') :: FLOAT)
                        )
                    ) AS total_size_bytes,
                    SUM(
                        COALESCE(
                            json_extract(a.schema, '$.file_size_bytes') :: INT,
                            (json_extract(a.schema, '$.row_count') :: INT) * 
                            (json_extract(a.schema, '$.avg_row_bytes') :: FLOAT)
                        ) * (1 - a.compression_ratio / 100)
                    ) AS total_compressed_bytes,
                    AVG(a.compression_ratio) AS avg_compression
                FROM archives a
                GROUP BY a.original_path
                ORDER BY a.original_path
                """
                cursor = conn.execute(query)

            rows = cursor.fetchall() # Fetch all rows
            if not rows or not cursor.description: # Check if rows or description is empty
                return []
            # Ensure cols is not None before list comprehension
            cols = [d[0] for d in cursor.description if d[0] is not None]
            return [{cols[i]: row[i] for i in range(len(cols))} for row in rows]
        finally:
            if self._conn is None: # Close connection if not persistent
                conn.close()

    def get_stats(self, file_path: Optional[str] = None) -> Dict:
        """Get statistics about archived files, for specific file or all archives."""
        conn = self._connect()
        try:
            query: str
            params: Optional[tuple[str]] = None
            if file_path:
                path = str(Path(file_path).resolve())
                query = """
                SELECT 
                    a.original_path,
                    COUNT(*) AS versions,
                    MAX(a.version) AS latest_version,
                    MAX(a.timestamp) AS last_modified,
                    SUM(
                        (1 - a.compression_ratio / 100) * COALESCE(
                            json_extract(a.schema, '$.file_size_bytes') :: INT, 
                            a.row_count * (json_extract(a.schema, '$.avg_row_bytes') :: FLOAT)
                        )
                    ) AS size_saved
                FROM archives a
                WHERE a.original_path = ?
                GROUP BY a.original_path
                """
                params = (path,)
                cursor = conn.execute(query, params)
            else:
                query = """
                SELECT 
                    COUNT(*) AS total_archives,
                    SUM(
                        (1 - a.compression_ratio / 100) * COALESCE(
                            json_extract(a.schema, '$.file_size_bytes') :: INT,
                            a.row_count * (json_extract(a.schema, '$.avg_row_bytes') :: FLOAT)
                        )
                    ) AS total_size_saved,
                    AVG(a.compression_ratio) AS avg_compression_ratio
                FROM archives a
                """
                cursor = conn.execute(query)

            row = cursor.fetchone() if cursor else None
            if row and cursor.description: # Added check for cursor.description
                cols = [d[0] for d in cursor.description if d[0] is not None]
                return dict(zip(cols, row))
            return {}
        finally:
            if self._conn is None:
                conn.close()

    def remove_archives(
        self, file_path: str, version: Optional[int] = None, all_versions: bool = False
    ) -> Dict:
        """
        Remove archive entries from the database.

        Args:
            file_path: Path to the file
            version: Version number, or None for latest
            all_versions: If True, remove all versions

        Returns:
            Dict: Information about the removed entries
        """
        conn = self._connect()
        try:
            sel_query_str: str
            params_typed: Union[tuple[str], tuple[str, int]]

            if version is not None: # Check for specific version first
                sel_query_str = (
                    "SELECT id, storage_path FROM archives "
                    "WHERE original_path LIKE ? AND version = ?"
                )
                params_typed = (f"%{Path(file_path).name}%", version)
            elif all_versions:
                sel_query_str = "SELECT id, storage_path FROM archives WHERE original_path LIKE ?"
                params_typed = (f"%{Path(file_path).name}%",)
            else: # Latest version
                sel_query_str = """
                SELECT id, storage_path FROM archives
                WHERE original_path LIKE ?
                ORDER BY version DESC
                LIMIT 1
                """
                params_typed = (f"%{Path(file_path).name}%",)

            rows_tuples = conn.execute(sel_query_str, params_typed).fetchall()
            ids = [r[0] for r in rows_tuples if r] if rows_tuples else []
            paths = [r[1] for r in rows_tuples if r] if rows_tuples else []

            # Delete stats
            if ids:
                conn.execute(f"DELETE FROM stats WHERE archive_id IN ({','.join(['?']*len(ids))})", (*ids,))
                conn.commit()

            # Delete archives
            delq_str: str
            delp_params: Union[tuple[str], tuple[str, int]]

            if version is not None: # Check for specific version first
                delq_str = "DELETE FROM archives WHERE original_path LIKE ? AND version = ?"
                delp_params = (f"%{Path(file_path).name}%", version)
            elif all_versions:
                delq_str = "DELETE FROM archives WHERE original_path LIKE ?"
                delp_params = (f"%{Path(file_path).name}%",)
            else: # Latest version
                if ids: # Should be a single ID if not all_versions and version is None
                    delq_str = "DELETE FROM archives WHERE id = ?"
                    delp_params = (ids[0],) 
                else: # No archive found to delete
                    return {"storage_paths": [], "count": 0}

            if ids: # Only execute if there are archives to delete
                conn.execute(delq_str, delp_params)
                conn.commit()

            return {"storage_paths": paths, "count": len(paths)}
        finally:
            if self._conn is None:
                conn.close()

    def find_archives_by_name(self, name_part: str) -> List[Dict]:
        """Find archives by part of the original file name or archive filename."""
        conn = self._connect()
        try:
            basename = Path(name_part).name
            query = """
            SELECT original_path, MAX(version) as latest_version
            FROM archives
            WHERE SPLIT_PART(original_path, '/', -1) = ?
            GROUP BY original_path
            ORDER BY original_path
            """
            cursor = conn.execute(query, (basename,)) # Execute query with basename
            results_tuples = cursor.fetchall() # Fetch all results
            if not results_tuples: # If no results, try fallback 1
                query_fallback1 = """
                SELECT original_path, MAX(version) as latest_version
                FROM archives
                WHERE CONTAINS(original_path, ?)
                GROUP BY original_path
                ORDER BY original_path
                """
                cursor = conn.execute(query_fallback1, (name_part,))
                results_tuples = cursor.fetchall()
            if not results_tuples: # If no results, try fallback 2
                query_fallback2 = """
                SELECT original_path, version as latest_version, storage_path
                FROM archives
                WHERE CONTAINS(SPLIT_PART(storage_path, '/', -1), ?)
                ORDER BY original_path
                """
                cursor = conn.execute(query_fallback2, (name_part,))
                results_tuples = cursor.fetchall()

            if not results_tuples or not cursor.description: # Check if results or description is empty
                return []
            # Ensure cols is not None before list comprehension
            cols = [d[0] for d in cursor.description if d[0] is not None]
            return [{cols[i]: row[i] for i in range(len(cols))} for row in results_tuples]
        finally:
            if self._conn is None: # Close connection if not persistent
                conn.close()
