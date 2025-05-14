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
    """Handles database interactions for Frostbyte metadata."""

    def __init__(self, db_path: Union[str, Path]):
        """Initialize the metadata store with database path."""
        self.db_path = Path(db_path)
        # For in-memory databases, keep one persistent connection
        if str(self.db_path) == ":memory:":
            self._conn = duckdb.connect()
        else:
            self._conn = None

    def _connect(self) -> duckdb.DuckDBPyConnection:
        """Return a connection: persistent for memory, new for file."""
        if self._conn:
            return self._conn
        return duckdb.connect(str(self.db_path))

    def initialize(self) -> None:
        """Create the database schema, deleting any existing database."""
        # Remove existing file if on disk
        if self._conn is None and self.db_path.exists():
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
        """Add a new archive entry to the database with associated metadata."""
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
                for col_name, stats in schema["columns"].items():
                    if "stats" in stats:
                        conn.execute(
                            """
                        INSERT INTO stats (
                            archive_id, column_name, min, max, mean, stddev
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                            (
                                id,
                                col_name,
                                stats["stats"].get("min"),
                                stats["stats"].get("max"),
                                stats["stats"].get("mean"),
                                stats["stats"].get("stddev"),
                            ),
                        )
            conn.commit()
        finally:
            if self._conn is None:
                conn.close()

    def get_next_version(self, file_path: str) -> int:
        """Get the next sequential version number for a file."""
        normalized_path = str(Path(file_path).resolve())
        conn = self._connect()
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
            if self._conn is None:
                conn.close()

    def get_archive(
        self, file_path: str, version: Optional[Union[int, float]] = None
    ) -> Optional[Dict]:
        """Get information about an archived file, optionally by specific version."""
        normalized_path = str(Path(file_path).resolve())
        conn = self._connect()
        try:
            if version is None:
                query = """
                SELECT * FROM archives
                WHERE original_path = ?
                ORDER BY version DESC
                LIMIT 1
                """
                params = (normalized_path,)
            else:
                query = """
                SELECT * FROM archives
                WHERE original_path = ? AND version = ?
                """
                params = (normalized_path, int(version))
            cursor = conn.execute(query, params)
            result = cursor.fetchone()
            if not result and "/" in normalized_path:
                basename = Path(normalized_path).name
                if version is None:
                    fallback = """
                    SELECT * FROM archives
                    WHERE SPLIT_PART(original_path, '/', -1) = ?
                    ORDER BY version DESC
                    LIMIT 1
                    """
                    fb_params = (basename,)
                else:
                    fallback = """
                    SELECT * FROM archives
                    WHERE SPLIT_PART(original_path, '/', -1) = ? AND version = ?
                    """
                    fb_params = (basename, int(version))
                cursor = conn.execute(fallback, fb_params)
                result = cursor.fetchone()
            if result and cursor.description:
                cols = [d[0] for d in cursor.description]
                return {cols[i]: result[i] for i in range(len(cols))}
            return None
        finally:
            if self._conn is None:
                conn.close()

    def list_archives(self, show_all: bool = False) -> List[Dict]:
        """List archived files, optionally showing all versions."""
        conn = self._connect()
        try:
            if show_all:
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
                    SPLIT_PART(a.storage_path, '/', -1) AS archive_filename
                FROM archives a
                ORDER BY a.original_path, a.version
                """
            else:
                query = """
                SELECT 
                    a.original_path,
                    MAX(a.version) AS latest_version,
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
            rows = cursor.fetchall() if cursor else []
            if not rows:
                return []
            cols = [d[0] for d in cursor.description]
            return [{cols[i]: row[i] for i in range(len(cols))} for row in rows]
        finally:
            if self._conn is None:
                conn.close()

    def get_stats(self, file_path: Optional[str] = None) -> Dict:
        """Get statistics about archived files, for specific file or all archives."""
        conn = self._connect()
        try:
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
                cursor = conn.execute(query, (path,))
                row = cursor.fetchone() if cursor else None
                if row:
                    cols = [d[0] for d in conn.description] if conn.description else []
                    return dict(zip(cols, row))
                return {}
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
            if row:
                cols = [d[0] for d in conn.description] if conn.description else []
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
            if all_versions:
                sel = "SELECT id, storage_path FROM archives WHERE original_path LIKE ?"
                params = (f"%{Path(file_path).name}%",)
            elif version is not None:
                sel = (
                    "SELECT id, storage_path FROM archives "
                    "WHERE original_path LIKE ? AND version = ?"
                )
                params = (f"%{Path(file_path).name}%", version)
            else:
                sel = """
                SELECT id, storage_path FROM archives
                WHERE original_path LIKE ?
                ORDER BY version DESC
                LIMIT 1
                """
                params = (f"%{Path(file_path).name}%",)
            rows = conn.execute(sel, params).fetchall()
            ids = [r[0] for r in rows] if rows else []
            paths = [r[1] for r in rows] if rows else []
            # Delete stats
            for aid in ids:
                conn.execute("DELETE FROM stats WHERE archive_id = ?", (aid,))
            conn.commit()
            # Delete archives
            if all_versions:
                delq = "DELETE FROM archives WHERE original_path LIKE ?"
                delp = (f"%{Path(file_path).name}%",)
            elif version is not None:
                delq = "DELETE FROM archives WHERE original_path LIKE ? AND version = ?"
                delp = (f"%{Path(file_path).name}%", version)
            else:
                delq = """
                DELETE FROM archives
                WHERE id IN (
                    SELECT id FROM archives WHERE original_path LIKE ? ORDER BY version DESC LIMIT 1
                )"""
                delp = (f"%{Path(file_path).name}%",)
            conn.execute(delq, delp)
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
            cursor = conn.execute(query, (basename,))
            results = cursor.fetchall() if cursor else []
            if not results:
                query = """
                SELECT original_path, MAX(version) as latest_version
                FROM archives
                WHERE CONTAINS(original_path, ?)
                GROUP BY original_path
                ORDER BY original_path
                """
                cursor = conn.execute(query, (name_part,))
                results = cursor.fetchall() if cursor else []
            if not results:
                query = """
                SELECT original_path, version as latest_version, storage_path
                FROM archives
                WHERE CONTAINS(SPLIT_PART(storage_path, '/', -1), ?)
                ORDER BY original_path
                """
                cursor = conn.execute(query, (name_part,))
                results = cursor.fetchall() if cursor else []
            if not results:
                return []
            cols = [d[0] for d in cursor.description]
            return [{cols[i]: row[i] for i in range(len(cols))} for row in results]
        finally:
            if self._conn is None:
                conn.close()
