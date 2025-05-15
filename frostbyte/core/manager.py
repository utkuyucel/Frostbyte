"""
Core archive manager for Frostbyte.

Orchestrates the archiving, restoring, and management of data files.
"""

import logging
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

from frostbyte.core.compressor import Compressor
from frostbyte.core.store import MetadataStore
from frostbyte.utils.file_utils import get_file_hash, get_file_size
from frostbyte.utils.schema import extract_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("frostbyte")


class ArchiveManager:
    def __init__(self, _: Optional[str] = None):
        self.base_dir = Path(os.getcwd())
        self.frostbyte_dir = self.base_dir / ".frostbyte"
        self.archives_dir = self.frostbyte_dir / "archives"

        self.store = MetadataStore(self.frostbyte_dir / "manifest.db")
        self.compressor = Compressor()

    def initialize(self) -> bool:
        try:
            self.frostbyte_dir.mkdir(exist_ok=True)
            self.archives_dir.mkdir(exist_ok=True)
            # Clean up existing archives
            if any(self.archives_dir.iterdir()):
               logger.info(f"Found existing archives directory: {self.archives_dir}")
               logger.info("Cleaning up existing archives...")
               for archive_file in self.archives_dir.glob("*"):
                   if archive_file.is_file():
                       archive_file.unlink()
                       logger.debug(f"Removed existing archive file: {archive_file}")

            self.store.initialize()

            return True
        except Exception as e:
            print(f"Error initializing Frostbyte: {e}")
            return False

    def archive(
        self,
        file_path: str,
        quiet: bool = False,
        progress_callback: Optional[Callable[[float], None]] = None,
    ) -> Dict:
        file_path_obj = Path(file_path).resolve()
        file_path_str = str(file_path_obj)

        file_hash = get_file_hash(file_path_obj)
        original_size = get_file_size(file_path_obj)

        schema = extract_schema(file_path_obj)
        row_count = schema.get("row_count", 0)

        version = self.store.get_next_version(file_path_str)

        archive_name = f"{file_path_obj.stem}_v{version}.parquet"
        archive_path = self.archives_dir / archive_name

        compress_threshold = 10 * 1024 * 1024  # 10 MB in bytes
        should_optimize_compression = original_size >= compress_threshold

        if not quiet:
            if should_optimize_compression:
                logger.info(
                    f"Compressing large file: {file_path} ({original_size / (1024 * 1024):.2f} MB)"
                )
            else:
                logger.info(f"Compressing small file: {file_path} ({original_size / 1024:.2f} KB)")
        target_path, compressed_size = self.compressor.compress(
            file_path, archive_path, progress_callback
        )

        compression_ratio = (
            100 * (1 - (compressed_size / original_size)) if original_size > 0 else 0
        )

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
            original_extension=original_extension,
        )

        return {
            "archive_id": archive_id,
            "original_path": str(file_path),
            "version": version,
            "archive_name": archive_name,
            "original_size": original_size,
            "compressed_size": compressed_size,
            "compression_ratio": compression_ratio,
        }

    def restore(
        self,
        path_spec: str,
        version: Optional[Union[int, float]] = None,
        progress_callback: Optional[Callable[[float], None]] = None,
    ) -> Dict:
        normalized_path_spec = (
            str(Path(path_spec).resolve()) if os.path.exists(path_spec) else path_spec
        )

        if "_v" in path_spec and path_spec.endswith(".parquet"):
            try:
                matches = self.find_by_name(path_spec)
                if matches:
                    if len(matches) == 1:
                        # Use provided version or fall back to the one from the match
                        archive_version = (
                            version if version is not None else matches[0]["latest_version"]
                        )
                        archive_info = self.store.get_archive(
                            matches[0]["original_path"], archive_version
                        )
                    else:
                        match_paths = [
                            f"{m['original_path']} (v{m['latest_version']})" for m in matches
                        ]
                        matches_str = "\n  ".join(match_paths)
                        raise ValueError(
                            f"Multiple archives match '{path_spec}':\n"
                            f"  {matches_str}\n"
                            f"Be more specific."
                        )
                else:
                    raise ValueError(f"No archives found matching: {path_spec}")
            except ValueError:
                archive_info = None
        else:
            if version is not None:
                # First try exact path with version
                archive_info = self.store.get_archive(normalized_path_spec, version)

                # If not found, try using the basename with version
                if not archive_info:
                    basename = Path(path_spec).name
                    matches = self.find_by_name(basename)
                    if matches:
                        versioned_matches = []
                        for match in matches:
                            match_info = self.store.get_archive(match["original_path"], version)
                            if match_info:
                                versioned_matches.append(match_info)

                        if len(versioned_matches) == 1:
                            archive_info = versioned_matches[0]
                        elif len(versioned_matches) > 1:
                            match_paths = [
                                f"{m['original_path']} (v{version})" for m in versioned_matches
                            ]
                            matches_str = "\n  ".join(match_paths)
                            raise ValueError(
                                f"Multiple archives for v{version}:\n  {matches_str}\nSpecify path."
                            )

                if not archive_info:
                    raise ValueError(f"Archive not found: {path_spec} version {version}")
            else:
                # First try as exact path with latest version
                archive_info = self.store.get_archive(normalized_path_spec)

                # If not found, try as a partial name search
                if not archive_info:
                    matches = self.find_by_name(path_spec)
                    if not matches:
                        raise ValueError(f"No archives found matching: {path_spec}")

                    if len(matches) > 1:
                        match_paths = [
                            f"{m['original_path']} (v{m['latest_version']})" for m in matches
                        ]
                        matches_str = "\n  ".join(match_paths)
                        raise ValueError("Multiple matches. Use specific path.")

                    # Get the archive info for the only match
                    archive_info = self.store.get_archive(
                        matches[0]["original_path"], matches[0]["latest_version"]
                    )

        if not archive_info:
            if version is not None:
                raise ValueError(f"Could not locate archive: {path_spec} version {version}")
            raise ValueError(f"Could not locate archive: {path_spec}")

        # Decompress file
        storage_path = Path(archive_info["storage_path"])
        original_path = Path(archive_info["original_path"])
        original_extension = archive_info.get("original_extension")

        # Validate storage path exists
        if not storage_path.exists():
            raise FileNotFoundError(
                f"Archive file not found: {storage_path}. "
                f"The archive may have been deleted or moved."
            )

        if not original_extension:
            # Complex fallback logic for missing extension
            logger.warning(
                f"Original extension not found for {storage_path}. "
                f"Attempting to restore using best guess format."
            )
            original_extension = original_path.suffix or ".csv"

        start_time = time.time()
        try:
            decompress_result = self.compressor.decompress(
                storage_path, original_path, original_extension, progress_callback
            )
            # Store execution time in the result
            decompress_result["execution_time"] = time.time() - start_time
        except ValueError as e:
            if "Invalid Parquet file" in str(e) or "Parquet magic bytes" in str(e):
                # This likely means the file wasn't properly converted to parquet format
                raise ValueError(
                    f"The archive file appears to be corrupted or not in proper Parquet format. "
                    f"This may happen with archives created in older versions. "
                    f"Please try re-archiving the file. Error details: {e!s}"
                ) from e
            raise

        # Calculate or extract file sizes
        schema = archive_info.get("schema", {})
        if isinstance(schema, str):
            import json

            schema = json.loads(schema)

        # Get the compression ratio
        compression_ratio = archive_info.get("compression_ratio", 0)

        original_size = 0
        if schema:
            if "file_size_bytes" in schema:
                original_size = schema.get("file_size_bytes", 0)
            elif "row_count" in schema and "avg_row_bytes" in schema:
                original_size = schema["row_count"] * schema["avg_row_bytes"]

        # Complex size estimation from disk if not available in schema
        if not original_size:
            storage_path = Path(archive_info["storage_path"])
            if storage_path.exists():
                storage_size = storage_path.stat().st_size
                if compression_ratio:
                    if compression_ratio != 100:  # avoid division by zero
                        original_size = storage_size / (1 - compression_ratio / 100)
                    else:
                        original_size = storage_size  # placeholder for 100% compression
                else:
                    original_size = storage_size

        compressed_size = 0
        if original_size:
            if compression_ratio >= 0:
                compressed_size = original_size * (1 - compression_ratio / 100)
            else:
                compressed_size = original_size * (1 + abs(compression_ratio) / 100)

        return {
            "original_path": str(original_path),
            "version": archive_info["version"],
            "timestamp": archive_info["timestamp"],
            "original_size": original_size,
            "compressed_size": compressed_size,
            "compression_ratio": compression_ratio,
            "execution_time": decompress_result.get("execution_time", 0.0),
        }

    def list_archives(self, show_all: bool = False) -> List[Dict]:
        return self.store.list_archives(show_all)

    def get_stats(self, file_path: Optional[str] = None) -> Dict:
        return self.store.get_stats(file_path)

    def purge(
        self,
        file_path: str,
        version: Optional[Union[int, float]] = None,
        all_versions: bool = False,
    ) -> Dict:
        if all_versions:
            version = None

        result = self.store.remove_archives(
            file_path, int(version) if version is not None else None, all_versions
        )

        from contextlib import suppress

        for archive_path in result.get("storage_paths", []):
            with suppress(Exception):
                Path(archive_path).unlink(missing_ok=True)

        return {"original_path": file_path, "version": version, "count": result.get("count", 0)}

    def _parse_version(self, version_str: str) -> Union[int, float]:
        try:
            if "." in version_str:
                return float(version_str)
            return int(version_str)
        except ValueError as err:
            raise ValueError(f"Invalid version format: {version_str}") from err

    def find_by_name(self, name_part: str) -> List[Dict]:
        """Find archives by partial filename match."""
        return self.store.find_archives_by_name(name_part)
