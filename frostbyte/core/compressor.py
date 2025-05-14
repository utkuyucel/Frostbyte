"""
Compression functionality for Frostbyte.

Provides Parquet-based data storage for efficient dataset versioning and management.
Uses Parquet format exclusively for columnar compression of data files.
"""

import hashlib
import shutil
from pathlib import Path
from typing import Union, Dict, Any, Tuple, Optional

import pandas as pd
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore

class Compressor:
    """Handles Parquet-based data storage and retrieval for dataset versioning."""

    def __init__(self, compression_level: str = 'snappy', row_group_size: int = 100000):
        """Initialize the compressor with compression settings.

        Args:
            compression_level: Parquet compression codec ('snappy', 'gzip', 'brotli', etc.)
            row_group_size: Number of rows per row group in Parquet file
        """
        self.compression = compression_level
        self.row_group_size = row_group_size

    def compress(self, source_path: Union[str, Path], target_path: Optional[Union[str, Path]] = None) -> Tuple[Path, int]:
        """Convert any data file to Parquet format and return the path and compressed file size.

        Args:
            source_path: Path to the source data file
            target_path: Optional path for the output file. If not provided, uses source path with .parquet extension

        Returns:
            tuple: (Path to parquet file, size in bytes)
        """
        source_path = Path(source_path)

        # If no target path is provided, use source path with .parquet extension
        if target_path is None:
            target_path = source_path.with_suffix('.parquet')
        else:
            target_path = Path(target_path)
            # Ensure target has .parquet extension
            if target_path.suffix.lower() not in ('.parquet', '.pq'):
                target_path = target_path.with_suffix('.parquet')

        # Create directories if they don't exist
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # Read the source file based on its extension
        file_ext = source_path.suffix.lower()
        if file_ext == '.csv':
            df = pd.read_csv(source_path)
        elif file_ext in ('.xls', '.xlsx', '.xlsm'):
            df = pd.read_excel(source_path)
        elif file_ext in ('.parquet', '.pq'):
            df = pd.read_parquet(source_path)
        else:
            raise ValueError(
                f"Unsupported file format: {file_ext}. Supported input formats are CSV, Excel, and Parquet."
            )

        # Write to Parquet format with compression
        self._save_dataframe(df, target_path)

        return target_path, target_path.stat().st_size

    def read_parquet(self, source_path: Union[str, Path]) -> pd.DataFrame:
        """Read a Parquet file and return as DataFrame."""
        source_path = Path(source_path)
        return pd.read_parquet(source_path)

    def _save_dataframe(self, df: pd.DataFrame, target_path: Path) -> int:
        """Save DataFrame to Parquet format and return file size."""
        table = pa.Table.from_pandas(df)
        pq.write_table(
            table,
            target_path,
            compression=self.compression,
            row_group_size=self.row_group_size
        )
        return target_path.stat().st_size

    def compute_hash(self, file_path: Union[str, Path]) -> str:
        """Compute SHA-256 hash of parquet file contents for versioning."""
        file_path = Path(file_path)

        # For Parquet files, hash the raw content
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def compare_datasets(self, path1: Union[str, Path], path2: Union[str, Path]) -> Dict[str, Any]:
        """Compare two Parquet datasets and return differences.

        Returns a dictionary with:
            - row_count_diff: Difference in number of rows
            - column_diff: List of columns that differ
            - identical: Boolean indicating if datasets are identical
        """
        df1 = self.read_parquet(path1)
        df2 = self.read_parquet(path2)

        results = {
            "row_count_diff": len(df1) - len(df2),
            "column_diff": [],
            "identical": False
        }

        # Check for column differences
        columns1 = set(df1.columns)
        columns2 = set(df2.columns)
        results["column_diff"] = list(columns1.symmetric_difference(columns2))

        # For common columns, check if content is identical
        common_columns = columns1.intersection(columns2)
        if not common_columns or results["row_count_diff"] != 0 or results["column_diff"]:
            return results

        # If shapes and columns match, check for equality
        try:
            results["identical"] = df1.equals(df2)
        except Exception:
            results["identical"] = False
            
        return results

    def decompress(self, source_parquet_path: Union[str, Path], target_restore_path: Union[str, Path], original_extension: str) -> None:
        """
        Decompress a Parquet file to its original format.

        Args:
            source_parquet_path: Path to the source Parquet file.
            target_restore_path: Path to restore the decompressed file.
            original_extension: The original extension of the file (e.g., '.csv', '.xlsx').
        """
        source_path = Path(source_parquet_path)
        target_path = Path(target_restore_path)

        # Ensure target directory exists
        target_path.parent.mkdir(parents=True, exist_ok=True)

        original_ext_lower = original_extension.lower()

        if original_ext_lower in ['.parquet', '.pq']:
            # If the original was Parquet, just copy it
            shutil.copyfile(source_path, target_path)
        else:
            # Read Parquet file
            df = pd.read_parquet(source_path)

            if original_ext_lower == '.csv':
                df.to_csv(target_path, index=False)
            elif original_ext_lower in ['.xls', '.xlsx', '.xlsm']:
                # Ensure the target path has the correct Excel extension for writing
                # pandas to_excel uses the suffix of the path to determine the writer engine
                target_path_excel = target_path.with_suffix(original_ext_lower)
                df.to_excel(target_path_excel, index=False)
                # If the original target_path had a different suffix (e.g. from original_path.suffix)
                # and it's not the one we just wrote, remove it.
                if target_path != target_path_excel and target_path.exists():
                     target_path.unlink()
            else:
                raise ValueError(f"Unsupported original file extension for decompression: {original_extension}")
