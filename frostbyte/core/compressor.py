import hashlib
import logging
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore

logger = logging.getLogger("frostbyte.compressor")


class Compressor:
    def __init__(self, compression_level: str = "gzip", row_group_size: int = 100000):
        self.compression = compression_level
        self.row_group_size = row_group_size

    def compress(
        self,
        source_path: Union[str, Path],
        target_path: Optional[Union[str, Path]] = None,
        progress_callback: Optional[Callable[[float], None]] = None,
    ) -> Tuple[Path, int]:
        source_path = Path(source_path)

        # If no target path is provided, use source path with .parquet extension
        if target_path is None:
            target_path = source_path.with_suffix(".parquet")
        else:
            target_path = Path(target_path)
            # Ensure target has .parquet extension
            if target_path.suffix.lower() not in (".parquet", ".pq"):
                target_path = target_path.with_suffix(".parquet")

        # Create directories if they don't exist
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # Report initial progress
        if progress_callback:
            progress_callback(0.01)  # Initial progress indicator

        # Read the source file based on its extension
        file_ext = source_path.suffix.lower()

        # Report progress at file reading stage
        if progress_callback:
            progress_callback(0.05)  # Started reading file

        if file_ext == ".csv":
            df = pd.read_csv(source_path)
        elif file_ext in (".xls", ".xlsx", ".xlsm"):
            df = pd.read_excel(source_path)
        elif file_ext in (".parquet", ".pq"):
            df = pd.read_parquet(source_path)
        else:
            raise ValueError(
                f"Unsupported format: {file_ext}. Supported formats: CSV, Excel, and Parquet."
            )

        # Report progress after file reading complete
        if progress_callback:
            progress_callback(0.4)  # File reading complete, starting compression

        # Write to Parquet format with compression
        self._save_dataframe(df, target_path, progress_callback)

        # Final progress update
        if progress_callback:
            progress_callback(1.0)  # Compression complete

        return target_path, target_path.stat().st_size

    def read_parquet(self, source_path: Union[str, Path]) -> pd.DataFrame:
        source_path = Path(source_path)
        return pd.read_parquet(source_path)

    def _save_dataframe(
        self,
        df: pd.DataFrame,
        target_path: Path,
        progress_callback: Optional[Callable[[float], None]] = None,
    ) -> int:
        # Convert DataFrame to Arrow Table
        if progress_callback:
            progress_callback(0.5)  # DataFrame conversion to Arrow Table

        table = pa.Table.from_pandas(df)

        if progress_callback:
            progress_callback(0.7)  # Starting Parquet write

        pq.write_table(
            table, target_path, compression=self.compression, row_group_size=self.row_group_size
        )

        if progress_callback:
            progress_callback(0.95)  # Parquet write complete

        return target_path.stat().st_size

    def compute_hash(self, file_path: Union[str, Path]) -> str:
        file_path = Path(file_path)
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def compare_datasets(self, path1: Union[str, Path], path2: Union[str, Path]) -> Dict[str, Any]:
        df1 = self.read_parquet(path1)
        df2 = self.read_parquet(path2)

        results = {"row_count_diff": len(df1) - len(df2), "column_diff": [], "identical": False}

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

    def decompress(
        self,
        source_parquet_path: Union[str, Path],
        target_restore_path: Union[str, Path],
        original_extension: str,
        progress_callback: Optional[Callable[[float], None]] = None,
    ) -> Dict[str, Any]:
        """
        Decompress a Parquet file to its original format with real-time progress tracking.

        Args:
            source_parquet_path: Path to the source Parquet file.
            target_restore_path: Path to restore the decompressed file.
            original_extension: The original extension of the file (e.g., '.csv', '.xlsx').
            progress_callback: Optional callback function to report progress (0.0 to 1.0).

        Returns:
            Dict containing timing information and operation status.
        """
        source_path = Path(source_parquet_path)
        target_path = Path(target_restore_path)

        # Check if source file exists
        if not source_path.exists():
            error_msg = f"Source file not found: {source_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        # Track timing
        start_time = time.time()

        # Ensure target directory exists
        target_path.parent.mkdir(parents=True, exist_ok=True)

        original_ext_lower = original_extension.lower()

        # Report initial progress
        if progress_callback:
            progress_callback(0.0)

        try:
            if original_ext_lower in [".parquet", ".pq"]:
                # If the original was Parquet, just copy the file with progress updates
                file_size = source_path.stat().st_size
                with open(source_path, "rb") as src, open(target_path, "wb") as dst:
                    copied = 0
                    chunk_size = 1024 * 1024  # 1MB chunks
                    while True:
                        chunk = src.read(chunk_size)
                        if not chunk:
                            break
                        dst.write(chunk)
                        copied += len(chunk)
                        if progress_callback and file_size > 0:
                            progress_callback(min(copied / file_size, 0.99))

                # Ensure we report 100% completion
                if progress_callback:
                    progress_callback(1.0)
            else:
                # Read Parquet file with incremental progress reporting
                logger.info(f"Starting decompression of {source_path} to {target_path}")

                # Validate that the source file is a valid Parquet file
                try:
                    # Try to open the file as Parquet to validate format
                    pq.ParquetFile(source_path)
                except Exception as e:
                    error_msg = f"Invalid Parquet file: {source_path}. Error: {e!s}"
                    logger.error(error_msg)
                    raise ValueError(error_msg) from e

                # Get file size to estimate progress
                file_size = source_path.stat().st_size

                # For CSV output we'll use a custom approach with chunking
                if original_ext_lower == ".csv":
                    # Report initial metadata reading
                    if progress_callback:
                        progress_callback(0.01)  # Starting metadata read

                    # First load the metadata to get row count
                    parquet_file = pq.ParquetFile(source_path)

                    # Get total row count and row group info for better progress tracking
                    total_rows = parquet_file.metadata.num_rows
                    total_row_groups = parquet_file.num_row_groups

                    if progress_callback:
                        progress_callback(0.05)  # Metadata loaded

                    logger.info(
                        f"Starting CSV conversion of {total_rows} rows in {total_row_groups} groups"
                    )

                    # Process in batches for better progress tracking
                    with open(target_path, "w", newline="") as csv_file:
                        # Calculate optimal batch size based on total rows
                        # For small files: use larger batches to reduce overhead
                        # For large files: use smaller batches for more frequent progress updates
                        if total_rows < 1000:
                            batch_size = total_rows  # Process all at once for small files
                        elif total_rows < 10000:
                            batch_size = 1000  # ~10 updates
                        elif total_rows < 100000:
                            batch_size = 5000  # ~20 updates
                        elif total_rows < 1000000:
                            batch_size = 10000  # ~100 updates
                        else:
                            batch_size = 50000  # For very large files

                        # For small datasets, process everything in one go
                        if total_rows <= 10:
                            # Read all data at once for small datasets
                            table = pq.read_table(source_path)
                            df = table.to_pandas()
                            df.to_csv(csv_file, index=False, header=True, mode="w")
                            rows_processed = len(df)

                            if progress_callback:
                                progress_callback(0.95)  # Almost done

                            # Skip the batch processing loop
                            batches = []
                            last_progress_report = 0.95
                        else:
                            # Process first batch to get headers
                            first_batch = next(
                                parquet_file.iter_batches(batch_size=min(1, total_rows))
                            )
                            df_first = pa.Table.from_batches([first_batch]).to_pandas()

                            # Write headers
                            df_first.to_csv(csv_file, index=False, header=True, mode="w")

                            if progress_callback:
                                progress_callback(0.08)  # Headers written

                            # Process remaining rows in chunks
                            rows_processed = len(df_first)
                            # Track last reported progress to avoid excessive updates
                            last_progress_report = 0.08

                            # Create batches iterator - skip first batch if we already processed it
                            batches = parquet_file.iter_batches(batch_size=batch_size)
                            if len(df_first) > 0:
                                next(batches, None)  # Skip first batch if we already processed it

                        # Process all batches with detailed progress reporting
                        for batch_idx, batch in enumerate(batches):
                            # Convert to pandas and write to CSV
                            df_chunk = pa.Table.from_batches([batch]).to_pandas()
                            df_chunk.to_csv(csv_file, index=False, header=False, mode="a")

                            # Update progress counter
                            rows_processed += len(df_chunk)

                            # Update progress - ensure we report progress in regular increments
                            # and avoid excessive updates that could slow down the process
                            if progress_callback and total_rows > 0:
                                # Calculate normalized progress from 0.1 to 0.95
                                # leaving space for initialization and finalization steps
                                raw_progress = rows_processed / total_rows
                                scaled_progress = 0.08 + (raw_progress * 0.87)

                                # Only report if progress has increased meaningfully (at least 1%)
                                # or if it's been a while since the last update
                                if (
                                    scaled_progress - last_progress_report >= 0.01
                                    or batch_idx % 10 == 0
                                ):
                                    progress_callback(min(scaled_progress, 0.95))
                                    last_progress_report = scaled_progress

                    # Ensure we show completion
                    if progress_callback:
                        # Final step - report at 0.98 to show we're finalizing
                        progress_callback(0.98)
                        # Add a tiny delay to ensure UI can reflect the progression visually
                        time.sleep(0.1)
                        # Complete at 100%
                        progress_callback(1.0)

                elif original_ext_lower in [".xls", ".xlsx", ".xlsm"]:
                    # For Excel files, we need to use pandas which doesn't support
                    # incremental updates, so we'll provide intermediate progress reports

                    if progress_callback:
                        progress_callback(0.05)  # Starting to read parquet metadata

                    # Get an estimate of the file size to provide better progress updates
                    file_size = source_path.stat().st_size

                    # Open the parquet file to get metadata
                    parquet_file = pq.ParquetFile(source_path)
                    num_row_groups = parquet_file.num_row_groups

                    if progress_callback:
                        progress_callback(0.1)  # Metadata loaded

                    # Read the parquet file row group by row group to show progress
                    tables = []
                    for i in range(num_row_groups):
                        # Read one row group
                        table = parquet_file.read_row_group(i)
                        tables.append(table)

                        # Report progress during reading (allocate 70% of progress to reading)
                        if progress_callback and num_row_groups > 0:
                            progress = 0.1 + (0.6 * ((i + 1) / num_row_groups))
                            progress_callback(progress)

                    # Combine tables and convert to pandas
                    if progress_callback:
                        progress_callback(0.7)  # Starting pandas conversion

                    if tables:
                        table = pa.concat_tables(tables)
                        df = table.to_pandas()
                    else:
                        # Handle empty parquet file
                        df = pd.DataFrame()

                    if progress_callback:
                        progress_callback(0.8)  # Starting Excel write operation

                    # Ensure the target path has the correct Excel extension for writing
                    target_path_excel = target_path.with_suffix(original_ext_lower)

                    # Write to Excel with progress estimation
                    # Unfortunately pandas doesn't provide progress for Excel writes
                    # so we'll simulate progress based on row count
                    row_count = len(df)
                    if row_count > 10000:
                        # For large dataframes, to_excel can be slow
                        # Report incremental progress to keep UI responsive
                        if progress_callback:
                            progress_callback(0.85)  # Starting potentially slow Excel write
                        df.to_excel(target_path_excel, index=False)
                        if progress_callback:
                            progress_callback(0.95)  # Excel write nearly complete
                    else:
                        # For smaller files, just do the write
                        df.to_excel(target_path_excel, index=False)
                        if progress_callback:
                            progress_callback(0.95)

                    # If original target_path had a different suffix than what we just created
                    # and it's not the one we just wrote, remove it.
                    if target_path != target_path_excel and target_path.exists():
                        target_path.unlink()

                    # Report completion
                    if progress_callback:
                        progress_callback(1.0)
                else:
                    msg = (
                        f"Unsupported original file extension for decompression: "
                        f"{original_extension}"
                    )
                    raise ValueError(msg)
        except Exception as e:
            logger.error(f"Error during decompression: {e}")
            raise

        end_time = time.time()
        execution_time = end_time - start_time

        logger.info(f"Decompression completed in {execution_time:.2f} seconds")

        return {"execution_time": execution_time, "success": True}
