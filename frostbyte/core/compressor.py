import contextlib
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

    def _estimate_rows_and_chunk_size(self, source_path: Path, file_size: int) -> Tuple[int, int]:
        """Estimate total rows and determine optimal chunk size for CSV files."""
        with open(source_path) as f:
            sample_lines = []
            for _ in range(10):
                try:
                    sample_lines.append(next(f))
                except StopIteration:
                    break
            
        if sample_lines:
            avg_line_size = sum(len(line) for line in sample_lines) / len(sample_lines)
            estimated_total_rows = max(100, int(file_size / avg_line_size))
        else:
            estimated_total_rows = 1000

        # Determine chunk size based on estimated rows
        if estimated_total_rows < 1000:
            return estimated_total_rows, estimated_total_rows
        if estimated_total_rows < 10000:
            return estimated_total_rows, 1000
        if estimated_total_rows < 100000:
            return estimated_total_rows, 5000
        if estimated_total_rows < 1000000:
            return estimated_total_rows, 10000
        return estimated_total_rows, 50000

    def _process_csv_file(
        self, 
        source_path: Path, 
        file_size: int, 
        progress_callback: Optional[Callable[[float], None]] = None
    ) -> pd.DataFrame:
        """Process CSV file with progress tracking."""
        estimated_total_rows, chunksize = self._estimate_rows_and_chunk_size(
            source_path, file_size
        )
        
        chunks = []
        total_rows_read = 0
        last_progress_report = 0.05

        for i, chunk in enumerate(pd.read_csv(source_path, chunksize=chunksize)):
            chunks.append(chunk)
            total_rows_read += len(chunk)

            if progress_callback and i % 2 == 0:
                raw_progress = min(total_rows_read / estimated_total_rows, 1.0)
                scaled_progress = 0.05 + (raw_progress * 0.25)

                if scaled_progress - last_progress_report >= 0.01:
                    progress_callback(scaled_progress)
                    last_progress_report = scaled_progress

        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

    def _determine_batch_size(self, total_rows: int) -> int:
        """Determine optimal batch size based on total rows."""
        if total_rows < 1000:
            return total_rows
        if total_rows < 10000:
            return 1000
        if total_rows < 100000:
            return 5000
        if total_rows < 1000000:
            return 10000
        return 50000

    def compress(
        self,
        source_path: Union[str, Path],
        target_path: Optional[Union[str, Path]] = None,
        progress_callback: Optional[Callable[[float], None]] = None,
    ) -> Tuple[Path, int]:
        start_time = time.time()
        source_path = Path(source_path)
        logger.info(f"Starting compression of {source_path}")

        if target_path is None:
            target_path = source_path.with_suffix(".parquet")
        else:
            target_path = Path(target_path)
            if target_path.suffix.lower() not in (".parquet", ".pq"):
                target_path = target_path.with_suffix(".parquet")

        target_path.parent.mkdir(parents=True, exist_ok=True)

        if progress_callback:
            progress_callback(0.01)

        file_ext = source_path.suffix.lower()
        file_size = source_path.stat().st_size

        try:
            if progress_callback:
                progress_callback(0.05)

            if file_ext == ".csv":
                df = self._process_csv_file(source_path, file_size, progress_callback)
                if progress_callback:
                    progress_callback(0.35)

            elif file_ext in (".xls", ".xlsx", ".xlsm"):
                if progress_callback and file_size > 10_000_000:
                    progress_callback(0.1)
                    progress_callback(0.2)

                df = pd.read_excel(source_path)

                if progress_callback:
                    progress_callback(0.35)

            elif file_ext in (".parquet", ".pq"):
                parquet_file = pq.ParquetFile(source_path)

                if progress_callback:
                    progress_callback(0.1)

                if parquet_file.num_row_groups > 1:
                    tables = []
                    for i in range(parquet_file.num_row_groups):
                        table = parquet_file.read_row_group(i)
                        tables.append(table)

                        if progress_callback and parquet_file.num_row_groups > 0:
                            progress = 0.1 + (0.2 * ((i + 1) / parquet_file.num_row_groups))
                            progress_callback(progress)

                    table = pa.concat_tables(tables)
                    df = table.to_pandas()
                else:
                    df = pd.read_parquet(source_path)

                if progress_callback:
                    progress_callback(0.35)
            else:
                raise ValueError(
                    f"Unsupported format: {file_ext}. Supported formats: CSV, Excel, and Parquet."
                )

            if progress_callback:
                progress_callback(0.4)

            self._save_dataframe(df, target_path, progress_callback)

            if progress_callback:
                progress_callback(1.0)

            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(f"Compression completed in {execution_time:.2f} seconds")

            return target_path, target_path.stat().st_size

        except Exception as e:
            logger.error(f"Error during compression: {e}")
            raise

    def read_parquet(self, source_path: Union[str, Path]) -> pd.DataFrame:
        source_path = Path(source_path)
        return pd.read_parquet(source_path)

    def _save_dataframe(
        self,
        df: pd.DataFrame,
        target_path: Path,
        progress_callback: Optional[Callable[[float], None]] = None,
    ) -> int:
        if progress_callback:
            progress_callback(0.5)

        row_count = len(df)
        table = pa.Table.from_pandas(df)

        if progress_callback:
            progress_callback(0.6)
            progress_callback(0.7)

        if row_count > 10000 and self.row_group_size < row_count:
            num_row_groups = (row_count + self.row_group_size - 1) // self.row_group_size

            with pq.ParquetWriter(
                target_path, table.schema, compression=self.compression
            ) as writer:
                for i in range(num_row_groups):
                    start_idx = i * self.row_group_size
                    end_idx = min((i + 1) * self.row_group_size, row_count)

                    batch = table.slice(start_idx, end_idx - start_idx)
                    writer.write_table(batch)

                    if progress_callback:
                        progress = 0.7 + (0.25 * ((i + 1) / num_row_groups))
                        progress_callback(min(progress, 0.95))
        else:
            pq.write_table(
                table, target_path, compression=self.compression, row_group_size=self.row_group_size
            )

        if progress_callback:
            progress_callback(0.95)

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

        columns1 = set(df1.columns)
        columns2 = set(df2.columns)
        results["column_diff"] = list(columns1.symmetric_difference(columns2))

        common_columns = columns1.intersection(columns2)
        if not common_columns or results["row_count_diff"] != 0 or results["column_diff"]:
            return results

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

        if not source_path.exists():
            error_msg = f"Source file not found: {source_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        start_time = time.time()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        original_ext_lower = original_extension.lower()

        if progress_callback:
            progress_callback(0.0)

        try:
            if original_ext_lower in [".parquet", ".pq"]:
                file_size = source_path.stat().st_size
                with open(source_path, "rb") as src, open(target_path, "wb") as dst:
                    copied = 0
                    chunk_size = 1024 * 1024
                    while True:
                        chunk = src.read(chunk_size)
                        if not chunk:
                            break
                        dst.write(chunk)
                        copied += len(chunk)
                        if progress_callback and file_size > 0:
                            progress_callback(min(copied / file_size, 0.99))

                if progress_callback:
                    progress_callback(1.0)
            else:
                logger.info(f"Starting decompression of {source_path} to {target_path}")

                try:
                    pq.ParquetFile(source_path)
                except Exception as e:
                    error_msg = f"Invalid Parquet file: {source_path}. Error: {e!s}"
                    logger.error(error_msg)
                    raise ValueError(error_msg) from e

                file_size = source_path.stat().st_size

                if original_ext_lower == ".csv":
                    if progress_callback:
                        progress_callback(0.01)

                    parquet_file = pq.ParquetFile(source_path)
                    total_rows = parquet_file.metadata.num_rows
                    total_row_groups = parquet_file.num_row_groups

                    if progress_callback:
                        progress_callback(0.05)

                    logger.info(
                        f"Starting CSV conversion of {total_rows} rows in {total_row_groups} groups"
                    )

                    with open(target_path, "w", newline="") as csv_file:
                        if total_rows < 1000:
                            batch_size = total_rows
                        elif total_rows < 10000:
                            batch_size = 1000
                        elif total_rows < 100000:
                            batch_size = 5000
                        elif total_rows < 1000000:
                            batch_size = 10000
                        else:
                            batch_size = 50000

                        if total_rows <= 10:
                            table = pq.read_table(source_path)
                            df = table.to_pandas()
                            df.to_csv(csv_file, index=False, header=True, mode="w")
                            rows_processed = len(df)

                            if progress_callback:
                                progress_callback(0.95)

                            batches = []
                            last_progress_report = 0.95
                        else:
                            first_batch = next(
                                parquet_file.iter_batches(batch_size=min(1, total_rows))
                            )
                            df_first = pa.Table.from_batches([first_batch]).to_pandas()
                            df_first.to_csv(csv_file, index=False, header=True, mode="w")

                            if progress_callback:
                                progress_callback(0.08)

                            rows_processed = len(df_first)
                            last_progress_report = 0.08

                            batch_iterator = parquet_file.iter_batches(batch_size=batch_size)
                            if len(df_first) > 0:
                                # Skip the first batch since we already processed it
                                with contextlib.suppress(StopIteration):
                                    next(batch_iterator)
                            batches = list(batch_iterator)

                        for batch_idx, batch in enumerate(batches):
                            df_chunk = pa.Table.from_batches([batch]).to_pandas()
                            df_chunk.to_csv(csv_file, index=False, header=False, mode="a")
                            rows_processed += len(df_chunk)

                            if progress_callback and total_rows > 0:
                                raw_progress = rows_processed / total_rows
                                scaled_progress = 0.08 + (raw_progress * 0.87)

                                if (
                                    scaled_progress - last_progress_report >= 0.01
                                    or batch_idx % 10 == 0
                                ):
                                    progress_callback(min(scaled_progress, 0.95))
                                    last_progress_report = scaled_progress

                    if progress_callback:
                        progress_callback(1.0)

                elif original_ext_lower in [".xls", ".xlsx", ".xlsm"]:
                    if progress_callback:
                        progress_callback(0.05)

                    file_size = source_path.stat().st_size
                    parquet_file = pq.ParquetFile(source_path)
                    num_row_groups = parquet_file.num_row_groups

                    if progress_callback:
                        progress_callback(0.1)

                    tables = []
                    for i in range(num_row_groups):
                        table = parquet_file.read_row_group(i)
                        tables.append(table)

                        if progress_callback and num_row_groups > 0:
                            progress = 0.1 + (0.6 * ((i + 1) / num_row_groups))
                            progress_callback(progress)

                    if progress_callback:
                        progress_callback(0.7)

                    if tables:
                        table = pa.concat_tables(tables)
                        df = table.to_pandas()
                    else:
                        df = pd.DataFrame()

                    if progress_callback:
                        progress_callback(0.8)

                    target_path_excel = target_path.with_suffix(original_ext_lower)

                    df_row_count = len(df)
                    if df_row_count > 10000:
                        if progress_callback:
                            progress_callback(0.85)
                        df.to_excel(target_path_excel, index=False)
                        if progress_callback:
                            progress_callback(0.95)
                    else:
                        df.to_excel(target_path_excel, index=False)
                        if progress_callback:
                            progress_callback(0.95)

                    if target_path != target_path_excel and target_path.exists():
                        target_path.unlink()

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
