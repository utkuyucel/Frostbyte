"""
Archive validation for Frostbyte.

Provides comprehensive validation focusing on hash-based integrity checking.
"""

import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from frostbyte.core.compressor import Compressor
from frostbyte.core.store import MetadataStore
from frostbyte.utils.file_utils import get_file_hash

logger = logging.getLogger("frostbyte.validation")


@dataclass(frozen=True)
class ValidationResult:
    """Immutable result of a validation check."""

    is_valid: bool
    errors: List[str]
    warnings: List[str]
    check_type: str
    details: Optional[Dict[str, Any]] = None


class ArchiveValidator:
    """Archive validation system with focus on hash-based integrity."""

    def __init__(self, store: MetadataStore, archives_dir: Path):
        self.store = store
        self.archives_dir = Path(archives_dir)
        self.compressor = Compressor()

    def validate_content_hash(
        self, file_path: str, version: Optional[int] = None
    ) -> ValidationResult:
        """Compare content hash with stored metadata."""
        errors: List[str] = []
        warnings: List[str] = []
        details: Dict[str, Any] = {}

        try:
            archive_info = self.store.get_archive(file_path, version)
            if not archive_info:
                errors.append(f"Archive not found: {file_path}")
                return ValidationResult(False, errors, warnings, "hash", details)

            stored_hash = archive_info.get("hash")
            details["stored_hash"] = stored_hash[:16] + "..." if stored_hash else "None"

            if not stored_hash:
                warnings.append("No stored hash found for comparison")
                return ValidationResult(True, errors, warnings, "hash", details)

            storage_path = Path(archive_info["storage_path"])
            original_extension = archive_info.get("original_extension", ".csv")

            with tempfile.NamedTemporaryFile(suffix=original_extension, delete=False) as temp_file:
                temp_path = Path(temp_file.name)

            try:
                self.compressor.decompress(storage_path, temp_path, original_extension)

                current_hash = get_file_hash(temp_path)
                details["current_hash"] = current_hash[:16] + "..."
                details["hashes_match"] = stored_hash == current_hash

                if stored_hash != current_hash:
                    errors.append("Content hash mismatch detected!")
                    errors.append(f"  Stored:  {stored_hash[:16]}...")
                    errors.append(f"  Current: {current_hash[:16]}...")
                    errors.append("This indicates data corruption or modification")

            finally:
                temp_path.unlink(missing_ok=True)

        except Exception as e:
            errors.append(f"Hash validation failed: {e!s}")

        return ValidationResult(len(errors) == 0, errors, warnings, "hash", details)

    def validate_row_integrity(
        self, file_path: str, version: Optional[int] = None, sample_rate: float = 0.1
    ) -> ValidationResult:
        """Compare row counts and sample data integrity."""
        errors: List[str] = []
        warnings: List[str] = []
        details: Dict[str, Any] = {}

        try:
            archive_info = self.store.get_archive(file_path, version)
            if not archive_info:
                errors.append(f"Archive not found: {file_path}")
                return ValidationResult(False, errors, warnings, "rows", details)

            expected_rows = archive_info.get("row_count", 0)
            details["expected_rows"] = expected_rows

            storage_path = Path(archive_info["storage_path"])
            original_extension = archive_info.get("original_extension", ".csv")

            with tempfile.NamedTemporaryFile(suffix=original_extension, delete=False) as temp_file:
                temp_path = Path(temp_file.name)

            try:
                self.compressor.decompress(storage_path, temp_path, original_extension)

                actual_rows = self._count_rows(temp_path, original_extension)
                details["actual_rows"] = actual_rows

                if expected_rows != actual_rows:
                    errors.append("Row count mismatch:")
                    errors.append(f"  Expected: {expected_rows:,} rows")
                    errors.append(f"  Actual:   {actual_rows:,} rows")
                    errors.append(f"  Difference: {abs(expected_rows - actual_rows):,} rows")

                if actual_rows > 10000 and sample_rate > 0:
                    sample_errors = self._validate_sample_data(
                        temp_path, original_extension, sample_rate
                    )
                    errors.extend(sample_errors)
                    details["sample_validation_performed"] = True
                    details["sample_rate"] = sample_rate
                else:
                    details["sample_validation_performed"] = False

            finally:
                temp_path.unlink(missing_ok=True)

        except Exception as e:
            errors.append(f"Row validation failed: {e!s}")

        return ValidationResult(len(errors) == 0, errors, warnings, "rows", details)

    def validate_all_archives(
        self, validation_levels: List[str], sample_rate: float = 0.1
    ) -> Dict[str, List[ValidationResult]]:
        """Validate all archives in the database."""
        all_archives = self.store.list_archives()
        results = {}

        for archive_summary in all_archives:
            file_path = archive_summary["original_path"]
            file_versions = self.store.list_archives(file_name=file_path)
            file_results = []

            for version_info in file_versions:
                version = version_info["version"]

                for level in validation_levels:
                    if level == "hash":
                        result = self.validate_content_hash(file_path, version)
                        file_results.append(result)
                    elif level == "rows":
                        result = self.validate_row_integrity(file_path, version, sample_rate)
                        file_results.append(result)

            results[file_path] = file_results

        return results

    def _count_rows(self, file_path: Path, extension: str) -> int:
        """Count rows in a data file."""
        try:
            if extension.lower() == ".csv":
                with open(file_path, encoding="utf-8") as f:
                    line_count = sum(1 for _ in f)
                return max(0, line_count - 1)  # Subtract header
            if extension.lower() == ".parquet":
                df = pd.read_parquet(file_path)
                return len(df)
            return 0
        except Exception:
            return 0

    def _validate_sample_data(
        self, file_path: Path, extension: str, sample_rate: float
    ) -> List[str]:
        """Validate a sample of the data for consistency."""
        errors = []
        try:
            if extension.lower() == ".csv":
                df = pd.read_csv(file_path)
            elif extension.lower() == ".parquet":
                df = pd.read_parquet(file_path)
            else:
                return ["Unsupported file type for sampling"]

            if df.empty:
                errors.append("Sample data is empty")
                return errors

            sample_size = max(1, int(len(df) * sample_rate))
            sample_df = df.sample(n=sample_size)

            # Basic data quality checks on sample
            total_cells = len(sample_df) * len(sample_df.columns)
            null_count = sample_df.isnull().sum().sum()
            null_percentage = (null_count / total_cells) * 100

            if null_percentage > 50:
                errors.append(f"High null percentage in sample: {null_percentage:.1f}%")

            empty_cols = sample_df.columns[sample_df.isnull().all()].tolist()
            if empty_cols:
                errors.append(f"Empty columns detected in sample: {empty_cols}")

        except Exception as e:
            errors.append(f"Sample validation failed: {e!s}")

        return errors
