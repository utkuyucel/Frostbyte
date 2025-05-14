"""
Tests for the Frostbyte compressor module.
"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from frostbyte.core.compressor import Compressor


def test_compress() -> None:
    """Test data file compression to Parquet format."""
    # Create a temporary CSV file with test data
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
        temp_file_path = Path(temp_file.name)

    try:
        # Create test DataFrame and save as CSV
        test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        test_data.to_csv(temp_file_path, index=False)

        # Compress the file to Parquet
        compressor = Compressor()
        target_path = None  # Let the compressor determine the output path
        parquet_path, compressed_size = compressor.compress(temp_file_path, target_path)

        # Verify compression worked
        assert parquet_path.exists()
        assert parquet_path.suffix == ".parquet"
        assert compressed_size > 0

        # Read back and verify data
        df_read = compressor.read_parquet(parquet_path)
        assert df_read.equals(test_data)

        # Test with explicit target path
        explicit_path = Path(tempfile.mktemp(suffix=".custom"))
        actual_path, _ = compressor.compress(temp_file_path, explicit_path)

        # Verify extension was corrected to .parquet
        assert actual_path.suffix == ".parquet"
        assert actual_path.exists()

    finally:
        # Clean up temp files
        temp_file_path.unlink(missing_ok=True)
        if "parquet_path" in locals() and Path(parquet_path).exists():
            Path(parquet_path).unlink(missing_ok=True)
        if "actual_path" in locals() and Path(actual_path).exists():
            Path(actual_path).unlink(missing_ok=True)


def test_unsupported_file_format() -> None:
    """Test error handling for unsupported file formats."""
    # Create a temporary text file
    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as temp_file:
        temp_file_path = Path(temp_file.name)
        temp_file.write(b"This is a plain text file that should not be compressed with Parquet.")

    try:
        compressor = Compressor()

        # Attempt to compress the text file should raise ValueError
        with pytest.raises(ValueError) as excinfo:
            compressor.compress(temp_file_path)

        # Verify the error message
        assert (
            str(excinfo.value)
            == "Unsupported format: .txt. Supported formats: CSV, Excel, and Parquet."
        )

    finally:
        # Clean up temp file
        temp_file_path.unlink(missing_ok=True)


def test_compute_hash() -> None:
    """Test computing hash of a Parquet file."""
    # Create a temporary Parquet file with test data
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
        temp_file_path = Path(temp_file.name)

    try:
        # Create test DataFrame and save as Parquet
        test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        compressor = Compressor()
        compressor._save_dataframe(test_data, temp_file_path)

        # Compute hash
        hash1 = compressor.compute_hash(temp_file_path)
        assert isinstance(hash1, str)
        assert len(hash1) > 0

        # Same data should produce same hash
        hash2 = compressor.compute_hash(temp_file_path)
        assert hash1 == hash2

        # Different data should produce different hash
        different_data = pd.DataFrame({"col1": [4, 5, 6], "col2": ["d", "e", "f"]})
        different_path = Path(tempfile.mktemp(suffix=".parquet"))
        compressor._save_dataframe(different_data, different_path)

        hash3 = compressor.compute_hash(different_path)
        assert hash1 != hash3

    finally:
        # Clean up temp files
        temp_file_path.unlink(missing_ok=True)
        if "different_path" in locals() and different_path.exists():
            different_path.unlink(missing_ok=True)


def test_compare_datasets() -> None:
    """Test comparing two Parquet datasets."""
    compressor = Compressor()

    # Create temporary Parquet files with test data
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as file1:
        path1 = Path(file1.name)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as file2:
        path2 = Path(file2.name)

    try:
        # Create identical datasets
        df1 = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        df2 = df1.copy()

        compressor._save_dataframe(df1, path1)
        compressor._save_dataframe(df2, path2)

        # Test identical datasets
        result = compressor.compare_datasets(path1, path2)
        assert result["identical"] is True
        assert result["row_count_diff"] == 0
        assert result["column_diff"] == []

        # Test different row count - using concat instead of deprecated append
        df3 = pd.concat([df1, pd.DataFrame({"col1": [4], "col2": ["d"]})], ignore_index=True)

        path3 = Path(tempfile.mktemp(suffix=".parquet"))
        compressor._save_dataframe(df3, path3)

        result = compressor.compare_datasets(path1, path3)
        assert result["identical"] is False
        assert result["row_count_diff"] == -1  # df1 has 1 fewer row than df3

        # Test different columns
        df4 = pd.DataFrame(
            {
                "col1": [1, 2, 3],
                "col3": ["x", "y", "z"],  # Different column name
            }
        )
        path4 = Path(tempfile.mktemp(suffix=".parquet"))
        compressor._save_dataframe(df4, path4)

        result = compressor.compare_datasets(path1, path4)
        assert result["identical"] is False
        assert sorted(result["column_diff"]) == sorted(["col2", "col3"])

    finally:
        # Clean up temp files
        for p in [path1, path2]:
            if p.exists():
                p.unlink(missing_ok=True)
        for path_name_str in ["path3", "path4"]:
            if path_name_str in locals():
                path_to_delete = locals()[path_name_str]
                if path_to_delete.exists():
                    path_to_delete.unlink(missing_ok=True)


def test_decompress() -> None:
    """Test Parquet file decompression to original formats."""
    compressor = Compressor()

    # Test data
    test_df = pd.DataFrame({"col1": [10, 20], "col2": ["x", "y"]})

    # --- Test CSV decompression ---
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_csv_orig:
        original_csv_path = Path(tmp_csv_orig.name)
    test_df.to_csv(original_csv_path, index=False)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_parquet:
        compressed_parquet_path_csv = Path(tmp_parquet.name)

    with tempfile.NamedTemporaryFile(suffix=".restored_csv", delete=False) as tmp_restored_csv:
        restored_csv_path = Path(tmp_restored_csv.name)

    try:
        # Compress CSV to Parquet
        compressor.compress(original_csv_path, compressed_parquet_path_csv)
        assert compressed_parquet_path_csv.exists()

        # Decompress Parquet back to CSV
        compressor.decompress(compressed_parquet_path_csv, restored_csv_path, ".csv")
        assert restored_csv_path.exists()
        assert (
            restored_csv_path.suffix == ".restored_csv"
        )  # Original suffix is preserved by target_restore_path

        # Verify content
        restored_df_csv = pd.read_csv(restored_csv_path)
        pd.testing.assert_frame_equal(restored_df_csv, test_df)

    finally:
        original_csv_path.unlink(missing_ok=True)
        compressed_parquet_path_csv.unlink(missing_ok=True)
        restored_csv_path.unlink(missing_ok=True)

    # --- Test Excel (.xlsx) decompression ---
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp_xlsx_orig:
        original_xlsx_path = Path(tmp_xlsx_orig.name)
    # Need openpyxl for .xlsx
    try:
        test_df.to_excel(original_xlsx_path, index=False, engine="openpyxl")
    except ImportError:
        pytest.skip("openpyxl not installed, skipping Excel test")

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_parquet_xlsx:
        compressed_parquet_path_xlsx = Path(tmp_parquet_xlsx.name)

    with tempfile.NamedTemporaryFile(suffix=".restored_xlsx", delete=False) as tmp_restored_xlsx:
        restored_xlsx_path = Path(tmp_restored_xlsx.name)

    try:
        # Compress XLSX to Parquet
        compressor.compress(original_xlsx_path, compressed_parquet_path_xlsx)
        assert compressed_parquet_path_xlsx.exists()

        # Decompress Parquet back to XLSX
        # The decompress method will ensure the correct suffix based on original_extension
        compressor.decompress(compressed_parquet_path_xlsx, restored_xlsx_path, ".xlsx")

        # The actual written file will have .xlsx suffix
        expected_restored_xlsx_path = restored_xlsx_path.with_suffix(".xlsx")
        assert expected_restored_xlsx_path.exists()

        # Verify content
        restored_df_xlsx = pd.read_excel(expected_restored_xlsx_path)
        pd.testing.assert_frame_equal(restored_df_xlsx, test_df)

    finally:
        original_xlsx_path.unlink(missing_ok=True)
        compressed_parquet_path_xlsx.unlink(missing_ok=True)
        if "expected_restored_xlsx_path" in locals() and expected_restored_xlsx_path.exists():
            expected_restored_xlsx_path.unlink(missing_ok=True)
        # Also clean up the placeholder if it exists and is different
        if restored_xlsx_path.exists() and restored_xlsx_path != expected_restored_xlsx_path:
            restored_xlsx_path.unlink(missing_ok=True)

    # --- Test Parquet decompression (copy) ---
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_pq_orig:
        original_pq_path = Path(tmp_pq_orig.name)
    compressor._save_dataframe(test_df, original_pq_path)  # Save directly as Parquet

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_parquet_pq:
        compressed_parquet_path_pq = Path(
            tmp_parquet_pq.name
        )  # This will be a copy of original_pq_path

    with tempfile.NamedTemporaryFile(suffix=".restored_pq", delete=False) as tmp_restored_pq:
        restored_pq_path = Path(tmp_restored_pq.name)

    try:
        # "Compress" Parquet to Parquet (effectively a read and write by current compress logic)
        compressor.compress(original_pq_path, compressed_parquet_path_pq)
        assert compressed_parquet_path_pq.exists()

        # Decompress Parquet back to Parquet (should be a file copy)
        compressor.decompress(compressed_parquet_path_pq, restored_pq_path, ".parquet")
        assert restored_pq_path.exists()
        assert (
            restored_pq_path.suffix == ".restored_pq"
        )  # Original suffix is preserved by target_restore_path

        # Verify content
        restored_df_pq = pd.read_parquet(restored_pq_path)
        pd.testing.assert_frame_equal(restored_df_pq, test_df)

    finally:
        original_pq_path.unlink(missing_ok=True)
        compressed_parquet_path_pq.unlink(missing_ok=True)
        restored_pq_path.unlink(missing_ok=True)

    # --- Test unsupported extension for decompression ---
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_parquet_unsupported:
        unsupported_source_path = Path(tmp_parquet_unsupported.name)
    compressor._save_dataframe(test_df, unsupported_source_path)

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp_restored_unsupported:
        unsupported_target_path = Path(tmp_restored_unsupported.name)

    try:
        with pytest.raises(ValueError) as excinfo:
            compressor.decompress(unsupported_source_path, unsupported_target_path, ".txt")
        assert "Unsupported original file extension for decompression: .txt" in str(excinfo.value)
    finally:
        unsupported_source_path.unlink(missing_ok=True)
        unsupported_target_path.unlink(missing_ok=True)
