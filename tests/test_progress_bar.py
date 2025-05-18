"""Unit tests for progress bar functionality."""

import os

import pandas as pd
import pytest

from frostbyte.core.compressor import Compressor


class TestProgressTracking:
    """Tests for progress tracking functionality in the decompression process."""

    @pytest.fixture
    def temp_test_files(self, tmp_path):
        """Create temporary test files for compression/decompression tests."""
        # Create test directory and files
        test_dir = tmp_path / "test_progress"
        test_dir.mkdir(exist_ok=True)

        # Create a larger test dataset (25,000 rows) to better demonstrate progress steps
        # This size should be enough to show multiple progress updates
        size = 25000

        # Create a test CSV file with multiple columns to increase size
        test_csv = test_dir / "test_data.csv"
        df = pd.DataFrame(
            {
                "id": range(size),
                "value": [f"value_{i}" for i in range(size)],
                "num1": [i * 1.5 for i in range(size)],
                "num2": [i * 2.5 for i in range(size)],
                "category": [f"cat_{i % 10}" for i in range(size)],
            }
        )
        df.to_csv(test_csv, index=False)

        # Create a test parquet file directly
        test_parquet = test_dir / "test_data.parquet"
        df.to_parquet(test_parquet, index=False)

        yield {"test_dir": test_dir, "test_csv": test_csv, "test_parquet": test_parquet}

        # Clean up (happening after the test completes)
        for file in [test_csv, test_parquet]:
            if file.exists():
                os.unlink(file)

    def test_progress_callback_csv(self, temp_test_files):
        """Test that progress callbacks are called with appropriate values for CSV files."""
        progress_values = []

        def progress_callback(progress):
            progress_values.append(progress)

        # Use the compressor to decompress with progress tracking
        compressor = Compressor()
        result = compressor.decompress(
            temp_test_files["test_parquet"],
            temp_test_files["test_dir"] / "restored.csv",
            ".csv",
            progress_callback,
        )

        # Verify progress reporting
        assert result["success"] is True
        assert len(progress_values) > 3  # Should have multiple progress updates
        assert progress_values[0] <= 0.05  # Should start near 0
        assert progress_values[-1] == 1.0  # Should end at 1.0

        # Progress should be monotonically increasing
        for i in range(1, len(progress_values)):
            assert progress_values[i] >= progress_values[i - 1]

        # Progress should be reasonably distributed (not just jumps from 0 to 1)
        # For small test files, we might not have values in the middle range,
        # but we should still have multiple intermediate steps
        print(f"Progress values: {progress_values}")
        if len(progress_values) < 4:
            pytest.skip("Test file too small for meaningful progress tracking")

        # Either we should have intermediate values OR at least 4 different progress points
        has_intermediate_values = any(0.2 < p < 0.8 for p in progress_values)
        has_multiple_steps = len({round(p, 1) for p in progress_values}) >= 4

        assert has_intermediate_values or has_multiple_steps, (
            "Progress should have intermediate steps"
        )

    def test_progress_callback_excel(self, temp_test_files):
        """Test that progress callbacks are called with appropriate values for Excel files."""
        progress_values = []

        def progress_callback(progress):
            progress_values.append(progress)

        # Use the compressor to decompress with progress tracking
        compressor = Compressor()
        result = compressor.decompress(
            temp_test_files["test_parquet"],
            temp_test_files["test_dir"] / "restored.xlsx",
            ".xlsx",
            progress_callback,
        )

        # Verify progress reporting
        assert result["success"] is True
        assert len(progress_values) > 3  # Should have multiple progress updates
        assert progress_values[0] <= 0.05  # Should start near 0
        assert progress_values[-1] == 1.0  # Should end at 1.0

        # Check for key progress points (should be more than just 0%, 50%, 100%)
        # We're expecting to see values around 0.05, 0.1, values in 0.1-0.7 range,
        # 0.7, 0.8, 0.95, and 1.0
        progress_points = {round(p, 1) for p in progress_values}
        assert len(progress_points) >= 4, "Should have at least 4 distinct progress points"
