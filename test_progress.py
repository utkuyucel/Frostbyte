import os
from pathlib import Path

import numpy as np
import pandas as pd

from frostbyte.core.compressor import Compressor


def test_progress():
    # Create a test directory
    test_dir = Path("test_progress")
    test_dir.mkdir(exist_ok=True)

    # Create a test file
    test_file = test_dir / "test_data.csv"
    df = pd.DataFrame(np.random.rand(10000, 5), columns=[f"col_{i}" for i in range(5)])
    df.to_csv(test_file, index=False)

    # Create parquet file
    parquet_file = test_dir / "test_data.parquet"

    # Create compressor
    compressor = Compressor()

    # Define progress callback
    def progress_callback(progress):
        print(f"Progress: {progress * 100:.1f}%", end="\r")
        if progress >= 1.0:
            print("\nCompleted!")

    # Compress and decompress with progress tracking
    compressor.compress(test_file, parquet_file)
    print("Compressing complete, now testing decompression...")

    # Test decompression with progress tracking
    result = compressor.decompress(
        parquet_file, test_dir / "test_data_restored.csv", ".csv", progress_callback
    )

    print(f"Execution time: {result['execution_time']:.2f} seconds")
    print(f"Success: {result['success']}")

    # Clean up
    os.remove(test_file)
    os.remove(parquet_file)
    os.remove(test_dir / "test_data_restored.csv")
    os.rmdir(test_dir)

    print("Test completed successfully!")


if __name__ == "__main__":
    test_progress()
