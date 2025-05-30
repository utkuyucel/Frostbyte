"""Common utility functions and data classes for Frostbyte."""

from dataclasses import dataclass
from functools import lru_cache
from typing import Tuple

# Constants for byte conversions (DRY principle)
KB = 1024
MB = KB * 1024
GB = MB * 1024

# Chunk size thresholds and corresponding sizes
CHUNK_THRESHOLDS = (
    (1000, lambda rows: rows),      # Small datasets: use all rows
    (10000, lambda _: 1000),        # Small-medium: 1K chunks
    (100000, lambda _: 5000),       # Medium: 5K chunks
    (1000000, lambda _: 10000),     # Large: 10K chunks
    (float('inf'), lambda _: 50000) # Very large: 50K chunks
)


@dataclass(frozen=True)
class FileSize:
    """Immutable representation of file size."""
    
    bytes: int
    
    @property
    def formatted(self) -> Tuple[float, str]:
        """Return formatted size and unit."""
        size_mappings = (
            (GB, "GB"),
            (MB, "MB"),
            (KB, "KB"),
            (1, "B")
        )
        
        for threshold, unit in size_mappings:
            if self.bytes >= threshold:
                return self.bytes / threshold, unit
        return float(self.bytes), "B"
    
    def __str__(self) -> str:
        value, unit = self.formatted
        return f"{value:.2f} {unit}"


@lru_cache(maxsize=128)
def format_file_size(size_bytes: int) -> str:
    """Format file size in human readable format with caching."""
    return str(FileSize(size_bytes))


def determine_chunk_size(estimated_rows: int) -> int:
    """Determine optimal chunk size based on estimated row count using threshold mapping."""
    for threshold, chunk_func in CHUNK_THRESHOLDS:
        if estimated_rows < threshold:
            return chunk_func(estimated_rows)
    return 50000  # Fallback (should never reach here)


def safe_division(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers, returning default if denominator is zero."""
    return numerator / denominator if denominator != 0 else default
