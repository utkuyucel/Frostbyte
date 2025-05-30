"""Common utility functions and data classes for Frostbyte."""

from dataclasses import dataclass
from functools import lru_cache
from typing import Tuple

KB = 1024
MB = KB * 1024
GB = MB * 1024

CHUNK_THRESHOLDS = (
    (1000, lambda rows: rows),
    (10000, lambda _: 1000),
    (100000, lambda _: 5000),
    (1000000, lambda _: 10000),
    (float("inf"), lambda _: 50000),
)


@dataclass(frozen=True)
class FileSize:
    """Immutable representation of file size."""

    bytes: int

    @property
    def formatted(self) -> Tuple[float, str]:
        """Return formatted size and unit."""
        size_mappings = ((GB, "GB"), (MB, "MB"), (KB, "KB"), (1, "B"))

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
    return 50000


def safe_division(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers, returning default if denominator is zero."""
    return numerator / denominator if denominator != 0 else default
