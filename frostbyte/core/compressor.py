"""
Compression funct    def compress(self, source_path: Union[str, Path], target_path: Union[str, Path]) -> int:
        """Compress a file using Zstandard and return compressed file size in bytes."""r Frostbyte.

Uses Zstandard (zstd) for high-ratio compression of data files.
"""

import os
from pathlib import Path
from typing import Union

import zstandard as zstd


class Compressor:
    """Handles compression and decompression of data files."""
    
    def __init__(self, compression_level: int = 3, chunk_size: int = 1024 * 1024):
        """Initialize the compressor with compression level and chunk size."""
        self.compression_level = compression_level
        self.chunk_size = chunk_size
    
    def compress(self, source_path: Union[str, Path], target_path: Union[str, Path]) -> int:
        """
        Compress a file using Zstandard.
        
        Args:
            source_path: Path to the source file
            target_path: Path where the compressed file will be written
            
        Returns:
            int: Size of the compressed file in bytes
        """
        source_path = Path(source_path)
        target_path = Path(target_path)
        
        cctx = zstd.ZstdCompressor(level=self.compression_level)
        
        with open(source_path, 'rb') as source, open(target_path, 'wb') as target:
            compressor = cctx.stream_writer(target)
            
            while True:
                chunk = source.read(self.chunk_size)
                if not chunk:
                    break
                compressor.write(chunk)
            
            compressor.flush()
        
        return target_path.stat().st_size
    
    def decompress(self, source_path: Union[str, Path], target_path: Union[str, Path]) -> int:
        """Decompress a file compressed with Zstandard and return decompressed file size."""
        source_path = Path(source_path)
        target_path = Path(target_path)
        
        # Create directories if they don't exist
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        dctx = zstd.ZstdDecompressor()
        
        with open(source_path, 'rb') as source, open(target_path, 'wb') as target:
            decompressor = dctx.stream_writer(target)
            
            while True:
                chunk = source.read(self.chunk_size)
                if not chunk:
                    break
                decompressor.write(chunk)
            
            decompressor.flush()
        
        return target_path.stat().st_size
