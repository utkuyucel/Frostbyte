"""
Command line interface implementation for Frostbyte.

This module provides the command implementations for the Frostbyte CLI.
"""

import sys
import time
from pathlib import Path
from typing import Optional

import click
from tabulate import tabulate

import frostbyte


@click.group()
@click.version_option(version=frostbyte.__version__)
def cli() -> None:
    """
    Frostbyte: Cold Data Archiving for Pandas Workflows.

    A lightweight, local-first cold data archiving tool for managing
    large, infrequently accessed datasets.
    """
    pass


@cli.command("init")
def init_cmd() -> None:
    """Initialize project, create .frostbyte/ directory. Recreates database if it exists."""
    try:
        # Check if the .frostbyte directory already exists
        if Path(".frostbyte").exists() and not click.confirm(
            click.style(
                "⚠️  WARNING: Reset existing Frostbyte database?",
                fg="yellow",
            ),
            default=False,
        ):
            click.echo(click.style("Initialization aborted", fg="blue"))
            return

        result = frostbyte.init()
        if result:
            click.echo(click.style("✓ Frostbyte initialized successfully", fg="green"))
            click.echo(click.style("  Database reset to empty state", fg="blue"))
        else:
            click.echo(click.style("✗ Failed to initialize Frostbyte", fg="red"))
            sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"✗ Error: {e!s}", fg="red"))
        sys.exit(1)


@cli.command("archive")
@click.argument("path", required=True, type=click.Path(exists=True))
def archive_cmd(path: str) -> None:
    """Compress file, record metadata."""
    try:
        result = frostbyte.archive(path)

        # Format file sizes for display
        def format_size(size_bytes: float) -> str:
            if size_bytes >= 1024 * 1024 * 1024:  # GB
                return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
            if size_bytes >= 1024 * 1024:  # MB
                return f"{size_bytes / (1024 * 1024):.2f} MB"
            if size_bytes >= 1024:  # KB
                return f"{size_bytes / 1024:.2f} KB"
            # For smaller sizes, round to nearest integer
            return f"{round(size_bytes)} bytes"

        # Get file sizes from result
        original_size = result.get("original_size", 0)
        compressed_size = result.get("compressed_size", 0)

        click.echo(click.style(f"✓ Archived: {result['original_path']}", fg="green"))
        click.echo(f"  Version: {result['version']}")
        click.echo(f"  Archive: {result['archive_name']}")
        click.echo(f"  Original size: {format_size(original_size)}")
        click.echo(f"  Compressed size: {format_size(compressed_size)}")
        click.echo(f"  Compression ratio: {result['compression_ratio']:.2f}%")
    except Exception as e:
        click.echo(click.style(f"✗ Error: {e!s}", fg="red"))
        sys.exit(1)


@cli.command("restore")
@click.argument("path_spec", required=True)
@click.option("--version", "-v", type=int, help="Specific version to restore")
def restore_cmd(path_spec: str, version: Optional[int] = None) -> None:
    """Decompress and restore original file.

    PATH_SPEC can be:
    - A file path (e.g., data/file.csv)
    - An archive filename (e.g., customer_data_v1.parquet)
    - A partial filename to search for (e.g., customer_data)

    You can specify a version using the --version/-v option.
    If no version is specified, the latest version is restored.
    When using a partial name, if multiple files match, you'll be asked to be more specific.
    """
    try:
        # Define a function to format file size
        def format_progress_size(size_bytes: float) -> str:
            """Format bytes to human-readable size."""
            if size_bytes >= 1024**3:  # GB
                return f"{size_bytes / 1024**3:.2f} GB"
            if size_bytes >= 1024**2:  # MB
                return f"{size_bytes / 1024**2:.2f} MB"
            if size_bytes >= 1024:  # KB
                return f"{size_bytes / 1024:.2f} KB"
            return f"{size_bytes} bytes"

        # Setup for progress tracking
        progress_bar = None
        start_time = time.time()
        last_update_time = 0.0  # Use float for time consistency
        estimated_size = 0  # Will be updated once we have info

        def progress_callback(progress: float) -> None:
            """Progress callback for the restore operation with enhanced visual feedback."""
            nonlocal progress_bar, start_time, last_update_time, estimated_size
            current_time = time.time()

            # Initialize the progress bar on first call
            if progress_bar is None:
                # Create a more visually appealing progress bar
                progress_bar = click.progressbar(
                    length=100,
                    label="Decompressing",
                    fill_char="█",  # Solid block for filled portion
                    empty_char="░",  # Light shade for empty portion
                    show_pos=True,
                    show_percent=True,
                    bar_template="%(label)s [%(bar)s] %(info)s",
                )

            # Convert from 0-1 to 0-100 scale
            current = int(progress * 100)

            # Update the progress bar if:
            # 1. Progress has increased, and
            # 2. Either it's been at least 0.1 seconds since last update OR
            #    progress increased by at least 2%
            enough_time_passed = current_time - last_update_time > 0.1
            enough_progress = current - progress_bar.pos >= 2
            progress_increased = progress_bar.pos < current
            update_needed = progress_increased and (enough_time_passed or enough_progress)

            if update_needed:
                # Calculate and display estimated time remaining
                elapsed = current_time - start_time
                # Only estimate after some progress to avoid wild initial estimates
                if progress > 0.05:
                    estimated_total = elapsed / progress
                    remaining = estimated_total - elapsed

                    # Round the seconds appropriately
                    # Use ternary operator for time string format
                    time_str = f"{remaining / 60:.1f}m" if remaining > 60 else f"{remaining:.1f}s"

                    # Update progress with time info
                    info = f"ETA: {time_str}"
                    progress_bar.label = f"Decompressing ({info})"

                # Update the progress bar position
                progress_bar.update(current - progress_bar.pos)
                last_update_time = current_time

            # Handle completion
            if progress >= 1.0 and progress_bar is not None:
                total_time = time.time() - start_time
                # Use ternary operator for completion time string
                time_str = (
                    f"{total_time / 60:.1f} minutes"
                    if total_time >= 60
                    else f"{total_time:.2f} seconds"
                )
                progress_bar.label = f"Decompressed in {time_str}"
                progress_bar.finish()

        # Start timer
        start_time = time.time()

        # Call restore with the progress callback
        result = frostbyte.restore(path_spec, version, progress_callback)

        # Format file sizes for display
        def format_size(size_bytes: float) -> str:
            if size_bytes >= 1024 * 1024 * 1024:  # GB
                return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
            if size_bytes >= 1024 * 1024:  # MB
                return f"{size_bytes / (1024 * 1024):.2f} MB"
            if size_bytes >= 1024:  # KB
                return f"{size_bytes / 1024:.2f} KB"
            # For smaller sizes, round to nearest integer
            return f"{round(size_bytes)} bytes"

        original_size = result.get("original_size", 0)
        compressed_size = result.get("compressed_size", 0)

        # Get execution time either from result or by calculating it
        execution_time = result.get("execution_time", time.time() - start_time)

        click.echo(click.style(f"✓ Restored: {result['original_path']}", fg="green"))
        click.echo(f"  Version: {result['version']}")
        click.echo(f"  Timestamp: {result['timestamp']}")
        click.echo(f"  Original size: {format_size(original_size)}")
        click.echo(f"  Compressed size: {format_size(compressed_size)}")
        click.echo(f"  Compression ratio: {result.get('compression_ratio', 0):.1f}%")
        click.echo(f"  Restore time: {execution_time:.2f} seconds")
    except Exception as e:
        click.echo(click.style(f"✗ Error: {e!s}", fg="red"))
        sys.exit(1)


@cli.command("ls")
@click.option(
    "--all",
    "-a",
    "show_all",
    is_flag=True,
    help="Show all versions with detailed information (dates, sizes, filenames)",
)
def list_cmd(show_all: bool) -> None:
    """List archived files and versions.

    Without --all: Shows summary information with latest version and total stats.
    With --all: Shows detailed information for each version (creation date, size, filename).
    """
    try:
        results = frostbyte.ls(show_all)

        if not results:
            click.echo("No archives found.")
            return

        if show_all:
            # Format for showing all versions with detailed information
            table_data = []
            for result in results:
                # Convert sizes to KB or MB for better readability
                original_size = result["original_size_bytes"] / 1024  # KB
                compressed_size = result["compressed_size_bytes"] / 1024  # KB
                size_unit = "KB"

                if original_size > 1024:
                    original_size /= 1024  # MB
                    compressed_size /= 1024  # MB
                    size_unit = "MB"

                table_data.append(
                    [
                        result["original_path"],
                        result["version"],
                        result["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                        f"{original_size:.2f} {size_unit}",
                        f"{compressed_size:.2f} {size_unit}",
                        f"{result.get('compression_ratio', 0):.1f}%",
                        result["archive_filename"],
                    ]
                )
            click.echo(
                tabulate(
                    table_data,
                    headers=[
                        "Path",
                        "Ver",
                        "Created",
                        "Orig Size",
                        "Comp Size",
                        "Savings",
                        "Filename",
                    ],
                    tablefmt="simple",
                )
            )
        else:
            # Format for showing latest versions with summary information
            table_data = []
            for result in results:
                # Convert sizes to KB or MB for better readability
                total_size = result["total_size_bytes"] / 1024  # KB
                total_compressed = result["total_compressed_bytes"] / 1024  # KB
                size_unit = "KB"

                if total_size > 1024:
                    total_size /= 1024  # MB
                    total_compressed /= 1024  # MB
                    size_unit = "MB"

                table_data.append(
                    [
                        result["original_path"],
                        result["latest_version"],
                        result["version_count"],
                        result["last_modified"].strftime("%Y-%m-%d %H:%M:%S"),
                        f"{total_size:.2f} {size_unit}",
                        f"{total_compressed:.2f} {size_unit}",
                        f"{result.get('avg_compression', 0):.1f}%",
                    ]
                )
            click.echo(
                tabulate(
                    table_data,
                    headers=[
                        "Path",
                        "Latest Ver",
                        "Total Vers",
                        "Last Modified",
                        "Total Size",
                        "Comp Size",
                        "Avg Savings",
                    ],
                    tablefmt="simple",
                )
            )
    except Exception as e:
        click.echo(click.style(f"✗ Error: {e!s}", fg="red"))
        sys.exit(1)


@cli.command("stats")
@click.argument("file_path", required=False)
def stats_cmd(file_path: Optional[str] = None) -> None:
    """Display statistics about archived files.

    Optional: provide a file path to see stats for a specific file.
    """
    try:
        stats_result = frostbyte.stats(file_path)
        if stats_result:
            # Format size values for better readability
            for key in stats_result:
                if "size" in key.lower() and isinstance(stats_result[key], (int, float)):
                    size_bytes = stats_result[key]
                    if size_bytes > 1024 * 1024 * 1024:  # More than 1 GB
                        stats_result[key] = f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
                    elif size_bytes > 1024 * 1024:  # More than 1 MB
                        stats_result[key] = f"{size_bytes / (1024 * 1024):.2f} MB"
                    elif size_bytes > 1024:  # More than 1 KB
                        stats_result[key] = f"{size_bytes / 1024:.2f} KB"
                    else:
                        stats_result[key] = f"{size_bytes:.0f} bytes"

            # Rename keys for better readability
            if "total_size_saved" in stats_result:
                stats_result["Total Size Saved"] = stats_result.pop("total_size_saved")
            if "total_archives" in stats_result:
                stats_result["Total Archives"] = stats_result.pop("total_archives")
            if "avg_compression_ratio" in stats_result:
                stats_result["Avg Compression"] = (
                    f"{stats_result.pop('avg_compression_ratio'):.1f}%"
                )
            if "size_saved" in stats_result:
                stats_result["Size Saved"] = stats_result.pop("size_saved")

            click.echo(click.style("✓ Archive Statistics:", fg="green"))
            click.echo(tabulate([stats_result], headers="keys"))
        else:
            click.echo("No archives found.")
    except Exception as e:
        click.echo(click.style(f"✗ Error: {e!s}", fg="red"))
        sys.exit(1)


@cli.command("purge")
@click.argument("file_path", required=True)
@click.option("--version", "-v", type=int, help="Specific version to purge")
@click.option("--all", "-a", "all_versions", is_flag=True, help="Remove all versions of the file")
def purge_cmd(file_path: str, version: Optional[int] = None, all_versions: bool = False) -> None:
    """Remove archive versions or entire file from storage."""
    try:
        result = frostbyte.purge(file_path, version, all_versions)

        if all_versions:
            message = f"Removed all versions of {result['original_path']}"
        else:
            message = f"Removed version {result['version']} of {result['original_path']}"

        click.echo(click.style(f"✓ {message}", fg="green"))
        click.echo(f"  Removed {result['count']} archive(s)")
    except Exception as e:
        click.echo(click.style(f"✗ Error: {e!s}", fg="red"))
        sys.exit(1)
