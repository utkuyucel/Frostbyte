import logging
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
    """Frostbyte: Cold Data Archiving for Pandas Workflows."""
    pass


@cli.command("init")
def init_cmd() -> None:
    """Initialize project, create .frostbyte/ directory. Recreates database if it exists."""
    try:
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
        progress_bar = None
        start_time = time.time()
        last_update_time = 0.0

        compressor_logger = logging.getLogger("frostbyte.compressor")
        compressor_logger.setLevel(logging.WARNING)

        def progress_callback(progress: float) -> None:
            nonlocal progress_bar, start_time, last_update_time
            current_time = time.time()

            if progress_bar is None:
                progress_bar = click.progressbar(
                    length=100,
                    label="Archiving",
                    fill_char="█",
                    empty_char="░",
                    show_pos=True,
                    show_percent=True,
                    bar_template="%(label)s [%(bar)s] %(info)s",
                )

            current = int(progress * 100)

            enough_time_passed = current_time - last_update_time > 0.1
            enough_progress = current - progress_bar.pos >= 2
            progress_increased = progress_bar.pos < current
            update_needed = progress_increased and (enough_time_passed or enough_progress)

            if update_needed:
                progress_bar.label = "Archiving"
                progress_bar.update(current - progress_bar.pos)
                last_update_time = current_time

            if progress >= 1.0 and progress_bar is not None:
                total_time = time.time() - start_time
                time_str = (
                    f"{total_time / 60:.1f} minutes"
                    if total_time >= 60
                    else f"{total_time:.2f} seconds"
                )
                progress_bar.label = f"Archived in {time_str}"
                progress_bar.finish()

        try:
            result = frostbyte.archive(path, quiet=True, progress_callback=progress_callback)
        finally:
            compressor_logger = logging.getLogger("frostbyte.compressor")
            compressor_logger.setLevel(logging.INFO)

        def format_size(size_bytes: float) -> str:
            if size_bytes >= 1024 * 1024 * 1024:
                return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
            if size_bytes >= 1024 * 1024:
                return f"{size_bytes / (1024 * 1024):.2f} MB"
            if size_bytes >= 1024:
                return f"{size_bytes / 1024:.2f} KB"
            return f"{round(size_bytes)} bytes"

        original_size = result.get("original_size", 0)
        compressed_size = result.get("compressed_size", 0)

        click.echo(click.style(f"\n✓ Archived: {result['original_path']}", fg="green"))
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

        def format_progress_size(size_bytes: float) -> str:
            if size_bytes >= 1024**3:
                return f"{size_bytes / 1024**3:.2f} GB"
            if size_bytes >= 1024**2:
                return f"{size_bytes / 1024**2:.2f} MB"
            if size_bytes >= 1024:
                return f"{size_bytes / 1024:.2f} KB"
            return f"{size_bytes} bytes"

        progress_bar = None
        start_time = time.time()
        last_update_time = 0.0
        estimated_size = 0

        compressor_logger = logging.getLogger("frostbyte.compressor")
        compressor_logger.setLevel(logging.WARNING)

        def progress_callback(progress: float) -> None:
            nonlocal progress_bar, start_time, last_update_time, estimated_size
            current_time = time.time()

            if progress_bar is None:
                progress_bar = click.progressbar(
                    length=100,
                    label="Decompressing",
                    fill_char="█",
                    empty_char="░",
                    show_pos=True,
                    show_percent=True,
                    bar_template="%(label)s [%(bar)s] %(info)s",
                )

            current = int(progress * 100)

            enough_time_passed = current_time - last_update_time > 0.1
            enough_progress = current - progress_bar.pos >= 2
            progress_increased = progress_bar.pos < current
            update_needed = progress_increased and (enough_time_passed or enough_progress)

            if update_needed:
                progress_bar.label = "Decompressing"
                progress_bar.update(current - progress_bar.pos)
                last_update_time = current_time

            if progress >= 1.0 and progress_bar is not None:
                total_time = time.time() - start_time
                time_str = (
                    f"{total_time / 60:.1f} minutes"
                    if total_time >= 60
                    else f"{total_time:.2f} seconds"
                )
                progress_bar.label = f"Decompressed in {time_str}"
                progress_bar.finish()

        start_time = time.time()

        try:
            result = frostbyte.restore(path_spec, version, progress_callback)
        finally:
            compressor_logger = logging.getLogger("frostbyte.compressor")
            compressor_logger.setLevel(logging.INFO)

        def format_size(size_bytes: float) -> str:
            if size_bytes >= 1024 * 1024 * 1024:
                return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
            if size_bytes >= 1024 * 1024:
                return f"{size_bytes / (1024 * 1024):.2f} MB"
            if size_bytes >= 1024:
                return f"{size_bytes / 1024:.2f} KB"
            return f"{round(size_bytes)} bytes"

        original_size = result.get("original_size", 0)
        compressed_size = result.get("compressed_size", 0)
        execution_time = result.get("execution_time", time.time() - start_time)

        click.echo(click.style(f"\n✓ Restored: {result['original_path']}", fg="green"))
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
            table_data = []
            for result in results:
                original_size = result["original_size_bytes"] / 1024
                compressed_size = result["compressed_size_bytes"] / 1024
                size_unit = "KB"

                if original_size > 1024:
                    original_size /= 1024
                    compressed_size /= 1024
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
            table_data = []
            for result in results:
                total_size = result["total_size_bytes"] / 1024
                total_compressed = result["total_compressed_bytes"] / 1024
                size_unit = "KB"

                if total_size > 1024:
                    total_size /= 1024
                    total_compressed /= 1024
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
            for key in stats_result:
                if "size" in key.lower() and isinstance(stats_result[key], (int, float)):
                    size_bytes = stats_result[key]
                    if size_bytes > 1024 * 1024 * 1024:
                        stats_result[key] = f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
                    elif size_bytes > 1024 * 1024:
                        stats_result[key] = f"{size_bytes / (1024 * 1024):.2f} MB"
                    elif size_bytes > 1024:
                        stats_result[key] = f"{size_bytes / 1024:.2f} KB"
                    else:
                        stats_result[key] = f"{size_bytes:.0f} bytes"

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
