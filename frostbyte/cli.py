from typing import Optional

import click

from frostbyte.core.manager import ArchiveManager


@click.group()
@click.version_option()
def cli() -> None:
    """Frostbyte: Cold data archiving for pandas workflows."""
    pass


@cli.command()
def init() -> None:
    """Initialize Frostbyte in the current directory (recreates database if it exists)."""
    manager = ArchiveManager()
    manager.initialize()
    click.echo("Initialized Frostbyte in current directory. Database reset to empty state.")


@cli.command()
@click.argument("path", type=click.Path(exists=True))
def archive(path: str) -> None:
    """Archive a CSV or Parquet file."""
    manager = ArchiveManager()
    result = manager.archive(path)
    click.echo(
        f"Archived as {result['archive_name']}, {result['compression_ratio']:.0f}% size saved"
    )


@cli.command()
@click.argument("path", type=str)
@click.option("--version", "-v", type=int, help="Specific version to restore")
def restore(path: str, version: Optional[int] = None) -> None:
    """Restore an archived file to its original location."""
    manager = ArchiveManager()
    result = manager.restore(path, version)
    click.echo(f"Restored {result['original_path']} (version {result['version']})")


@cli.command()
@click.option(
    "--all",
    is_flag=True,
    help="Show all versions with detailed information (dates, sizes, filenames)",
)
def ls(all: bool) -> None:
    """List archived files, optionally showing all versions."""
    manager = ArchiveManager()
    results = manager.list_archives(all)

    if not results:
        click.echo("No archives found.")
        return

    if all:
        # Format table headers and rows
        headers = ["Path", "Version", "Created", "Size (MB)", "Compressed (MB)", "Savings"]
        table_data = []

        for result in results:
            # Convert bytes to MB
            original_size_mb = result.get("original_size_bytes", 0) / (1024 * 1024)
            compressed_size_mb = result.get("compressed_size_bytes", 0) / (1024 * 1024)

            table_data.append(
                [
                    result["original_path"],
                    result["version"],
                    result["timestamp"],
                    f"{original_size_mb:.2f}",
                    f"{compressed_size_mb:.2f}",
                    f"{result.get('compression_ratio', 0):.0f}%",
                ]
            )

        # Print formatted table
        col_widths = [
            max(len(str(row[i])) for row in [*table_data, headers]) for i in range(len(headers))
        ]

        # Print headers
        header_fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
        click.echo(header_fmt.format(*headers))

        # Print separator
        click.echo("  ".join("-" * w for w in col_widths))

        # Print rows
        row_fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
        for row in table_data:
            click.echo(row_fmt.format(*[str(cell) for cell in row]))
    else:
        # Show only latest versions
        headers = ["Path", "Latest", "# Vers", "Last Modified", "Total Size (MB)"]
        table_data = []

        for result in results:
            # Convert bytes to MB
            total_size_mb = result.get("total_size_bytes", 0) / (1024 * 1024)

            table_data.append(
                [
                    result["original_path"],
                    result["latest_version"],
                    result["version_count"],
                    result["last_modified"],
                    f"{total_size_mb:.2f}",
                ]
            )

        # Print formatted table
        col_widths = [
            max(len(str(row[i])) for row in [*table_data, headers]) for i in range(len(headers))
        ]

        # Print headers
        header_fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
        click.echo(header_fmt.format(*headers))

        # Print separator
        click.echo("  ".join("-" * w for w in col_widths))

        # Print rows
        row_fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
        for row in table_data:
            click.echo(row_fmt.format(*[str(cell) for cell in row]))


@cli.command()
@click.argument("file_path", required=False)
def stats(file_path: Optional[str] = None) -> None:
    """Show statistics about archived files."""
    manager = ArchiveManager()
    stats_data = manager.get_stats(file_path)

    if not stats_data:
        click.echo("No archives found.")
        return

    if "total_archives" in stats_data:
        # Global stats
        click.echo(f"Total archives: {stats_data['total_archives']}")
        click.echo(f"Total size: {stats_data['total_size_bytes'] / (1024 * 1024):.2f} MB")
        click.echo(
            f"Total compressed: {stats_data['total_compressed_bytes'] / (1024 * 1024):.2f} MB"
        )
        click.echo(f"Size saved: {stats_data['total_size_saved'] / (1024 * 1024):.2f} MB")
        click.echo(f"Average compression ratio: {stats_data['avg_compression_ratio']:.0f}%")
    else:
        click.echo(f"File: {stats_data['original_path']}")
        click.echo(f"Versions: {stats_data['versions']}")
        click.echo(f"Latest version: {stats_data['latest_version']}")
        click.echo(f"Size saved: {stats_data['size_saved']:.2f} MB")
        click.echo(f"Last modified: {stats_data['last_modified']}")


@cli.command()
@click.argument("file", type=str)
@click.option("--version", "-v", type=int, help="Specific version to purge")
@click.option("--all", is_flag=True, help="Remove all versions")
def purge(file: str, version: Optional[int] = None, all: bool = False) -> None:
    """Remove archive versions or entire file from storage."""
    manager = ArchiveManager()
    result = manager.purge(file, version, all_versions=all)

    if all:
        click.echo(f"Removed all versions of {result['original_path']}")
    else:
        click.echo(f"Removed version {result['version']} of {result['original_path']}")
