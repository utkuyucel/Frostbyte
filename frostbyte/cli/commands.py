"""
Command line interface implementation for Frostbyte.

This module provides the command implementations for the Frostbyte CLI.
"""

import click
from pathlib import Path
from tabulate import tabulate
import sys

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


@cli.command('init')
def init_cmd() -> None:
    """Initialize project, create .frostbyte/ directory. Recreates database if it exists."""
    try:
        # Check if the .frostbyte directory already exists
        if Path('.frostbyte').exists():
            if not click.confirm(
                click.style("⚠️  WARNING: Frostbyte is already initialized. This will delete all archives and reset the database. Continue?", fg="yellow"),
                default=False
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
        click.echo(click.style(f"✗ Error: {str(e)}", fg="red"))
        sys.exit(1)


@cli.command('archive')
@click.argument('path', required=True, type=click.Path(exists=True))
def archive_cmd(path: str) -> None:
    """Compress file, record metadata."""
    try:
        result = frostbyte.archive(path)
        click.echo(click.style(f"✓ Archived: {result['original_path']}", fg="green"))
        click.echo(f"  Version: {result['version']}")
        click.echo(f"  Archive: {result['archive_name']}")
        click.echo(f"  Compression ratio: {result['compression_ratio']:.2f}%")
    except Exception as e:
        click.echo(click.style(f"✗ Error: {str(e)}", fg="red"))
        sys.exit(1)


@cli.command('restore')
@click.argument('path_spec', required=True)
def restore_cmd(path_spec: str) -> None:
    """Decompress and restore original file.
    
    PATH_SPEC can be a file path with an optional version (e.g., data/file.csv@2).
    If no version is specified, the latest version is restored.
    """
    try:
        result = frostbyte.restore(path_spec)
        click.echo(click.style(f"✓ Restored: {result['original_path']}", fg="green"))
        click.echo(f"  Version: {result['version']}")
        click.echo(f"  Timestamp: {result['timestamp']}")
    except Exception as e:
        click.echo(click.style(f"✗ Error: {str(e)}", fg="red"))
        sys.exit(1)


@cli.command('ls')
@click.option('--all', '-a', 'show_all', is_flag=True, help="Show all versions with detailed information (creation date, file sizes, archive filenames)")
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
                original_size = result['original_size_bytes'] / 1024  # KB
                compressed_size = result['compressed_size_bytes'] / 1024  # KB
                size_unit = "KB"
                
                if original_size > 1024:
                    original_size /= 1024  # MB
                    compressed_size /= 1024  # MB
                    size_unit = "MB"
                
                table_data.append([
                    result['original_path'],
                    result['version'],
                    result['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                    f"{original_size:.2f} {size_unit}",
                    f"{compressed_size:.2f} {size_unit}",
                    f"{result.get('compression_ratio', 0):.1f}%",
                    result['archive_filename']
                ])
            click.echo(tabulate(
                table_data,
                headers=["Path", "Ver", "Created", "Orig Size", "Comp Size", "Savings", "Filename"],
                tablefmt="simple"
            ))
        else:
            # Format for showing latest versions with summary information
            table_data = []
            for result in results:
                # Convert sizes to KB or MB for better readability
                total_size = result['total_size_bytes'] / 1024  # KB
                total_compressed = result['total_compressed_bytes'] / 1024  # KB
                size_unit = "KB"
                
                if total_size > 1024:
                    total_size /= 1024  # MB
                    total_compressed /= 1024  # MB
                    size_unit = "MB"
                    
                table_data.append([
                    result['original_path'],
                    result['latest_version'],
                    result['version_count'],
                    result['last_modified'].strftime('%Y-%m-%d %H:%M:%S'),
                    f"{total_size:.2f} {size_unit}",
                    f"{total_compressed:.2f} {size_unit}",
                    f"{result.get('avg_compression', 0):.1f}%"
                ])
            click.echo(tabulate(
                table_data,
                headers=["Path", "Latest Ver", "Total Vers", "Last Modified", "Total Size", "Comp Size", "Avg Savings"],
                tablefmt="simple"
            ))
    except Exception as e:
        click.echo(click.style(f"✗ Error: {str(e)}", fg="red"))
        sys.exit(1)


@cli.command('stats')
def stats_cmd() -> None:
    """Display statistics about archived files."""
    try:
        stats_result = frostbyte.stats()
        click.echo(tabulate(stats_result, headers='keys'))
    except Exception as e:
        click.echo(click.style(f"✗ Error: {str(e)}", fg="red"))
        sys.exit(1)


@cli.command('purge')
@click.argument('file_path', required=True)
@click.option('--all', '-a', 'all_versions', is_flag=True, 
              help="Remove all versions of the file")
def purge_cmd(file_path: str, all_versions: bool) -> None:
    """Remove archive versions or entire file from storage."""
    try:
        result = frostbyte.purge(file_path, all_versions)
        
        if all_versions:
            message = f"Removed all versions of {result['original_path']}"
        else:
            message = f"Removed version {result['version']} of {result['original_path']}"
        
        click.echo(click.style(f"✓ {message}", fg="green"))
        click.echo(f"  Removed {result['count']} archive(s)")
    except Exception as e:
        click.echo(click.style(f"✗ Error: {str(e)}", fg="red"))
        sys.exit(1)



