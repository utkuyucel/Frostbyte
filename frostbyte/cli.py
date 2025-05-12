"""
Command-line interface for Frostbyte.
"""

import click
from typing import Optional

from frostbyte.core.manager import ArchiveManager


@click.group()
@click.version_option()
def cli():
    """
    Frostbyte: Cold data archiving for pandas workflows.
    
    A lightweight, local-first tool for efficient compression, versioning,
    and management of large, infrequently accessed datasets.
    """
    pass


@cli.command()
def init():
    """Initialize Frostbyte in the current directory (recreates database if it exists)."""
    manager = ArchiveManager()
    manager.initialize()
    click.echo("Initialized Frostbyte in current directory. Database reset to empty state.")


@cli.command()
@click.argument('path', type=click.Path(exists=True))
def archive(path: str):
    """Archive a CSV or Parquet file."""
    manager = ArchiveManager()
    result = manager.archive(path)
    click.echo(f"Archived as {result['archive_name']}, {result['compression_ratio']:.0f}% size saved")


@cli.command()
@click.argument('path', type=str)
def restore(path: str):
    """Restore an archived file to its original location."""
    manager = ArchiveManager()
    result = manager.restore(path)
    click.echo(f"Restored {result['original_path']} (version {result['version']})")


@cli.command()
@click.option('--all', is_flag=True, help='Show all versions with detailed information (creation date, file sizes, archive filenames)')
def ls(all: bool):
    """List archived files and versions.
    
    Without --all: Shows summary with latest version and total stats.
    With --all: Shows detailed information for each version.
    """
    manager = ArchiveManager()
    archives = manager.list_archives(show_all=all)
    
    if not archives:
        click.echo("No archives found.")
        return
        
    for archive in archives:
        if all:
            # Convert sizes to KB or MB for better readability
            original_size = archive['original_size_bytes'] / 1024
            compressed_size = archive['compressed_size_bytes'] / 1024
            size_unit = "KB"
            
            if original_size > 1024:
                original_size /= 1024
                compressed_size /= 1024
                size_unit = "MB"
            
            click.echo(f"{archive['original_path']}@{archive['version']} - {archive['archive_filename']}")
            click.echo(f"  Created: {archive['timestamp']} - Size: {original_size:.2f} {size_unit} → {compressed_size:.2f} {size_unit} ({archive['compression_ratio']:.1f}% saved)")
            click.echo(f"  Rows: {archive['row_count']}")
        else:
            last_version = archive['latest_version']
            total_versions = archive['version_count']
            
            # Convert sizes to KB or MB for better readability
            total_size = archive['total_size_bytes'] / 1024
            total_compressed = archive['total_compressed_bytes'] / 1024
            size_unit = "KB"
            
            if total_size > 1024:
                total_size /= 1024
                total_compressed /= 1024
                size_unit = "MB"
                
            click.echo(f"{archive['original_path']} - Version {last_version} of {total_versions}")
            click.echo(f"  Last modified: {archive['last_modified']}")
            click.echo(f"  Total size: {total_size:.2f} {size_unit} → {total_compressed:.2f} {size_unit} ({archive['avg_compression']:.1f}% saved)")


@cli.command()
@click.argument('file', type=str, required=False)
def stats(file: Optional[str] = None):
    """Show size savings, last access, total versions."""
    manager = ArchiveManager()
    stats_data = manager.get_stats(file_path=file)
    
    if not file:
        click.echo(f"Total archives: {stats_data['total_archives']}")
        click.echo(f"Total size saved: {stats_data['total_size_saved']:.2f} MB")
        click.echo(f"Average compression ratio: {stats_data['avg_compression_ratio']:.0f}%")
    else:
        click.echo(f"File: {stats_data['original_path']}")
        click.echo(f"Versions: {stats_data['versions']}")
        click.echo(f"Latest version: {stats_data['latest_version']}")
        click.echo(f"Size saved: {stats_data['size_saved']:.2f} MB")
        click.echo(f"Last modified: {stats_data['last_modified']}")


@cli.command()
@click.argument('file_a', type=str)
@click.argument('file_b', type=str)
def diff(file_a: str, file_b: str):
    """Show row/column-level diffs between two versions."""
    manager = ArchiveManager()
    diff_result = manager.diff_files(file_a, file_b)
    
    click.echo(f"Comparing {file_a} with {file_b}")
    click.echo(f"- Rows added: {diff_result['rows_added']}")
    click.echo(f"- Rows removed: {diff_result['rows_removed']}")
    click.echo(f"- Rows modified: {diff_result['rows_modified']}")
    
    if diff_result['schema_changes']:
        click.echo("Schema changes:")
        for change in diff_result['schema_changes']:
            click.echo(f"- {change}")


@cli.command()
@click.argument('file', type=str)
@click.option('--all', is_flag=True, help='Remove all versions')
def purge(file: str, all: bool):
    """Remove archive versions or entire file from storage."""
    manager = ArchiveManager()
    result = manager.purge(file, all_versions=all)
    
    if all:
        click.echo(f"Removed all versions of {result['original_path']}")
    else:
        click.echo(f"Removed version {result['version']} of {result['original_path']}")


if __name__ == '__main__':
    cli()
