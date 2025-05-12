"""
Command line interface implementation for Frostbyte.

This module provides the command implementations for the Frostbyte CLI.
"""

import click
from tabulate import tabulate
from datetime import datetime
import sys

import frostbyte


@click.group()
@click.version_option(version=frostbyte.__version__)
def cli():
    """
    Frostbyte: Cold Data Archiving for Pandas Workflows.

    A lightweight, local-first cold data archiving tool for managing
    large, infrequently accessed datasets.
    """
    pass


@cli.command('init')
def init_cmd():
    """Initialize project, create .frostbyte/ directory."""
    try:
        result = frostbyte.init()
        if result:
            click.echo(click.style("✓ Frostbyte initialized successfully", fg="green"))
        else:
            click.echo(click.style("✗ Failed to initialize Frostbyte", fg="red"))
            sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"✗ Error: {str(e)}", fg="red"))
        sys.exit(1)


@cli.command('archive')
@click.argument('path', required=True, type=click.Path(exists=True))
def archive_cmd(path):
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
def restore_cmd(path_spec):
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
@click.option('--all', '-a', 'show_all', is_flag=True, help="Show all versions")
def list_cmd(show_all):
    """List archived files and versions."""
    try:
        results = frostbyte.ls(show_all)
        
        if not results:
            click.echo("No archives found.")
            return
        
        if show_all:
            # Format for showing all versions
            table_data = []
            for result in results:
                table_data.append([
                    result['original_path'],
                    result['version'],
                    result['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                    f"{result.get('compression_ratio', 0):.2f}%"
                ])
            click.echo(tabulate(
                table_data,
                headers=["Path", "Version", "Timestamp", "Compression"],
                tablefmt="simple"
            ))
        else:
            # Format for showing latest versions
            table_data = []
            for result in results:
                table_data.append([
                    result['original_path'],
                    result['latest_version'],
                    result['version_count']
                ])
            click.echo(tabulate(
                table_data,
                headers=["Path", "Latest Version", "Total Versions"],
                tablefmt="simple"
            ))
    except Exception as e:
        click.echo(click.style(f"✗ Error: {str(e)}", fg="red"))
        sys.exit(1)


@cli.command('stats')
@click.argument('file_path', required=False)
def stats_cmd(file_path):
    """Show size savings, last access, total versions."""
    try:
        result = frostbyte.stats(file_path)
        
        if not result:
            click.echo("No statistics available.")
            return
        
        if file_path:
            # Stats for a specific file
            click.echo(f"Statistics for: {result['original_path']}")
            click.echo(f"  Versions: {result['versions']}")
            click.echo(f"  Latest version: {result['latest_version']}")
            click.echo(f"  Last modified: {result['last_modified'].strftime('%Y-%m-%d %H:%M:%S')}")
            click.echo(f"  Size saved: {result['size_saved'] / 1024 / 1024:.2f} MB")
        else:
            # Overall stats
            click.echo("Overall Statistics:")
            click.echo(f"  Total archives: {result['total_archives']}")
            click.echo(f"  Total size saved: {result['total_size_saved']:.2f} MB")
            click.echo(f"  Average compression ratio: {result['avg_compression_ratio']:.2f}%")
    except Exception as e:
        click.echo(click.style(f"✗ Error: {str(e)}", fg="red"))
        sys.exit(1)


@cli.command('diff')
@click.argument('file_a', required=True)
@click.argument('file_b', required=True)
def diff_cmd(file_a, file_b):
    """Show row/column-level diffs between two versions."""
    try:
        result = frostbyte.diff(file_a, file_b)
        
        click.echo(f"Comparing {file_a} and {file_b}:")
        click.echo(f"  Rows added: {result['rows_added']}")
        click.echo(f"  Rows removed: {result['rows_removed']}")
        click.echo(f"  Rows modified: {result['rows_modified']}")
        
        if result['schema_changes']:
            click.echo("\nSchema changes:")
            for change in result['schema_changes']:
                click.echo(f"  - {change}")
    except Exception as e:
        click.echo(click.style(f"✗ Error: {str(e)}", fg="red"))
        sys.exit(1)


@cli.command('purge')
@click.argument('file_path', required=True)
@click.option('--all', '-a', 'all_versions', is_flag=True, 
              help="Remove all versions of the file")
def purge_cmd(file_path, all_versions):
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


@cli.command('gui')
def gui_cmd():
    """Launch optional Streamlit GUI (coming in v0.2)."""
    click.echo(click.style("The GUI interface is coming in v0.2", fg="yellow"))
    click.echo("This feature is not yet implemented.")
