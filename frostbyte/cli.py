import sys
from typing import TYPE_CHECKING, List, Optional

import click

from frostbyte.core.manager import ArchiveManager

if TYPE_CHECKING:
    from frostbyte.core.validation import ArchiveValidator


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
    if all:
        # Show all versions - pass None to get all files, then get detailed view
        all_files = manager.list_archives()
        results = []
        for file_summary in all_files:
            file_path = file_summary["original_path"]
            file_results = manager.list_archives(file_name=file_path)
            results.extend(file_results)
    else:
        results = manager.list_archives()

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


@cli.command()
@click.argument("file_path", required=False)
@click.option("--version", "-v", type=int, help="Specific version to verify")
@click.option(
    "--level",
    type=click.Choice(["fast", "medium", "thorough", "full"]),
    default="medium",
    help="Validation thoroughness level",
)
@click.option(
    "--sample-rate", type=float, default=0.1, help="Sampling rate for row validation (0.0-1.0)"
)
def verify(
    file_path: Optional[str], version: Optional[int], level: str, sample_rate: float
) -> None:
    """Verify archive integrity and detect corruption."""
    from frostbyte.core.validation import ArchiveValidator

    manager = ArchiveManager()
    validator = ArchiveValidator(manager.store, manager.archives_dir)

    # Define validation levels
    validation_map = {
        "fast": ["hash"],
        "medium": ["hash"],
        "thorough": ["hash", "rows"],
        "full": ["hash", "rows"],
    }

    if file_path:
        # Validate specific file
        _validate_single_file(validator, file_path, version, validation_map[level], sample_rate)
    else:
        # Validate all archives
        _validate_all_archives(validator, validation_map[level], sample_rate)


def _validate_single_file(
    validator: "ArchiveValidator",
    file_path: str,
    version: Optional[int],
    checks: List[str],
    sample_rate: float,
) -> None:
    """Validate a single file with specified checks."""
    click.echo(f"Validating {file_path}" + (f" (v{version})" if version else ""))
    click.echo()

    all_valid = True
    total_errors = 0
    total_warnings = 0

    for check in checks:
        click.echo(f"Running {check} validation...", nl=False)

        try:
            if check == "hash":
                result = validator.validate_content_hash(file_path, version)
            elif check == "rows":
                result = validator.validate_row_integrity(file_path, version, sample_rate)
            else:
                continue

            # Display results
            status = "[PASS]" if result.is_valid else "[FAIL]"
            click.echo(f" {status}")

            if result.errors:
                for error in result.errors:
                    click.echo(f"    ERROR: {error}", err=True)
                total_errors += len(result.errors)

            if result.warnings:
                for warning in result.warnings:
                    click.echo(f"    WARNING: {warning}")
                total_warnings += len(result.warnings)

            # Show useful details for hash validation
            if check == "hash" and result.details and result.details.get("hashes_match"):
                click.echo("    VERIFIED: Content hash verified")

            all_valid = all_valid and result.is_valid

        except Exception as e:
            click.echo(" [FAIL]")
            click.echo(f"    ERROR: Validation failed: {e!s}", err=True)
            all_valid = False
            total_errors += 1

    click.echo()

    if all_valid:
        click.echo(click.style("SUCCESS: All validations passed", fg="green"))
        click.echo(f"Archive integrity confirmed for {file_path}")
    else:
        click.echo(click.style("FAILED: Validation failed", fg="red"))
        click.echo(f"Found {total_errors} error(s) and {total_warnings} warning(s)")
        sys.exit(1)


def _validate_all_archives(
    validator: "ArchiveValidator", checks: List[str], sample_rate: float
) -> None:
    """Validate all archives with specified checks."""
    click.echo("Validating all archives...")
    click.echo()

    try:
        all_results = validator.validate_all_archives(checks, sample_rate)
    except Exception as e:
        click.echo(f"ERROR: Failed to validate archives: {e!s}", err=True)
        sys.exit(1)

    if not all_results:
        click.echo("No archives found to validate.")
        return

    total_files = 0
    total_versions = 0
    failed_files = []
    total_errors = 0
    total_warnings = 0

    for file_path, results in all_results.items():
        total_files += 1
        file_versions = len({r.details.get("archive_id", "") for r in results if r.details})
        total_versions += file_versions

        file_valid = True
        file_errors = 0
        file_warnings = 0

        for result in results:
            if not result.is_valid:
                file_valid = False
            file_errors += len(result.errors)
            file_warnings += len(result.warnings)

        total_errors += file_errors
        total_warnings += file_warnings

        status = "[PASS]" if file_valid else "[FAIL]"
        click.echo(f"{status} {file_path}")

        if file_errors > 0:
            click.echo(f"    {file_errors} error(s), {file_warnings} warning(s)")
            failed_files.append(file_path)

    click.echo()
    click.echo(f"Validated {total_files} file(s) with {total_versions} version(s)")

    if total_errors == 0:
        click.echo(click.style("SUCCESS: All archives passed validation", fg="green"))
    else:
        click.echo(click.style(f"FAILED: {len(failed_files)} file(s) failed validation", fg="red"))
        click.echo(f"Total: {total_errors} error(s), {total_warnings} warning(s)")

        if failed_files:
            click.echo("\nFailed files:")
            for failed_file in failed_files:
                click.echo(f"  - {failed_file}")

        sys.exit(1)
