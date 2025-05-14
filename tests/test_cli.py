"""
Tests for the Frostbyte CLI.

This module contains tests for the command-line interface functionality.
"""

import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from frostbyte.cli.commands import cli


@pytest.fixture
def cli_runner() -> CliRunner:
    """Provides a CliRunner instance for testing CLI commands."""
    return CliRunner()


def test_cli_init(cli_runner: CliRunner, _tmp_path: Path) -> None:
    """Test initializing a Frostbyte repository."""
    with cli_runner.isolated_filesystem():
        # Run the init command
        result = cli_runner.invoke(cli, ["init"])

        # Check that the command succeeded
        assert result.exit_code == 0
        assert "Frostbyte initialized successfully" in result.output

        # Check that the directory was created
        assert os.path.exists(".frostbyte")
        assert os.path.exists(".frostbyte/archives")
        assert os.path.exists(".frostbyte/manifest.db")


def test_cli_archive_and_restore(cli_runner: CliRunner, sample_csv: str) -> None:
    """Test archiving and restoring a file."""
    with cli_runner.isolated_filesystem():
        # First initialize
        cli_runner.invoke(cli, ["init"])

        # Copy the sample file to the current directory
        sample_path = Path("sample.csv")
        with open(sample_path, "w") as f:
            f.write(sample_csv)

        # Archive the file
        archive_result = cli_runner.invoke(cli, ["archive", str(sample_path)])

        # Check that archiving succeeded
        assert archive_result.exit_code == 0
        assert "Archived" in archive_result.output

        # Remove the original file
        os.remove(sample_path)
        assert not os.path.exists(sample_path)

        # Restore the file
        restore_result = cli_runner.invoke(cli, ["restore", str(sample_path)])

        # Check that restoring succeeded
        assert restore_result.exit_code == 0
        assert "Restored" in restore_result.output

        # Check that the file exists again
        assert os.path.exists(sample_path)


def test_cli_ls(cli_runner: CliRunner, sample_csv: str) -> None:
    """Test listing archived files."""
    with cli_runner.isolated_filesystem():
        # First initialize
        cli_runner.invoke(cli, ["init"])

        # Create and archive a file
        sample_path = Path("sample.csv")
        with open(sample_path, "w") as f:
            f.write(sample_csv)

        cli_runner.invoke(cli, ["archive", str(sample_path)])

        # Test ls command
        result = cli_runner.invoke(cli, ["ls"])

        # Check that listing succeeded
        assert result.exit_code == 0
        assert "sample.csv" in result.output
        assert "1" in result.output  # Version number

        # Test ls --all command
        result_all = cli_runner.invoke(cli, ["ls", "--all"])

        # Check that detailed listing succeeded
        assert result_all.exit_code == 0
        assert "sample.csv" in result_all.output


def test_cli_stats(cli_runner: CliRunner, sample_csv: str) -> None:
    """Test getting statistics about archives."""
    with cli_runner.isolated_filesystem():
        # First initialize
        cli_runner.invoke(cli, ["init"])

        # Create and archive a file
        sample_path = Path("sample.csv")
        with open(sample_path, "w") as f:
            f.write(sample_csv)

        cli_runner.invoke(cli, ["archive", str(sample_path)])

        # Test overall stats
        result = cli_runner.invoke(cli, ["stats"])

        # Check that stats command succeeded
        assert result.exit_code == 0
        assert "Overall Statistics" in result.output

        # Test file-specific stats
        result_file = cli_runner.invoke(cli, ["stats", str(sample_path)])

        # Check that file stats command succeeded
        assert result_file.exit_code == 0
        assert "Statistics for" in result_file.output


def test_cli_diff(cli_runner: CliRunner, sample_csv: str) -> None:
    """Test diffing two versions of a file."""
    with cli_runner.isolated_filesystem():
        # First initialize
        cli_runner.invoke(cli, ["init"])

        # Create and archive a file
        sample_path = Path("sample.csv")
        with open(sample_path, "w") as f:
            f.write(sample_csv)

        cli_runner.invoke(cli, ["archive", str(sample_path)])

        # Modify the file and archive again
        with open(sample_path, "a") as f:
            f.write("d,4\n")

        cli_runner.invoke(cli, ["archive", str(sample_path)])

        # Test diff command
        result = cli_runner.invoke(cli, ["diff", f"{sample_path}@1", f"{sample_path}@2"])

        # Check that diff command succeeded
        assert result.exit_code == 0
        assert "Comparing" in result.output
        assert "Rows added" in result.output


def test_cli_purge(cli_runner: CliRunner, sample_csv: str) -> None:
    """Test purging archive versions."""
    with cli_runner.isolated_filesystem():
        # First initialize
        cli_runner.invoke(cli, ["init"])

        # Create and archive a file
        sample_path = Path("sample.csv")
        with open(sample_path, "w") as f:
            f.write(sample_csv)

        cli_runner.invoke(cli, ["archive", str(sample_path)])

        # Modify the file and archive again
        with open(sample_path, "a") as f:
            f.write("d,4\n")

        cli_runner.invoke(cli, ["archive", str(sample_path)])

        # Test purge command
        result = cli_runner.invoke(cli, ["purge", f"{sample_path}@1"])

        # Check that purge command succeeded
        assert result.exit_code == 0
        assert "Removed version" in result.output

        # Test purge --all command
        result_all = cli_runner.invoke(cli, ["purge", "--all", str(sample_path)])

        # Check that purge all command succeeded
        assert result_all.exit_code == 0
        assert "Removed all versions" in result_all.output


def test_cli_gui(cli_runner: CliRunner) -> None:
    """Test gui command."""
    result = cli_runner.invoke(cli, ["gui"])

    # Check that the command runs and shows the expected message
    assert result.exit_code == 0
    assert "coming in v0.2" in result.output
