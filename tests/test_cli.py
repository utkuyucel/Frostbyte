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


def test_cli_init(cli_runner: CliRunner, tmp_path: Path) -> None:
    """Test initializing a Frostbyte repository."""
    with cli_runner.isolated_filesystem(temp_dir=tmp_path):
        result = cli_runner.invoke(cli, ["init"])
        assert result.exit_code == 0
        assert "Frostbyte initialized successfully" in result.output
        assert os.path.exists(".frostbyte")
        assert os.path.exists(".frostbyte/archives")
        assert os.path.exists(".frostbyte/manifest.db")


def test_cli_ls(cli_runner: CliRunner, sample_csv: str) -> None:
    """Test listing archived files."""
    with cli_runner.isolated_filesystem():
        cli_runner.invoke(cli, ["init"])
        sample_path = Path("sample.csv")
        with open(sample_path, "w") as f:
            f.write(sample_csv)
        cli_runner.invoke(cli, ["archive", str(sample_path)])
        result = cli_runner.invoke(cli, ["ls"])  # Summary view
        assert result.exit_code == 0
        assert "sample.csv" in result.output
        assert "Total Row Count" in result.output  # Check for summary header

        # Archive again to have multiple versions for detailed view
        with open(sample_path, "a") as f:
            f.write("e,5\n")  # Add a new row
        cli_runner.invoke(cli, ["archive", str(sample_path)])

        result_detailed = cli_runner.invoke(cli, ["ls", str(sample_path)])  # Detailed view
        assert result_detailed.exit_code == 0
        assert "sample.csv" in result_detailed.output
        assert "sample_v1.parquet" in result_detailed.output  # Check for archive filename v1
        assert "sample_v2.parquet" in result_detailed.output  # Check for archive filename v2
        assert "Row Count" in result_detailed.output  # Check for detailed view header
        assert "Ver" in result_detailed.output  # Check for detailed view header (version)


def test_cli_stats(cli_runner: CliRunner, sample_csv: str) -> None:
    """Test getting statistics about archives."""
    with cli_runner.isolated_filesystem():
        cli_runner.invoke(cli, ["init"])
        sample_path = Path("sample.csv")
        with open(sample_path, "w") as f:
            f.write(sample_csv)
        cli_runner.invoke(cli, ["archive", str(sample_path)])
        result = cli_runner.invoke(cli, ["stats"])
        assert result.exit_code == 0
        assert "Archive Statistics" in result.output


def test_cli_purge(cli_runner: CliRunner, sample_csv: str) -> None:
    """Test purging archive versions."""
    with cli_runner.isolated_filesystem():
        cli_runner.invoke(cli, ["init"])
        sample_path = Path("sample.csv")
        with open(sample_path, "w") as f:
            f.write(sample_csv)
        cli_runner.invoke(cli, ["archive", str(sample_path)])
        with open(sample_path, "a") as f:
            f.write("d,4\n")
        cli_runner.invoke(cli, ["archive", str(sample_path)])
        purge_one = cli_runner.invoke(cli, ["purge", f"{sample_path}@1"])
        assert purge_one.exit_code == 0
        purge_all = cli_runner.invoke(cli, ["purge", "--all", str(sample_path)])
        assert purge_all.exit_code == 0
