#!/usr/bin/env python
"""
Command-line runner for Frostbyte.

This script is installed as the 'frostbyte' command when the package is installed.
"""

from frostbyte.cli import cli

if __name__ == '__main__':
    cli()
