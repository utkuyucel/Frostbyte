# Linting Guide

This guide provides information on how to run linters for code quality checks in the Frostbyte project.

## Linting Tools

The Frostbyte project uses the following linting tools:

- **Ruff**: A fast Python linter that replaces many tools (including Flake8, isort, etc.) and provides automatic fixes
- **Black**: Code formatter that enforces a consistent style
- **isort**: Sorts and formats imports

## Running Linters

We provide a shell script that runs all linters in sequence:

```bash
# Run linters and apply automatic fixes (default behavior)
./lint.sh

# Run linters in check-only mode (no fixes applied)
./lint.sh --check

# Run linters with fixes on a specific directory
./lint.sh frostbyte/core

# Run linters in check-only mode on a specific directory
./lint.sh --check frostbyte/core

# Process all files, ignoring .gitignore patterns
./lint.sh --ignore-git
```

By default, the linting script respects `.gitignore` patterns and only processes files that are tracked by Git or not explicitly ignored. This prevents unnecessary linting of files in virtual environments, build directories, and other excluded locations.

## Configuration

Linter configurations are defined in `pyproject.toml`:

- Line length is set to 100 characters
- Python 3.8+ target version
- Custom rules and ignores are configured for the project's needs

## Integration with Git Workflow

You can manually run the linters before committing changes to ensure code quality:

```bash
# Check only staged files that are about to be committed
git diff --cached --name-only --diff-filter=d | grep -E '\.py$' | xargs ./lint.sh
```

If you prefer to automate this process, you can create a pre-commit hook in your local repository.

## Individual Linters

You can also run linters individually:

```bash
# Ruff (lint)
ruff check .
ruff check --fix .

# Ruff (format)
ruff format .
ruff format --check .

# Black
black .
black --check .

# isort
isort .
isort --check .
```
