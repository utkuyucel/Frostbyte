#!/bin/bash

# lint.sh - Run all linters for the Frostbyte project
# Usage: 
#   ./lint.sh [--fix] [--ruff-only|--black-only|--isort-only] [--ignore-git] [directory]
#   ./lint.sh --help

# Default values
FIX=true  # Default to fix mode for local usage
CHECK_ONLY=false  # Flag to enable check-only mode (for CI)
TARGET_DIR="."
RUN_RUFF=true
RUN_BLACK=true
RUN_ISORT=true
RESPECT_GITIGNORE=true  # By default, respect .gitignore patterns
HELP=false

# Parse arguments
for arg in "$@"; do
  if [[ "$arg" == "--fix" ]]; then
    FIX=true
  elif [[ "$arg" == "--check" ]]; then
    FIX=false
    CHECK_ONLY=true
  elif [[ "$arg" == "--ruff-only" ]]; then
    RUN_RUFF=true
    RUN_BLACK=false
    RUN_ISORT=false
  elif [[ "$arg" == "--black-only" ]]; then
    RUN_RUFF=false
    RUN_BLACK=true
    RUN_ISORT=false
  elif [[ "$arg" == "--isort-only" ]]; then
    RUN_RUFF=false
    RUN_BLACK=false
    RUN_ISORT=true
  elif [[ "$arg" == "--ignore-git" ]]; then
    RESPECT_GITIGNORE=false
  elif [[ "$arg" == "--help" ]]; then
    HELP=true
  elif [[ "$arg" != -* ]]; then
    TARGET_DIR="$arg"
  fi
done

# Show help if requested
if [[ "$HELP" == "true" ]]; then
  cat << EOF
Frostbyte Linting Script

Usage:
  ./lint.sh [OPTIONS] [DIRECTORY]

Options:
  --fix          Fix issues automatically when possible (default)
  --check        Check only, don't fix issues (used in CI)
  --ruff-only    Run only ruff linter and formatter
  --black-only   Run only black formatter
  --isort-only   Run only isort import sorter
  --ignore-git   Process all files, ignoring .gitignore patterns
  --help         Show this help message

Examples:
  ./lint.sh                     # Fix issues in all files, respecting .gitignore (default)
  ./lint.sh --check             # Check only, don't fix issues
  ./lint.sh frostbyte/core      # Fix issues in the core module
  ./lint.sh --ruff-only         # Run only ruff with fixes
  ./lint.sh --ignore-git        # Process all files, including those in .gitignore
EOF
  exit 0
fi

echo "🔍 Running linters on: $TARGET_DIR"

# Set up target files based on git patterns if enabled
TARGET_FILES=""
if [[ "$RESPECT_GITIGNORE" == "true" ]]; then
  if [[ -d ".git" || -f ".git" ]]; then
    # We're in a git repository
    echo "🌱 Respecting .gitignore patterns"
    if [[ "$TARGET_DIR" == "." ]]; then
      # Get all tracked Python files or untracked but not ignored files
      TARGET_FILES=$(git ls-files "*.py" && git ls-files --others --exclude-standard "*.py")
    else
      # Get Python files from the specific directory
      TARGET_FILES=$(git ls-files "$TARGET_DIR/**/*.py" && git ls-files --others --exclude-standard "$TARGET_DIR/**/*.py")
      if [[ -z "$TARGET_FILES" ]]; then
        # Try again with direct path in case TARGET_DIR is a file
        TARGET_FILES=$(git ls-files "$TARGET_DIR" && git ls-files --others --exclude-standard "$TARGET_DIR")
      fi
    fi
    TARGET_FILES=$(echo "$TARGET_FILES" | sort -u)
  else
    echo "⚠️ Not a git repository, falling back to regular file search"
    RESPECT_GITIGNORE=false
  fi
fi

# Determine fix flags based on --fix argument
if [[ "$FIX" == "true" ]]; then
  RUFF_FIX_FLAG="--fix"
  BLACK_FIX_FLAG=""  # black always fixes
  ISORT_FIX_FLAG="--profile black"  # isort will apply fixes
  echo "🔧 Fix mode enabled - linters will attempt to fix issues automatically"
else
  RUFF_FIX_FLAG="--no-fix"
  BLACK_FIX_FLAG="--check"
  ISORT_FIX_FLAG="--profile black --check"
  echo "🔍 Check mode only - no automatic fixes will be applied"
fi

# Run all linters with appropriate flags
if [[ "$RUN_RUFF" == "true" ]]; then
  echo -e "\n📋 Step 1/4: Running ruff linter..."
  if [[ "$RESPECT_GITIGNORE" == "true" && ! -z "$TARGET_FILES" ]]; then
    echo "$TARGET_FILES" | xargs ruff check $RUFF_FIX_FLAG
    RUFF_EXIT=$?
  else
    ruff check $RUFF_FIX_FLAG $TARGET_DIR
    RUFF_EXIT=$?
  fi

  echo -e "\n📋 Step 2/4: Running ruff formatter..."
  if [[ "$RESPECT_GITIGNORE" == "true" && ! -z "$TARGET_FILES" ]]; then
    echo "$TARGET_FILES" | xargs ruff format $([[ "$FIX" == "false" ]] && echo "--check")
    RUFF_FORMAT_EXIT=$?
  else
    ruff format $([[ "$FIX" == "false" ]] && echo "--check") $TARGET_DIR
    RUFF_FORMAT_EXIT=$?
  fi
else
  RUFF_EXIT=0
  RUFF_FORMAT_EXIT=0
fi

if [[ "$RUN_BLACK" == "true" ]]; then
  echo -e "\n📋 Step 3/4: Running black formatter..."
  if [[ "$RESPECT_GITIGNORE" == "true" && ! -z "$TARGET_FILES" ]]; then
    echo "$TARGET_FILES" | xargs black $BLACK_FIX_FLAG
    BLACK_EXIT=$?
  else
    black $BLACK_FIX_FLAG $TARGET_DIR
    BLACK_EXIT=$?
  fi
else
  BLACK_EXIT=0
fi

if [[ "$RUN_ISORT" == "true" ]]; then
  echo -e "\n📋 Step 4/4: Running isort import sorter..."
  if [[ "$RESPECT_GITIGNORE" == "true" && ! -z "$TARGET_FILES" ]]; then
    echo "$TARGET_FILES" | xargs isort $ISORT_FIX_FLAG
    ISORT_EXIT=$?
  else
    isort $ISORT_FIX_FLAG $TARGET_DIR
    ISORT_EXIT=$?
  fi
else
  ISORT_EXIT=0
fi

# Check if any linter failed
if [[ $RUFF_EXIT -ne 0 || $RUFF_FORMAT_EXIT -ne 0 || $BLACK_EXIT -ne 0 || $ISORT_EXIT -ne 0 ]]; then
  echo -e "\n❌ Linting failed!"
  [[ $RUFF_EXIT -ne 0 ]] && echo "   - ruff check found issues"
  [[ $RUFF_FORMAT_EXIT -ne 0 ]] && echo "   - ruff format found issues"
  [[ $BLACK_EXIT -ne 0 ]] && echo "   - black found issues"
  [[ $ISORT_EXIT -ne 0 ]] && echo "   - isort found issues"
  
  if [[ "$FIX" == "false" ]]; then
    echo -e "\n💡 Run './lint.sh' without the --check flag to attempt to fix issues automatically."
  fi
  exit 1
else
  echo -e "\n✅ All linting checks passed!"
  exit 0
fi
