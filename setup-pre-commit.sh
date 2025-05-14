#!/bin/bash

# Script to install git pre-commit hook for linting

# Get the absolute path to the repository root
REPO_ROOT=$(git rev-parse --show-toplevel)

# Create the pre-commit hook file
PRE_COMMIT_FILE="$REPO_ROOT/.git/hooks/pre-commit"

# Create the pre-commit hook script
cat > "$PRE_COMMIT_FILE" << 'EOL'
#!/bin/bash

# Get the root directory of the git repository
REPO_ROOT=$(git rev-parse --show-toplevel)

# Get the list of staged files that are Python files
STAGED_FILES=$(git diff --cached --name-only --diff-filter=d | grep '\.py$' || echo "")

if [ -z "$STAGED_FILES" ]; then
  echo "No Python files to lint."
  exit 0
fi

echo "Running linters on staged Python files..."

# Run the lint script only on staged Python files
# Note: We're passing explicit file paths so we don't need to respect .gitignore here
"$REPO_ROOT/lint.sh" --ignore-git $STAGED_FILES

# If lint.sh found issues, exit with a non-zero status code to prevent the commit
if [ $? -ne 0 ]; then
  echo "❌ Linting failed! Fix the issues before committing."
  echo "   (You can bypass this check with 'git commit --no-verify')"
  exit 1
fi

echo "✅ Pre-commit checks passed!"
EOL

# Make the hook executable
chmod +x "$PRE_COMMIT_FILE"

echo "✅ Pre-commit hook installed successfully at $PRE_COMMIT_FILE"
echo "The hook will run lint.sh on staged Python files before each commit."
echo "You can bypass the hook if needed with: git commit --no-verify"
