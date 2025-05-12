# Frostbyte Examples

This directory contains example scripts and notebooks demonstrating how to use Frostbyte.

## Example Files

### `diff_functionality.ipynb`
A Jupyter notebook that provides an interactive tutorial on using Frostbyte's diff functionality. This includes:
- Direct DataFrame comparison
- Automatic key detection
- Archive-based workflow
- Version-specific comparisons

### `diff_demo.py`
A demo script that shows how to use Frostbyte's diff functionality with archive operations:
- Creating and archiving sample data files
- Comparing files directly
- Comparing specific versions

### `diff_with_keys.py`
Demonstrates how to use key-based comparison in the diff functionality:
- Using different columns as keys
- Comparing results with different key strategies
- Working with composite keys (multiple columns)

### `frostbyte_diff_roadmap.py`
A comprehensive example that shows a complete workflow:
- Direct DataFrame diffing
- Archive-based file comparison
- Working with versioned files

### `sample_pipeline.py`
Shows how to integrate Frostbyte into a data processing pipeline.

## Running the Examples

Most examples can be run directly from the command line:

```bash
# Activate your virtual environment
source ../frostbyte_venv/bin/activate

# Run an example script
python diff_demo.py
```

For the Jupyter notebook, you'll need to have Jupyter installed:

```bash
# Install Jupyter if needed
pip install jupyter

# Launch the notebook
jupyter notebook diff_functionality.ipynb
```
