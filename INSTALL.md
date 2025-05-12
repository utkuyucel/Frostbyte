# Installation Guide

## Requirements

- Python 3.8 or higher
- pip package manager

## Installing from PyPI (Recommended)

Once published to PyPI, you can install Frostbyte with:

```bash
pip install frostbyte
```

## Installing from Source

1. Clone the repository:
   ```bash
   git clone https://github.com/utkuyucel/Frostbyte.git
   cd frostbyte
   ```

2. Create and activate a virtual environment (recommended):
   ```bash
   python -m venv frostbyte_venv
   source frostbyte_venv/bin/activate  # On Windows: frostbyte_venv\Scripts\activate
   ```

3. Install the package in development mode:
   ```bash
   pip install -e .
   ```

   This will install Frostbyte along with its dependencies.

4. Verify the installation:
   ```bash
   frostbyte --version
   ```

## Installing from Requirements.txt

If you just want to install the dependencies:

```bash
pip install -r requirements.txt
```

## Testing the Installation

Run the test suite to make sure everything is working correctly:

```bash
pytest
```

## Using in Your Project

1. Initialize Frostbyte in your project directory:
   ```bash
   cd /path/to/your/project
   frostbyte init
   ```

2. Start archiving data files:
   ```bash
   frostbyte archive data/your_file.csv
   ```

3. Check the README.md for more usage examples.
