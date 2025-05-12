from setuptools import setup, find_packages

# Read long description from README.md
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="frostbyte",
    version="0.1.0",
    description="Cold Data Archiving for Pandas Workflows",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="utkuyucel",
    url="https://github.com/utkuyucel/frostbyte",  # Replace with actual GitHub URL
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.8",
    install_requires=[
        "click>=8.0.0",
        "pandas>=1.3.0",
        "numpy>=1.20.0",
        "duckdb>=0.7.0",
        "zstandard>=0.18.0",
        "pyarrow>=7.0.0",  # For Parquet support
        "pyyaml>=6.0",
        "tabulate>=0.9.0",
    ],
    extras_require={
        'dev': [
            "pytest>=7.0.0",
            "pytest-cov>=3.0.0",
            "black>=22.0.0",
            "isort>=5.10.0",
            "mypy>=0.950",
            "flake8>=5.0.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "frostbyte=frostbyte.cli:cli",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
)
