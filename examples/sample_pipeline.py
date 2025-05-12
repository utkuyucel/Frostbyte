"""
Sample data pipeline showing Frostbyte usage.
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path


def generate_sample_dataset(output_path: str, rows: int = 10000):
    """
    Generate a sample dataset for demonstration.
    
    Args:
        output_path: Path where the CSV will be saved
        rows: Number of rows to generate
    """
    # Create sample data
    data = {
        'id': np.arange(rows),
        'name': [f'Item {i}' for i in range(rows)],
        'value': np.random.rand(rows) * 100,
        'category': np.random.choice(['A', 'B', 'C', 'D'], size=rows),
        'date': pd.date_range(start='2023-01-01', periods=rows, freq='H')
    }
    
    df = pd.DataFrame(data)
    
    # Save to CSV
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False)
    print(f"Generated sample dataset with {rows} rows at {output_path}")
    
    
def transform_dataset(input_path: str, output_path: str):
    """
    Perform a simple transformation on a dataset.
    
    Args:
        input_path: Path to input CSV
        output_path: Path where transformed CSV will be saved
    """
    # Read input data
    df = pd.read_csv(input_path)
    
    # Apply transformations
    df['value_squared'] = df['value'] ** 2
    df['value_category'] = pd.cut(
        df['value'], 
        bins=[0, 25, 50, 75, 100], 
        labels=['Low', 'Medium', 'High', 'Very High']
    )
    
    # Save transformed data
    df.to_csv(output_path, index=False)
    print(f"Transformed dataset saved to {output_path}")
    
    
def main():
    """Run the sample pipeline."""
    # Generate raw data
    raw_path = 'data/raw/sample.csv'
    generate_sample_dataset(raw_path)
    
    # Archive raw data
    print("\nArchiving raw data...")
    os.system(f"frostbyte archive {raw_path}")
    
    # Transform data
    processed_path = 'data/processed/sample.csv'
    transform_dataset(raw_path, processed_path)
    
    # Archive processed data
    print("\nArchiving processed data...")
    os.system(f"frostbyte archive {processed_path}")
    
    # List archives
    print("\nListing archives...")
    os.system("frostbyte ls --all")
    
    # Show stats
    print("\nShowing archive stats...")
    os.system("frostbyte stats")
    
    print("\nDone! You can now experiment with restore, diff, and other commands.")


if __name__ == '__main__':
    main()
