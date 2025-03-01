"""
Create a sample dataset for DataCanary demos.
"""
import pandas as pd
import numpy as np
import os

def create_sample_dataset(output_path="data/sample_data.parquet"):
    """Create a sample dataset with data quality issues for demonstration."""
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Set random seed for reproducibility
    np.random.seed(42)
    
    # Create a DataFrame with a variety of column types and data issues
    n_rows = 1000
    
    data = {
        # ID column - perfect unique values
        'id': range(1, n_rows + 1),
        
        # Numeric column with some nulls
        'value': [
            float(i) if np.random.random() > 0.05 else None
            for i in np.random.normal(100, 30, n_rows)
        ],
        
        # Categorical column with imbalanced distribution
        'category': np.random.choice(['A', 'B', 'C'], n_rows, p=[0.7, 0.2, 0.1]),
        
        # Date column with some duplicates
        'date': pd.date_range('2023-01-01', periods=n_rows // 2).tolist() + 
                pd.date_range('2023-01-01', periods=n_rows // 2).tolist(),
        
        # Text column with some empty strings
        'name': [
            f"Item {i}" if np.random.random() > 0.1 else ""
            for i in range(n_rows)
        ],
        
        # Boolean column
        'is_valid': np.random.choice([True, False], n_rows),
        
        # Column with outliers
        'score': [
            np.random.randint(0, 100) if np.random.random() > 0.02 else np.random.randint(500, 1000)
            for _ in range(n_rows)
        ]
    }
    
    df = pd.DataFrame(data)
    
    # Save as Parquet
    df.to_parquet(output_path, index=False)
    print(f"Sample dataset created at: {output_path}")
    return df

if __name__ == "__main__":
    create_sample_dataset()