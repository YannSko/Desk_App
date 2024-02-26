import pandas as pd
import numpy as np
from data_process import infer_data_types, convert_columns

def test_infer_data_types():
    # Create a sample DataFrame
    df = pd.DataFrame({
        'A': [1, 2, None, 5],
        'B': ['2022-01-01', '2022-02-01', '2022-03-01', '2022-04-01'],
        'C': ['$100.00', '$200.50', None, '$500.25'],
        'D': ['100%', '50%', None, '80%'],
        'E': [10.5, 30.0, 40.1, 50.2],
        'F': ['2022', '2024', None, '2025']
    })
    
    # Define expected inferred data types
    expected_types = {
        'A': 'int64',
        'B': 'datetime.date',
        'C': 'float64',
        'D': 'float64',
        'E': 'float64',
        'F': 'int64'
    }
    
    # Infer data types
    inferred_types = infer_data_types(df)
    
    # Assert that inferred types match expected types
    for col, dtype in expected_types.items():
        assert inferred_types[col] == dtype

def test_convert_columns():
    # Create a sample DataFrame
    df = pd.DataFrame({
        'A': [1, 2, 3, 0],
        'B': ['2022-01-01', '2022-02-01', '2022-03-01', '2022-04-01'],
        'C': ['$10000.00', '$20050.00', None, '$50025.00'],
        'D': ['100.0', '50.0', '401.0', '80.0'],
        'E': ['105.0', '300.0', None, '502.0'],
        'F': ['2022', '2024', '2023', '2025']
    })
    
    # Define inferred types
    inferred_types = {
        'A': 'int64',
        'B': 'datetime.date',
        'C': 'float64',
        'D': 'float64',
        'E': 'float64',
        'F': 'int64'
    }
    
    # Convert columns
    converted_df = convert_columns(df, inferred_types)
    
    # Assert data types after conversion
    assert converted_df['A'].dtype == "int64"
    assert converted_df['B'].dtype == 'datetime64[ns]'
    assert converted_df['C'].dtype == "float64"
    assert converted_df['D'].dtype == "float64"
    assert converted_df['E'].dtype == "float64"
    assert converted_df['F'].dtype == "int64"
