import pandas as pd
import re
import warnings
import numpy as np

warnings.filterwarnings("ignore", message="Could not infer format")

def infer_data_types(df):
    # Define the percentage of rows to sample
    sample_percentage = 0.8
    
    # Sample the data
    sample = df.sample(frac=sample_percentage, random_state=42)
    
    # Dictionary to store inferred data types for each column
    inferred_data_types = {}
    
    # Loop through each column and infer the data type
    for col in sample.columns:
        inferred_data_type = infer_data_type(sample[col])
        inferred_data_types[col] = inferred_data_type
    return inferred_data_types


def infer_data_type(series):
    if series.dtype == 'object':
        # Convert non-string values to strings
        series = series.astype(str)
        
        # Replace 'None' with NaN
        series = series.replace('None', np.nan).infer_objects(copy=False)
        
        # Remove empty strings and NaNs from the series
        non_empty_series = series.dropna()
        non_empty_series = non_empty_series[non_empty_series != '']
        # Convert all non-empty series elements to strings
        non_empty_series = non_empty_series.astype(str)
        
        # Check if the series contains mostly integers
        if non_empty_series.str.replace('-', '').str.isdigit().mean() > 0.8:
            return 'datetime.date'
        
        # Check if the series contains mostly floats
        elif non_empty_series.str.replace('.', '').str.replace('-', '').str.replace(',', '').str.replace('%', '').str.isdigit().mean() > 0.8:
            return 'float64'
        
        # Check if the series contains mostly dates
        elif non_empty_series.str.match(r'^\d{4}-\d{2}-\d{2}$').mean() > 0.8:
            return 'datetime.date'
        
        # Check if the series contains mostly currency values ('$XXX.XX')
        elif non_empty_series.str.startswith('$').mean() > 0.8:
            return 'float64'
        
        # Check if the series contains mostly years (4-digit numbers)
        elif non_empty_series.str.match(r'^\d{4}$').mean() > 0.8:
            return 'int64'
        
        # Check if the series contains mostly percentage values ('X.XX%')
        elif non_empty_series.str.match(r'^-?\d+\.\d+%$').mean() > 0.8:
            return 'float64'
        
        # If none of the above conditions are met, assume it's an object
        else:
            return 'object'
    elif series.dtype == 'datetime64[ns]':
        return 'datetime.date'
    else:
        return str(series.dtype)

def convert_columns(df, inferred_types):
    for col, dtype in inferred_types.items():
        if dtype == 'datetime.date':
            try:
                df[col] = df[col].astype(str)
                df[col] = df[col].replace('None', np.nan)
                df[col] = df[col].str.replace('-', '')
                df[col] = pd.to_datetime(df[col]).dt.date
            except Exception as e:
                print(f"Error converting column '{col}' to date", e)
                df[col]= df[col].astype(str)
        elif dtype == 'int64':
            try:
            # Apply cleaning operations similar to infer_data_type function
                df[col] = df[col].astype(str)
                df[col] = df[col].replace('None', np.nan)
                df[col] = df[col].str.replace('-', '')
                df[col] = pd.to_numeric(df[col]).fillna(0).astype(int)  
            except Exception as e:
                print(f"Error converting column '{col}' to int", e)
                df[col]= df[col].astype(str) 
        elif dtype == 'float64':
            try:
                df[col] = df[col].astype(str)
                df[col] = df[col].replace('None', np.nan)
                df[col] = df[col].str.replace('-', '')
                df[col] = df[col].str.replace('.', '').str.replace('-', '').str.replace(',', '').str.replace('%', '').str.replace('$','')
                df[col] = pd.to_numeric(df[col]).fillna(0).astype(float)
            except Exception as e:
                print(f"Error converting column '{col}' to int", e)
                df[col]= df[col].astype(str) 
        elif dtype == 'object':
            df[col] = df[col].astype(str)
        # Add more conditions for other data types if needed
    return df