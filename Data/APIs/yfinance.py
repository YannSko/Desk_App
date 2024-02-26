import pandas as pd
import yfinance as yf
import json
import os


# Load DataFrame
df = pd.read_csv("nasdaq.csv")

# Get list of symbols
list_symbol = df['Symbol']

# Dictionary to store all stock data
all_stocks_data = {}

# Loop through each symbol and retrieve data
for symbol in list_symbol:
    try:
        stock_data = yf.Ticker(symbol)
        info = stock_data.info
        all_stocks_data[symbol] = info
        print(f"Data retrieved for {symbol}")
    except Exception as e:
        print(f"Error retrieving data for {symbol}: {e}")

# Define the directory path
directory = "apis/data_from_api"
os.makedirs(directory, exist_ok=True)

# Write data to JSON file
with open(os.path.join(directory, "stocks_data.json"), "w") as file:
    json.dump(all_stocks_data, file)

print("Data for all symbols has been saved in the file stocks_data.json.")
