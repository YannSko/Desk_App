import json
from database_utils import *
import time

# Prompt the user for the path to the JSON file
file_path = input("Enter the path to the JSON file: ")

try:
    with open(file_path, 'r') as file:
        input_json = json.load(file)
except FileNotFoundError:
    print("File not found. Please enter a valid file path.")
    exit()

# Example table name
table_name = "abudl"

# Create table using the JSON input
create_table_from_json(table_name, input_json)

# Insert nested data into the table
insert_nested_data(table_name, input_json)

time.sleep(5)
# Retrieve and print data from the table
data = get_data_x(table_name)
print("Retrieved data:", data)

# Update data in the table (example)
update_data(table_name, 1, {"age": 31})

# Delete data from the table (example)
delete_data(table_name, 1)