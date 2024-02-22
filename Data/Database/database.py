import json
from database_utils import *
import time





# Create table using the JSON input
create_table_from_json()

# Insert nested data into the table
insert_nested_data()

time.sleep(2)
# Retrieve and print data from the table
data = get_data_x()
print("Retrieved data:", data)

data2 = get_data_x()
print("Retrieved data:", data2)
data3 = get_data_x()
print("Retrieved data:", data3)





