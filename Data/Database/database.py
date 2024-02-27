import json
from database_utils import *
import time
from data_process import convert_columns,  infer_data_type, infer_data_types


#update_data()
#update_data()
#update_data()
#delete_data()
#delete_data()
#delete_data()
#create_tables_from_json()

######### pour créer les tables et insérer direct

df_to_sql_j()
#df_to_sql_cxe()## Commodities-Oil-Tout json simple
#df_to_sql_big() ## Json avec header > data ( headers -rows)
#insert_data_from_json()
#create_table()#create pour toutes les structs en gé
#create_table()
#create_table()
# Create table using the JSON inputF
#create_table_from_json()

# Insert nested data into the tableF
#insert_crypto_data_json()### Spé struc pour les crypto_data.json
#insert_forex_data_json()### spé structure forex
#insert_data_json()# insert pour les structs simple como et oil
#data= get_data()
#print("data:", data)

#time.sleep(2)
# Retrieve and print data from the tableF
#data = get_data_x()
#print("Retrieved data:", data)

#data2 = get_data_x()
#print("Retrieved data:", data2)
#data3 = get_data_x()
#print("Retrieved data:", data3)

#update_data()
#update_data()
#update_data()



#### test recup + treatment

df = get_data()
if isinstance(df, pd.DataFrame):
    inferred_types = infer_data_types(df)
    df = convert_columns(df, inferred_types)
    print(df)
else:
    print("Error: df is not a DataFrame.")