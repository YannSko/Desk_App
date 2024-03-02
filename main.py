from utils.Logs.logger import logs
from Data.Database.database_utils import *

@logs
def my_function(data):
    print(f"Traitement des données: {data}")


# Appeler la fonction
if __name__ == "__main__":
    my_function("données précises")
    