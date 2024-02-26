from utils.Logs.log import log_interaction

@log_interaction
def my_function(data):
    print(f"Traitement des données: {data}")

# Appeler la fonction
if __name__ == "__main__":
    my_function("données précises")