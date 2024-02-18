import pandas as pd
import yfinance as yf
import json

# Lire le DataFrame depuis le fichier CSV
df = pd.read_csv("nasdaq.csv")

# Récupérer tous les symboles de votre DataFrame
list_symbol = df['Symbol']

# Initialiser un dictionnaire pour stocker les données de chaque symbole
all_stocks_data = {}

# Itérer sur chaque symbole
for symbol in list_symbol:
    try:
        # Récupérer les données pour le symbole actuel
        stock_data = yf.Ticker(symbol)
        info = stock_data.info
        all_stocks_data[symbol] = info
        print(f"Données récupérées pour {symbol}")
    except Exception as e:
        print(f"Erreur lors de la récupération des données pour {symbol}: {e}")

# Enregistrer les données dans un fichier JSON
with open("stocks_data.json", "w") as file:
    json.dump(all_stocks_data, file)

print("Les données pour tous les symboles ont été enregistrées dans le fichier stocks_data.json.")
