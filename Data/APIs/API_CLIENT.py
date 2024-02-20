import concurrent.futures
from api_utils import APIClient, FOREX_API_KEY, COMMODITIES_API_KEY, OIL_API_KEY, CRYPTO_API_KEY

# instance client pour chaque API
Forex_CLIENT = APIClient(FOREX_API_KEY)
Forex_change_CLIENT = APIClient(FOREX_API_KEY)
Commodities_CLIENT = APIClient(COMMODITIES_API_KEY)
Oil_CLIENT = APIClient(OIL_API_KEY)
Crypto_CLIENT = APIClient(CRYPTO_API_KEY)

def forex_call(url, filename):
    return Forex_CLIENT.make_api_call_save_data(url, filename)

def forex_change_call(url, filename):
    return Forex_change_CLIENT.make_api_call_save_data(url, filename)

def commodities_call(url, filename):
    return Commodities_CLIENT.make_api_call_save_data(url, filename)

def oil_call(url, filename):
    return Oil_CLIENT.make_api_call_save_data(url, filename)

def crypto_call(url, filename):
    return Crypto_CLIENT.make_api_call_save_data(url, filename)

# ALL LINKS WTF
url_forex = f'https://www.alphavantage.co/query?function=FX_MONTHLY&from_symbol=EUR&to_symbol=USD&apikey={FOREX_API_KEY}'
url_forex_change_base = f'https://www.alphavantage.co/query?function=FX_MONTHLY&from_symbol=EUR&to_symbol={{}}&apikey={FOREX_API_KEY}'
forex_currencies = ['USD', 'JPY', 'GBP', 'CAD']
urls_forex_change = [url_forex_change_base.format(currency) for currency in forex_currencies]
url_commodities = f'https://www.alphavantage.co/query?function=ALL_COMMODITIES&interval=monthly&apikey={COMMODITIES_API_KEY}'
url_oil = f'https://www.alphavantage.co/query?function=WTI&interval=monthly&apikey={OIL_API_KEY}'
base_url_crypto = 'https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_MONTHLY&symbol={}&market=CNY&apikey={CRYPTO_API_KEY}'
crypto_symbols = ['ETC', 'ZEC', 'ALGO', 'FIL', 'CHZ', 'DGB', 'MANA', 'ZRX', 'ENJ', 'BAT', 'KSM', 'YFI', 'REN', 'LSK', 'CRV', 'BTT', 'QTUM', 'ICX', 'WAVES', 'GRT', 'RVN', 'ZIL', 'HBAR', 'BTG', 'HNT', 'CELO', 'SC', 'IOST', 'ONT', 'OMG', 'TUSD', 'XVG', 'DENT', 'GNT', 'DGB', 'ZEN', 'WTC', 'REP', 'NANO', 'KNC', 'SNT', 'THETA', 'ARDR', 'BCN', 'BTS', 'CVC', 'PAX', 'NPXS', 'MANA', 'STEEM', 'LRC', 'SUSHI', 'MATIC', 'RLC', 'STORJ', 'ANT', 'BCD', 'RSR', 'BAL', 'XEM', 'ZKS', 'SKL', 'ANKR', 'BNT', 'BAND', 'ONG', 'RLY', 'HOT', 'STMX', 'POLY', 'OXT', 'RLY', 'MIR', 'NMR', 'WAXP', 'CHSB', 'CRO', 'REP', 'LUNA', 'ZIL', 'WAVES', 'XHV', 'RIF', 'LPT', 'IQ', 'XWC', 'QSP', 'BTT', 'WOO', 'WIN', 'QNT', 'GNO', 'RLC', 'CHR', 'LTO', 'AVA', 'FTM', 'REQ', 'AMP', 'ARK', 'ANT', 'QKC', 'WAN', 'MED', 'TRU', 'HIVE', 'USDP', 'NMR', 'STPT', 'BCHA', 'COTI', 'BNT', 'TRB', 'FET', 'XHV', 'VTHO', 'DNT', 'ADX']

# Execute API calls concurrently
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Forex 
    forex_data_future = executor.submit(forex_call, url_forex, "Forex_data.json")
    
    # Forex  rates
    forex_change_futures = [executor.submit(forex_change_call, url, f"Taux_de_change_{currency}_data.json") for url, currency in zip(urls_forex_change, forex_currencies)]
    
    # Commodities
    commodities_data_future = executor.submit(commodities_call, url_commodities, "Commodities_data.json")
    
    # Oil 
    oil_data_future = executor.submit(oil_call, url_oil, "Oil_data.json")
    
    # Crypto 
    crypto_futures = [executor.submit(crypto_call, base_url_crypto, f"{symbol}_crypto_data.json") for symbol in crypto_symbols]


# RESULT
data_forex = forex_data_future.result()
data_commodities = commodities_data_future.result()
data_oil = oil_data_future.result()

# Print results
print(data_forex)
for future in forex_change_futures:
    print(future.result())
print(data_commodities)
print(data_oil)
for future in crypto_futures:
    print(future.result())
