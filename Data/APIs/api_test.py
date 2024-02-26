from Data.APIs.Api_call import (
    forex_call,
    forex_change_call,
    commodities_call,
    oil_call,
    crypto_call,
)
from api_utils import APIClient
import pytest
import os
import json
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get API keys from environment variables
FOREX_API_KEY = os.getenv("FOREX_API_KEY")
COMMODITIES_API_KEY = os.getenv("COMMODITIES_API_KEY")
OIL_API_KEY = os.getenv("OIL_API_KEY")
CRYPTO_API_KEY = os.getenv("CRYPTO_API_KEY")

@pytest.fixture
def mock_api_responses(tmpdir):
    data_folder = tmpdir.mkdir("data")
    data_folder_path = str(data_folder)

    class MockAPIClient:
        def make_api_call_save_data(self, url, filename):
            data = {"mock": "data"}
            with open(os.path.join(data_folder_path, filename), "w") as f:
                json.dump(data, f)
            return data

    return MockAPIClient()


def test_forex_call(mock_api_responses):
    url = f"https://www.alphavantage.co/query?function=FX_MONTHLY&from_symbol=EUR&to_symbol=USD&apikey={FOREX_API_KEY}"
    filename = "Forex_data.json"
    data = forex_call(url, filename)
    assert data == {"mock": "data"}


def test_forex_change_call(mock_api_responses):
    forex_currencies = ['USD', 'JPY', 'GBP', 'CAD']
    urls_forex_change = [f"https://www.alphavantage.co/query?function=FX_MONTHLY&from_symbol=EUR&to_symbol={currency}&apikey={FOREX_API_KEY}" for currency in forex_currencies]
    
    filename_template = "Taux_de_change_{}_data.json"
    for url, currency in zip(urls_forex_change, forex_currencies):
        filename = filename_template.format(currency)
        data = forex_change_call(url, filename)
        assert data == {"mock": "data"}


def test_commodities_call(mock_api_responses):
    url = f"https://www.alphavantage.co/query?function=ALL_COMMODITIES&interval=monthly&apikey={COMMODITIES_API_KEY}"
    filename = "Commodities_data.json"
    data = commodities_call(url, filename)
    assert data == {"mock": "data"}


def test_oil_call(mock_api_responses):
    url = f"https://www.alphavantage.co/query?function=WTI&interval=monthly&apikey={OIL_API_KEY}"
    filename = "Oil_data.json"
    data = oil_call(url, filename)
    assert data == {"mock": "data"}


def test_crypto_call(mock_api_responses):
    base_url_crypto = 'https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_MONTHLY&symbol={}&market=CNY&apikey={CRYPTO_API_KEY}'
    crypto_symbols = ['ETC', 'ZEC', 'ALGO', 'FIL', 'CHZ']  # Add more if needed
    
    filename_template = "{}_crypto_data.json"
    for symbol in crypto_symbols:
        url = base_url_crypto.format(symbol)
        filename = filename_template.format(symbol)
        data = crypto_call(url, filename)
        assert data == {"mock": "data"}


def test_APIClient_make_api_call_save_data(tmpdir):
    data_folder = str(tmpdir)
    api_client = APIClient()
    url = 'https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_MONTHLY&symbol={}&market=CNY&apikey={CRYPTO_API_KEY}'  # This is just a placeholder, replace it with a valid URL if needed
    filename = "test_data.json"
    data = {"test": "data"}

    api_client.make_api_call_save_data(url, filename, data_folder)

    with open(os.path.join(data_folder, filename), "r") as f:
        saved_data = json.load(f)

    assert saved_data == data
