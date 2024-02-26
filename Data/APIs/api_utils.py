import requests
import json

class APIClient:
    def __init__(self, api_key):
        self.api_key = api_key

    def make_api_call_save_data(self, url, filename):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Handle HTTP errors
            data = response.json()
            with open(filename, "w") as file:
                json.dump(data, file)
            print(f"Data saved in {filename}")
            return data
        except requests.RequestException as e:
            print(f"Error making request to {url}: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for {url}: {e}")
            return None
    

 
