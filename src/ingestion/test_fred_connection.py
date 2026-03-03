import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FRED_API_KEY")

url = "https://api.stlouisfed.org/fred/series/observations"

params = {
    "series_id": "UNRATE", 
    "api_key": API_KEY,
    "file_type": "json"
}

response = requests.get(url, params=params)

if response.status_code == 200:
    print("Connection successful!")
    data = response.json()
    print("First 5 observations:")
    print(data["observations"][:5])
else:
    print("Error:", response.status_code)
    print(response.text)