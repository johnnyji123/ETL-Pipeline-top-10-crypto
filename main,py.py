import pandas as pd
import polars as pl
import mysql.connector
import requests
import json

api_key = "69fc1391-a7e7-4dc0-8dbe-c96a959cd5c1"

url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"


parameters = {
    "start": "1",
    "limit": "10",
    "convert": "USD"
    }

headers = {
    "Accepts": "application/json",
    'X-CMC_PRO_API_KEY': 'edf8eb28-4a4f-4ca8-8be1-3fb45d624c36'
    }


response = requests.get(url, params = parameters, headers = headers)
data = json.loads(response.text)
data

