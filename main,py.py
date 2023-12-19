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
df = pd.json_normalize(data['data'])
df = pl.from_pandas(df)
df = df.drop(["slug", "num_market_pairs", "date_added", "tags", "max_supply", "circulating_supply", "total_supply",
         "infinite_supply", "platform", "self_reported_circulating_supply", "self_reported_market_cap", "tvl_ratio",
         "last_updated", "quote.USD.percent_change_1h", "quote.USD_percent_change_30d", "quote.USD_percent_change_90d",
         "quote.USD.market_cap", "quote.USD.fully_diluted_market_cap", "quote.USD.tvl", "quote.USD.last_updated", "platform.id",
         "platform.name", "platform.symbol", "platform.slug", "platform.token_address"
         ])

df.rename({"quote.USD.price": "price"})
