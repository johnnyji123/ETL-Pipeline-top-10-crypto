import pandas as pd
import polars as pl
import mysql.connector
import requests
import json

api_key = "69fc1391-a7e7-4dc0-8dbe-c96a959cd5c1"

url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"


db = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "projects123123",
        database = "stock_pipeline"
    )

cursor = db.cursor()

# table stock_data_pipeline

def fetch_data(url):
    try:
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
        return df
    
    
    except Exception as e:
        print("couldn't fetch data", e)
        
df = fetch_data(url)    



df = df.drop(["id","slug", "num_market_pairs", "date_added", "tags", "max_supply", "circulating_supply", "total_supply",
         "infinite_supply", "platform", "self_reported_circulating_supply", "self_reported_market_cap", "tvl_ratio",
         "last_updated", "quote.USD.percent_change_1h", "quote.USD_percent_change_30d", "quote.USD_percent_change_90d",
         "quote.USD.market_cap", "quote.USD.fully_diluted_market_cap", "quote.USD.tvl", "quote.USD.last_updated", "platform.id",
         "platform.name", "platform.symbol", "platform.slug", "platform.token_address", "quote.USD.percent_change_60d",
         "quote.USD.percent_change_90d", "quote.USD.volume.change_24h", "quote.USD.volume_change_24h"
         ])

df = df.rename({"quote.USD.price": "price", "quote.USD.volume_24h": "24h_volume",
           "quote.USD.percent_change_24h": "pct_change_price_24h", "quote.USD.percent_change_7d" : "pct_change_price_7d",
           "quote.USD.percent_change_30d": "pct_change_price_30d", "quote.USD.market_cap_dominance": "market_cap_dominance"
           })


df = df.with_columns(
        pl.col("24h_volume").round(decimals = 2),
        pl.col("pct_change_price_24h").round(decimals = 2),
        pl.col("pct_change_price_7d").round(decimals = 2),
        pl.col("pct_change_price_30d").round(decimals = 2),
        pl.col("market_cap_dominance").round(decimals = 2),
        pl.col("price").round(decimals = 2)
    )
    

# my dictionary
data_dict = df.to_dict(as_series = False)

keys = []
values = []

def extract_key_value(dictionary):
    for key, value in dictionary.items():
        keys.append(key)
        values.append(value)
        
extract_key_value(data_dict)

