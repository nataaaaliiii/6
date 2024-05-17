import json
import pandas as pd
from datetime import datetime
from pandas.core.api import DataFrame as DataFrame
from baseloader import BaseDataLoader
from enum import Enum

class Granularity(Enum):
    ONE_MINUTE = 60
    FIVE_MINUTES = 300
    FIFTEEN_MINUTES = 900
    ONE_HOUR = 3600
    SIX_HOURS = 21600
    ONE_DAY = 86400

class CoinbaseLoader(BaseDataLoader):

    def __init__(self, endpoint="https://api.exchange.coinbase.com"):
        super().__init__(endpoint)

    def get_pairs(self) -> pd.DataFrame:
        try:
            data = self._get_req("/products")
            df = pd.DataFrame(json.loads(data))
            df.set_index('id', drop=True, inplace=True)
            return df
        except Exception as e:
            print(f"Error fetching pairs: {e}")
            return pd.DataFrame()

    def get_stats(self, pair: str) -> pd.DataFrame:
        try:
            data = self._get_req(f"/products/{pair}")
            return pd.DataFrame(json.loads(data), index=[0])
        except Exception as e:
            print(f"Error fetching stats for pair {pair}: {e}")
            return pd.DataFrame()

    def get_historical_data(self, pair: str, begin: str, end: str, granularity: Granularity) -> DataFrame:
        try:
            begin_dt = datetime.strptime(begin, "%Y-%m-%d")
            end_dt = datetime.strptime(end, "%Y-%m-%d")
            params = {
                "start": begin_dt.timestamp(),
                "end": end_dt.timestamp(),
                "granularity": granularity.value
            }
            data = self._get_req("/products/" + pair + "/candles", params)
            df = pd.DataFrame(json.loads(data),
                              columns=("timestamp", "low", "high", "open", "close", "volume"))
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')  # Convert timestamp to datetime
            df.set_index('timestamp', drop=True, inplace=True)
            return df
        except Exception as e:
            print(f"Error fetching historical data for pair {pair}: {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    loader = CoinbaseLoader()
    
    pairs = loader.get_pairs()
    print(pairs)
    
    pair_stats = loader.get_stats("btc-usdt")
    print(pair_stats)
    
