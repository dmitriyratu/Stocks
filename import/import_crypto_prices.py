from pycoingecko import CoinGeckoAPI
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px


class collect_crypto_info:
    
    def __init__(self):    
        self.cg = CoinGeckoAPI()

    def import_prices(self, crypto:str) -> pd.DataFrame:
        
        data = self.cg.get_coin_market_chart_by_id(id=crypto, vs_currency='usd', days=365)
        prices = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
        prices['timestamp'] = pd.to_datetime(prices['timestamp'], unit='ms')

        return prices


cls = collect_crypto_info()

prices = cls.import_prices('bitcoin')

prices.shape

fig = px.line(prices, x='timestamp', y='price', title='Cryptocurrency Price Over Time', width = 800)
fig.show()

prices.to_parquet('../data/bitcoin_raw.parquet')


