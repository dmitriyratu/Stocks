# +
import pandas as pd
import os
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path
from tqdm.autonotebook import tqdm

from logger_config import setup_logger


# -

class GetCryptoPrices:

    TICKERS = [
        "ETH-USD", "SOL-USD", "BNB-USD", "XRP-USD", "DOGE-USD",
        "ADA-USD", "TON-USD", "AVAX-USD", "DOT-USD", "LINK-USD",
        "BTC-USD"
    ]

    TIME_WINDOW_DAYS = 365 * 2
    
    def __init__(self):
        
        self.data_dir = Path("../data/crypto_prices")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        log_file = Path("../logs/crypto_prices.log")
        self.logger = setup_logger("GetCryptoPrices", log_file)
        
    def store_crypto_prices(self):

        end_date = pd.Timestamp.now()

        for ticker in tqdm(self.TICKERS, desc="Processing tickers"):
            
            self.logger.info(f"Processing data for {ticker}...")
            file_path = self.data_dir / f"{ticker}.parquet"
            
            # Load existing data or initialize start date
            if os.path.exists(file_path):
                existing_data = pd.read_parquet(file_path)
                max_date = pd.to_datetime(existing_data.index.max())
            
                if max_date >= end_date.floor('D'):
                    self.logger.info(f"Data for {ticker} is already up-to-date.")
                    continue
            
                start_date = max_date + timedelta(days=1)
                self.logger.info(f"Fetching new data for {ticker} from {start_date} to {end_date}...")
            else:
                # Explicitly handle the case where no file exists
                self.logger.info(f"No existing data for {ticker}. Initializing fresh download.")
                existing_data = None
  
            # Fetch data and update
            try:
                start_date = end_date - timedelta(days=self.TIME_WINDOW_DAYS)
                
                new_data = yf.download(ticker, start=start_date, end=end_date, interval="1d", multi_level_index = False, progress = False)
                new_data.columns = ['_'.join(c.lower().split() + [ticker]) for c in new_data]
                
                if new_data.empty:
                    self.logger.info(f"No new data fetched for {ticker}. Skipping update.")
                    continue
                
                if existing_data is not None:
                    updated_data = pd.concat([existing_data, new_data])
                    updated_data = updated_data[~updated_data.index.duplicated(keep='last')]
                else:
                    updated_data = new_data
                
                updated_data.to_parquet(file_path)
                self.logger.info(f"Data for {ticker} saved to {file_path}.")
            except Exception as e:
                self.logger.error(f"Error fetching data for {ticker}: {e}")
                
    def fetch_crypto_prices(self, ticker):
        
        file_path = self.data_dir / f"{ticker}-USD.parquet"
        if not file_path.exists():
            self.logger.warning(f"No data found for {ticker}. Returning an empty DataFrame.")
            return pd.DataFrame()
        return pd.read_parquet(file_path)


cls = GetCryptoPrices()

cls.store_crypto_prices()

# Fetch the data
cls.fetch_crypto_prices('ETH')
