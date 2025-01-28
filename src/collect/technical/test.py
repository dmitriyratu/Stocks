# +
import yfinance as yf

btc = yf.Ticker("BTC-USD")

# Get hourly data
df = btc.history(period="1d", interval="1h")
# -

df.columns
