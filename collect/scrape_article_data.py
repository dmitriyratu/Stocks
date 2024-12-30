import pandas as pd
from pathlib import Path
from more_itertools import chunked
from tqdm.notebook import tqdm
from logger_config import setup_logger
import pyprojroot
from deltalake import DeltaTable


from collect.utils.utils_url_scraper import PowerScraper
from collect.utils.utils_news_persist import persist_news

# # Import News Data

base_path = pyprojroot.here() / Path('data/news/BTC/raw_data/')
dt = DeltaTable(str(base_path))
filters = [
    ('year_utc', '=', 2024),
    ('month_utc', '=', 12),
    ('day_utc', '>=', 20),
]
news_metadata = dt.to_pyarrow_table(filters=filters).to_pandas()

# # Scrape Article Text

# +
urls = pd.unique(news_metadata['news_url']).tolist()
url_chunks = list(chunked(urls, 100))
all_results = []
total_success = 0

with tqdm(total=len(urls)) as pbar:
    for chunk in url_chunks:
        with PowerScraper() as scraper:
            results = scraper.scrape_urls(chunk)
            all_results.extend(results)

            chunk_success = sum(1 for r in results if r.success)
            total_success += chunk_success
            
            pbar.update(len(chunk))
            pbar.set_postfix(
                chunk=f"{chunk_success}/{len(chunk)}",
                total=f"{total_success}/{len(all_results)} ({total_success/len(all_results)*100:.1f}%)"
            )
# -
df = pd.DataFrame(all_results)







df['error'].dropna().str.split(':').map(lambda x: x[0]).value_counts()





# +
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests


def test_proxy(proxy):
    test_url = 'https://httpbin.org/ip'
    try:
        response = requests.get(
            test_url, proxies={'http': proxy, 'https': proxy}, timeout=5
        )
        if response.status_code == 200:
            print(f"✅ Proxy Working: {proxy} - IP: {response.json()['origin']}")
            return proxy
    except Exception as e:
        print(f"❌ Proxy Failed: {proxy} - {str(e)}")
    return None

# Validate proxies in parallel
def validate_proxies(proxies, target):
    working_proxies = []
    stop_flag = False

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_proxy = {executor.submit(test_proxy, proxy): proxy for proxy in proxies}

        for future in as_completed(future_to_proxy):
            if stop_flag:
                future.cancel()
                continue
            
            proxy = future.result()
            if proxy:
                working_proxies.append(proxy)
                
            if len(working_proxies) >= target:
                print(f"\n🎯 Target of {target} working proxies reached. Stopping early.")
                return working_proxies
    
    print(f"\n🔍 Found {len(working_proxies)} working proxies.")
    return working_proxies


# -

working_proxy_list = validate_proxies(proxy_list, 10)

len(working_proxy_list)

len(validate_proxies(working_proxy_list, 10))

# ### Persist Data

persist_news(news_articles, path = 'data/news/BTC/scraped_data')


