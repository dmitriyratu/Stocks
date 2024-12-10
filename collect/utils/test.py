import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import trafilatura
import cloudscraper
import time
import subprocess
import winreg
import time


# +
def get_chrome_version():
    # try:
    key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon")
    version = winreg.QueryValueEx(key, "version")[0]
    return int(version.split('.')[0])
    # except:
    #     return 119

get_chrome_version()


# +
class PowerScraper:
    def __init__(self, logger):
        
        def get_chrome_version():
            try:
                key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon")
                version = winreg.QueryValueEx(key, "version")[0].split('.')[0]
                self.logger.info(f"Scraper using Chrome version: {version}")
                return version
            except:
                default_version = 119
                self.logger.warning(f"Scraper failed to get Chrome version from registry, using default: {default_version}")
                return default_version 
        
        self.logger = logger
        
        options = uc.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        
        chrome_version = get_chrome_version()
        
        self.driver = uc.Chrome(
            version_main=chrome_version,
            options=options
        )

        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )

    def fetch_fast(self, url):
        """Fast attempt using cloudscraper"""
        try:
            response = self.scraper.get(url, timeout=10)
            if response.status_code == 200:
                return response.text
        except:
            return None

    def fetch_reliable(self, url):
        """Reliable attempt using undetected-chromedriver"""
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            return self.driver.page_source
        except:
            return None

def scrape(self, url):
    """Smart scraping with fallback"""
    methods = [
        ('cloudscraper', self.fetch_fast),
        ('selenium', self.fetch_reliable)
    ]
    
    for method_name, fetcher in methods:
        t0 = time.perf_counter()
        if content := fetcher(url):
            return (
                trafilatura.extract(content),
                method_name,
                time.perf_counter() - t0
            )
    
    return None, None, None

    def cleanup(self):
        if hasattr(self, 'driver'):
            self.driver.quit()
