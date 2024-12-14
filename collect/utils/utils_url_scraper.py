import time
import random
from typing import Tuple, List, Optional
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Lock
import cloudscraper
from fake_useragent import UserAgent
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import trafilatura
from logger_config import setup_logger
import winreg
from contextlib import contextmanager
import requests
from pathlib import Path
import pyprojroot
import multiprocessing
import dataclass.data_structures as ds
from tqdm.notebook import tqdm
from textblob import TextBlob

log_file = pyprojroot.here() / Path("logs/crypto_news.log")
logger = setup_logger("ScapeNewsURLs", log_file)


class WebDriverPool:
    """A thread-safe pool for managing Selenium WebDriver instances."""
    def __init__(self, size: int, chrome_version: int):
        self.size = size
        self.pool = Queue(maxsize=size)
        self.lock = Lock()
        for _ in range(size):
            self.pool.put(self._create_driver(chrome_version))

    def _create_driver(self, chrome_version: int):
        options = uc.ChromeOptions()
        
        # Basic required options
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        # Anti-detection options
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument(f'--user-agent={UserAgent().random}')
        options.add_argument('--enable-cookies')
        options.add_argument('--enable-javascript')
        options.add_argument('--start-maximized')
        
        driver = uc.Chrome(version_main=chrome_version, options=options)
        
        # Set timeouts using centralized config
        driver.set_page_load_timeout(ds.TimeoutConfig.PAGE_LOAD)
        driver.set_script_timeout(ds.TimeoutConfig.SCRIPT)
        driver.implicitly_wait(ds.TimeoutConfig.IMPLICIT_WAIT)
        
        driver.command_executor._conn.timeout = ds.TimeoutConfig.COMMAND
        driver.command_executor._conn.read_timeout = ds.TimeoutConfig.COMMAND
        
        driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
        
        return driver

    def get_driver(self):
        with self.lock:
            return self.pool.get()

    def release_driver(self, driver):
        with self.lock:
            self.pool.put(driver)

    def cleanup(self):
        while not self.pool.empty():
            driver = self.pool.get_nowait()
            driver.quit()

    @contextmanager
    def driver_context(self):
        driver = self.get_driver()
        try:
            yield driver
        finally:
            self.release_driver(driver)


class PowerScraper:
    """High-performance scraper with persistent reusable resources."""

    RETRY_PROMPTS = [
        r"verifying you are human",
        r"please verify you are a human",
        r"captcha required",
    ]


    def __init__(self):
        self.chrome_version = self.get_chrome_version()
        self.driver_pool = WebDriverPool(
            size = min(multiprocessing.cpu_count() * 2, 8), 
            chrome_version=self.chrome_version
        )
        self.scraper, self.headers = self.setup_cloudscraper()

        
    def get_chrome_version(self) -> int:
        """Detect the installed Chrome version dynamically."""
        try:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon")
            version = winreg.QueryValueEx(key, "version")[0].split(".")[0]
            logger.info(f'Using winreg Chrome Version {version}')
            return int(version)
        except Exception as e:
            pass

        try:
            response = requests.get("https://chromedriver.storage.googleapis.com/LATEST_RELEASE", timeout=10)
            version = response.text.strip().split(".")[0]
            logger.info(f'Using Web Chrome Version {version}')            
            return int(version)
        except Exception as e:
            version = 119
            logger.info(f'Using Web Chrome Version {version}') 
            return int(version)

    def setup_cloudscraper(self) -> Tuple[cloudscraper.CloudScraper, dict]:
        """Set up Cloudscraper with connection pooling and dynamic headers."""
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False,
                'desktop': True,
            },
        )
        adapter = requests.adapters.HTTPAdapter(pool_connections=self.driver_pool.size, pool_maxsize=self.driver_pool.size)
        scraper.mount('http://', adapter)
        scraper.mount('https://', adapter)

        headers = {
            'User-Agent': UserAgent().random,
            'sec-ch-ua': f'"Google Chrome";v="{self.chrome_version}"',
            'sec-ch-ua-platform': 'Windows',
            'sec-ch-ua-mobile': '?0',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',            
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
        }
        return scraper, headers

    def fetch(self, url: str, reliable: bool = False) -> Optional[str]:
        """Fetch content using Cloudscraper or WebDriver."""
        
        try:
            
            text = None
            if not reliable:
                response = self.scraper.get(
                    url, headers=self.headers, 
                    timeout=(ds.TimeoutConfig.CONNECTION, ds.TimeoutConfig.READ)
                )
                if response.status_code == 200:
                    response.encoding = response.apparent_encoding
                    text = response.text
            else:
                with self.driver_pool.driver_context() as driver:
                    driver.get(url)                 
                    if any(rp in driver.page_source.lower() for rp in self.RETRY_PROMPTS):
                        time.sleep(ds.TimeoutConfig.VERIFICATION_SLEEP)
                        WebDriverWait(driver, ds.TimeoutConfig.VERIFICATION_WAIT).until(
                            EC.presence_of_element_located((By.XPATH, '//body'))
                        )
                    text = driver.page_source
                    if any(fp in text.lower() for fp in self.RETRY_PROMPTS):
                        text = None

            return text
            
        except Exception as e:
            logger.debug(f"Fetch error for {url} (reliable={reliable}): {e}")
            return None


    def scrape_url(self, url: str) -> Tuple[str, Optional[str], Optional[str], Optional[float]]:
        """Smart scraping with fast method fallback to reliable method."""
        for method, reliable in [("fast", False), ("reliable", True)]:
            try:
                t0 = time.perf_counter()
                content = self.fetch(url, reliable=reliable)
                elapsed_time = time.perf_counter() - t0
                if content:
                    text = trafilatura.extract(content)
                    text_size = len(TextBlob(text).words)
                    if text_size >= 25:
                        return url, text, text_size, method, elapsed_time
            except Exception as e:
                logger.debug(f"Scraping error for {url} using {method}: {e}")
    
        return url, None, None, None, None

    def scrape_urls(self, urls: List[str]) -> List[Tuple[Optional[str], Optional[str], Optional[float]]]:
        """Scrape multiple URLs in parallel with progress tracking."""
        with ThreadPoolExecutor(max_workers=self.driver_pool.size) as executor:
            return list(executor.map(self.scrape_url, urls))

    def refresh_driver_pool(self):
        """Refresh the WebDriver pool."""
        self.driver_pool.cleanup()
        self.driver_pool = WebDriverPool(size=self.driver_pool.size, chrome_version=self.chrome_version)        
    def close(self):
        self.driver_pool.cleanup()
