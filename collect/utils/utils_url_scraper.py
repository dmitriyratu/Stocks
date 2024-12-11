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
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--enable-features=NetworkService,NetworkServiceInProcess')
        options.add_argument('--log-level=3')

        driver = uc.Chrome(version_main=chrome_version, options=options)
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
    TIMEOUT_SECONDS = 15
    CONNECTION_TIMEOUT = 5
    READ_TIMEOUT = 10

    def __init__(self):
        self.chrome_version = self.get_chrome_version()
        self.scraper, self.headers = self.setup_cloudscraper()
        self.driver_pool = WebDriverPool(size = 5, chrome_version=self.chrome_version)

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
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        scraper.mount('http://', adapter)
        scraper.mount('https://', adapter)

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
            'sec-ch-ua': f'"Google Chrome";v="{self.chrome_version}"',
            'sec-ch-ua-platform': 'Windows',
            'sec-ch-ua-mobile': '?0',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'User-Agent': UserAgent().random,
        }
        return scraper, headers


    def fetch_fast(self, url: str) -> Optional[str]:
        """Quickly fetch content using Cloudscraper."""
        try:
            response = self.scraper.get(url, headers=self.headers, timeout=(self.CONNECTION_TIMEOUT, self.READ_TIMEOUT))
            if response.status_code == 200:
                response.encoding = response.apparent_encoding
                return response.text
        except Exception as e:
            logger.error(f"Cloudscraper error for {url}: {e}")
        return None

    def fetch_reliable(self, url: str) -> Optional[str]:
        """Fetch content reliably using Selenium WebDriver."""
        with self.driver_pool.driver_context() as driver:
            try:
                driver.get(url)
                WebDriverWait(driver, self.TIMEOUT_SECONDS).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                return driver.page_source
            except Exception as e:
                logger.error(f"Selenium error for {url}: {e}")
            return None

    def scrape(self, url: str) -> Tuple[Optional[str], Optional[str], Optional[float]]:
        """Smart scraping with Cloudscraper fallback to Selenium."""
        try:
            t0 = time.perf_counter()
            content = self.fetch_fast(url)
            if content:
                return trafilatura.extract(content), "cloudscraper", time.perf_counter() - t0

            t0 = time.perf_counter()
            content = self.fetch_reliable(url)
            if content:
                return trafilatura.extract(content), "selenium", time.perf_counter() - t0

        except Exception as e:
            logger.error(f"Scraping error for {url}: {e}")
        return None, None, None

    def close(self):
        self.driver_pool.cleanup()


def parallel_scrape(urls: List[str], max_workers: int = 10) -> List[Tuple[Optional[str], Optional[str], Optional[float]]]:
    """Scrape multiple URLs in parallel."""
    scraper = PowerScraper()
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(scraper.scrape, urls))
    finally:
        scraper.close()


