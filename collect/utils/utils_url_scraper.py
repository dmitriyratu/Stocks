from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict
import cloudscraper
import psutil
from dataclasses import dataclass
import queue
from fake_useragent import UserAgent
from requests.adapters import HTTPAdapter
import requests
from urllib3.util import Retry
import trafilatura
import winreg
import pyprojroot
from pathlib import Path
import random
from bs4 import BeautifulSoup

from logger_config import setup_logger
import dataclass.data_structures as ds

log_file = pyprojroot.here() / Path("logs/crypto_news.log")
logger = setup_logger("ScapeNewsURLs", log_file)


@dataclass
class ScrapingResult:
    """Result container for a scraping attempt"""
    url: str
    content: Optional[str] = None
    status_code: Optional[int] = None
    error: Optional[str] = None
    success: bool = False


class PowerScraper:
    DEFAULT_CHROME_VERSION = 119 

    def __init__(self):
        """Initialize scraper with dynamic worker count based on available memory"""
        self.chrome_version = self._get_chrome_version()
        available_memory = psutil.virtual_memory().available / (1024 * 1024)
        self.max_workers = min(int(available_memory // 500), 16)
        
        self.scraper_pool = queue.Queue()
        self._initialize_scraper_pool()

    def _get_chrome_version(self) -> int:
        """Get Chrome version or fallback to default"""
        try:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon")
            version = winreg.QueryValueEx(key, "version")[0].split(".")[0]
            return int(version)
        except:
            return self.DEFAULT_CHROME_VERSION

    def _create_single_scraper(self) -> cloudscraper.CloudScraper:
        """Create a configured scraper instance with retry logic"""

        browser_config = {
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True,
            'version': self.chrome_version
        }

        scraper = cloudscraper.create_scraper(
            browser=browser_config,
            interpreter='nodejs',
            doubleDown=True,
            allow_brotli=False,
            delay=10,
        )

        return scraper

    def _get_headers(self) -> Dict[str, str]:
        """Get enhanced headers that better mimic real browsers"""

        user_agent = UserAgent().chrome
        accept_encodings = random.choice(["gzip, deflate", "gzip"])
        languages = random.choice(["en-US,en;q=0.9", "en-GB,en;q=0.8", "en,en;q=0.7"])

        headers = {
            'User-Agent': user_agent,
            'sec-ch-ua': f'"Google Chrome";v="{self.chrome_version}", "Not;A=Brand";v="99"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding': accept_encodings,
            'Accept-Language': languages,
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

        return headers

    def _initialize_scraper_pool(self):
        """Fill the scraper pool"""
        for _ in range(self.max_workers):
            self.scraper_pool.put(self._create_single_scraper())

    def _extract_text(self, html: str) -> str:
        """Extract clean text from HTML"""
        return trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False,
            include_links=False,
            no_fallback=False,
        )
    
    def scrape_url(self, url: str) -> ScrapingResult:
        """Scrape a single URL and return the result"""
        scraper = self.scraper_pool.get()
        result = ScrapingResult(url=url)
        
        try:
            response = scraper.get(url, headers=self._get_headers(), timeout=15)

            response.raise_for_status()

            # Persist session cookies dynamically
            scraper.cookies.update(response.cookies)     
            
            # Extract content and update result
            response.encoding = response.apparent_encoding
            result.status_code = response.status_code
            
            if response.text:
                result.content = self._extract_text(response.text)
                result.success = bool(result.content)
            else:
                result.error = "empty response"
            
        except Exception as e:
            result.error = str(e)
            
        finally:
            self.scraper_pool.put(scraper)
            
        return result


    # def scrape_urls(self, urls: List[str]) -> List[ScrapingResult]:
    #     """Scrape multiple URLs, maintaining input order"""
    #     try:
    #         with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
    #             results = list(executor.map(self.scrape_url, urls))
    #         return results
    #     finally:
    #         while not self.scraper_pool.empty():
    #             scraper = self.scraper_pool.get()
    #             scraper.close()
    #         self._initialize_scraper_pool()

    def scrape_urls(self, urls: List[str]) -> List[ScrapingResult]:
        """Scrape URLs maintaining input order with per-URL timeout"""
        results = [ScrapingResult(url=url) for url in urls]
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.scrape_url, url): i for i, url in enumerate(urls)}
            for future in as_completed(futures):
                try:
                    results[futures[future]] = future.result(timeout=15)
                except (TimeoutError, Exception) as e:
                    results[futures[future]].error = f"Error: {str(e)}"
        
        return results

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        while not self.scraper_pool.empty():
            scraper = self.scraper_pool.get()
            scraper.close()
