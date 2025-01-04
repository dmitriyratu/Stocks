from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict
import cloudscraper
import psutil
from dataclasses import dataclass
import queue
from fake_useragent import UserAgent
from requests.adapters import HTTPAdapter
import requests
import trafilatura
import winreg
import pyprojroot
from pathlib import Path
import random
import time
from http import HTTPStatus

from logger_config import setup_logger

log_file = pyprojroot.here() / Path("logs/crypto_news.log")
logger = setup_logger("ScapeNewsURLs", log_file)


@dataclass
class ScrapingResult:
    """Result container for a scraping attempt"""
    news_url: str
    full_text: Optional[str] = None
    status_code: Optional[int] = None
    error: Optional[str] = None
    success: bool = False
    elapsed_time: float = 0


class PowerScraper:
    
    DEFAULT_CHROME_VERSION = 119 
    DEFAULT_TIMEOUT = 15

    def __init__(self):
        """Initialize scraper with dynamic worker count based on available memory"""
        self.chrome_version = self._get_chrome_version()
        available_memory = psutil.virtual_memory().available / (1024 * 1024)
        self.max_workers = min(int(available_memory // 500), 16)
        
        self.scraper_pool = queue.Queue()
        self._initialize_scraper_pool()

        self.headers = self._get_headers()

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

        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True,
                'version': self.chrome_version
            },
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
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
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
        return 
    
    def scrape_url(self, news_url: str) -> ScrapingResult:
        """Scrape a single URL and return the result"""
        start_time = time.perf_counter()
        scraper = self.scraper_pool.get()
        result = ScrapingResult(news_url=news_url)
        
        try:
            response = scraper.get(news_url, headers=self.headers, timeout=self.DEFAULT_TIMEOUT)

            response.raise_for_status()

            scraper.cookies.update(response.cookies)     
            
            response.encoding = response.apparent_encoding
            result.status_code = response.status_code
            
            if response.text:
                result.full_text = trafilatura.extract(
                    response.text,
                    include_comments=False,
                    include_tables=False,
                    include_links=False,
                    no_fallback=False,
                    
                )
                result.success = bool(result.full_text)
            else:
                result.error = "empty response"
            
        except requests.exceptions.HTTPError as e:
            result.error = str(e) 
            result.status_code = e.response.status_code
        except requests.exceptions.RequestException as e:
            result.error = str(e)
            result.status_code = 503
        except Exception as e:
            result.error = str(e)
            result.status_code = 500            
        finally:
            result.elapsed_time = time.perf_counter() - start_time
            self.scraper_pool.put(scraper)
            
        return result


    def scrape_urls(self, urls: List[str]) -> List[ScrapingResult]:
        """Scrape URLs maintaining input order with per-URL timeout"""
        results = [ScrapingResult(news_url=url) for url in urls]
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.scrape_url, url): i for i, url in enumerate(urls)}
            for future in as_completed(futures):
                try:
                    results[futures[future]] = future.result(timeout=self.DEFAULT_TIMEOUT)
                except Exception as e:
                    results[futures[future]].error = f"Error: {str(e)}"
        
        return results

    def __enter__(self): return self
        
    def __exit__(self, *_): self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        while not self.scraper_pool.empty():
            self.scraper_pool.get().close()
