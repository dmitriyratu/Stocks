import time
from typing import Tuple, List, Optional, NamedTuple, Dict
from queue import Queue
import cloudscraper
from fake_useragent import UserAgent
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import undetected_chromedriver as uc
from logger_config import setup_logger
import winreg
from contextlib import contextmanager
import requests
from pathlib import Path
import pyprojroot
import multiprocessing
from textblob import TextBlob
import gc
import emoji
import re
from nltk.corpus import stopwords
import nltk
import dataclass.data_structures as ds
import trafilatura
import spacy
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import psutil


log_file = pyprojroot.here() / Path("logs/crypto_news.log")
logger = setup_logger("ScapeNewsURLs", log_file)


class SpamDetector:
    """Thread-safe utility for calculating spam scores from text."""
    
    PROMO_WORDS = frozenset({
        'bonus', 'free', 'offer', 'deposit', 'win', 'prize', 'reward',
        'exclusive', 'limited', 'special', 'click', 'subscribe',
        'guaranteed', 'instant', 'sale', 'game', '100%', 'code',
        'new', 'lucky', 'winner'
    })
    EMOJI_SET = frozenset(emoji.EMOJI_DATA.keys())
    EXCLAMATION_PATTERN = re.compile(r'!{2,}')
    STOP_WORDS = frozenset(stopwords.words('english'))
    
    @staticmethod
    def load_model():
        global nlp
        if 'nlp' not in globals():
            nlp = spacy.load("en_core_web_sm")

    @staticmethod
    def get_score(text: str) -> float:
        
        SpamDetector.load_model()

        doc = nlp(text)
        words = [
            token.lemma_.lower()
            for token in doc if token.text.lower() not in SpamDetector.STOP_WORDS
        ]
        total_words = len(words) or 1
        emoji_count = len(set(text) & SpamDetector.EMOJI_SET)
        promo_count = sum(word in SpamDetector.PROMO_WORDS for word in words)

        scores = {
            'emoji': min(emoji_count / (total_words * 0.1), 1.0),
            'promo': min(promo_count / (total_words * 0.25), 1.0),
            'exclamations': min(len(SpamDetector.EXCLAMATION_PATTERN.findall(text)) / 3, 1.0)
        }
        return min(sum(scores.values()) / 2, 1)


class WebDriverPool:
    """A thread-safe pool for managing Selenium WebDriver instances."""

    def __init__(self, size: int, chrome_version: int):
        self.size = size
        self.chrome_version = chrome_version
        self.pool = Queue(maxsize=size)
        self.lock = threading.Lock()
        self.driver_pids = []


    def _create_driver(self) -> uc.Chrome:
        """Create and configure a Chrome WebDriver instance."""
        options = uc.ChromeOptions()
        for arg in [
            '--headless=new', 
            '--disable-gpu', 
            '--no-sandbox',
            '--disable-dev-shm-usage', 
            '--disable-blink-features=AutomationControlled',
            '--start-maximized',
            '--disable-features=ScriptStreaming',
            '--blink-settings=imagesEnabled=false',
            f'--user-agent={UserAgent().random}', 
        ]:
            options.add_argument(arg)

        options.set_capability('timeouts', {
            'implicit': ds.TimeoutConfig.IMPLICIT_WAIT * 1000,
            'pageLoad': ds.TimeoutConfig.PAGE_LOAD * 1000,
            'script': ds.TimeoutConfig.SCRIPT * 1000
        })

        driver = uc.Chrome(version_main=self.chrome_version, options=options)

        # PID Tracking
        pid = driver.service.process.pid
        self.driver_pids.append(pid)
        logger.info(f"Spawned Chrome PID: {pid}")
        
        # Set timeouts using dynamic configurations
        driver.set_page_load_timeout(ds.TimeoutConfig.PAGE_LOAD)
        driver.set_script_timeout(ds.TimeoutConfig.SCRIPT)
        driver.implicitly_wait(ds.TimeoutConfig.IMPLICIT_WAIT)

        # Bypass detection by defining webdriver as undefined
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        return driver

    @contextmanager
    def driver_context(self):
        driver = None
        with self.lock:
            if self.pool.empty():
                driver = self._create_driver()
            else:
                driver = self.pool.get(timeout=30)
                try:
                    driver.current_url
                except:
                    driver.quit()
                    driver = self._create_driver()
        try:
            yield driver
        finally:
            with self.lock:
                if driver:
                    try:
                        if not self.pool.full():
                            driver.switch_to.default_content() 
                            self.pool.put(driver)
                        else:
                            driver.quit()
                    except:
                        driver.quit()
    
    def cleanup(self):
        """Close and clean up all WebDriver instances."""
        logger.info(f"Starting cleanup - Pool size: {self.pool.qsize()}")
        
        while not self.pool.empty():
            driver = self.pool.get_nowait()
            try:
                session_id = driver.session_id
                logger.info(f"Cleaning up driver session: {session_id}")
                driver.quit()
            except Exception as e:
                logger.error(f"Error in driver cleanup: {e}")
    
        for pid in self.driver_pids:
            if psutil.pid_exists(pid):
                proc = psutil.Process(pid)
                logger.info(f"Killing Chrome PID: {pid}")
                try:
                    proc.kill()
                    proc.wait(timeout=5)
                except psutil.TimeoutExpired:
                    logger.error(f"Failed to kill Chrome PID: {pid}")
                except psutil.NoSuchProcess:
                    logger.info(f"Chrome PID {pid} already gone")
        
        self.driver_pids.clear()


# +
class ScrapingResult(NamedTuple):
    news_url: str
    full_text: Optional[str]
    full_word_count: Optional[int]
    scrape_method: Optional[str]
    elapsed_time: Optional[float]
    scrape_status: str
    corrupted_text: Optional[str]
    spam_score: Optional[float]

class PowerScraper:
    """High-performance scraper with persistent reusable resources."""

    DEFAULT_CHROME_VERSION = 119
    MIN_TEXT_LENGTH = 50
    MAX_SPAM_SCORE = 0.2
    SPAM_CHECK_LENGTH_THRESHOLD = 200

    RETRY_PATTERNS = [
        r"verifying you are human",
        r"please verify you are a human",
        r"captcha required",
    ]
    
    EXIT_PATTERNS = [
        r"free subscription for",
        r"page not found",
    ]

    RETRY_PROMPTS = [re.compile(p, re.IGNORECASE) for p in RETRY_PATTERNS]
    EXIT_PROMPTS = [re.compile(p, re.IGNORECASE) for p in EXIT_PATTERNS]
    
    def __init__(self):

        self.chrome_version = self.get_chrome_version()
        available_memory = psutil.virtual_memory().available / (1024 * 1024)
        self.pool_size = min(int(available_memory // 500), 8)       
        self._init_resources()

    def _init_resources(self) -> None:
        """Initialize WebDriver pool and Cloudscraper."""
        self.driver_pool = WebDriverPool(self.pool_size, self.chrome_version)
        self.scraper, self.headers = self.setup_cloudscraper()

    def get_chrome_version(self) -> int:
        """Detect the installed Chrome version dynamically."""
        try:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon")
            version = winreg.QueryValueEx(key, "version")[0].split(".")[0]
            return int(version)
        except (WindowsError, ValueError):
            logger.warning(f'Failed to get Chrome version from registry: {str(e)}')

        try:
            response = requests.get("https://chromedriver.storage.googleapis.com/LATEST_RELEASE", timeout=10)
            return int(response.text.strip().split(".")[0])
        except (requests.RequestException, ValueError):
            logger.warning(f'Failed to get Chrome version from web: {str(e)}')

        logger.info(f'Falling back to default Chrome version: {self.DEFAULT_CHROME_VERSION}')
        return self.DEFAULT_CHROME_VERSION

    def setup_cloudscraper(self) -> Tuple[cloudscraper.CloudScraper, Dict[str, str]]:
        """Set up Cloudscraper with connection pooling and dynamic headers."""


        browser_config = {
            'browser': 'chrome',
            'platform': 'windows',
            'mobile': False,
            'desktop': True,
            'version': self.chrome_version, 
        }
    
        scraper = cloudscraper.create_scraper(
            browser=browser_config,
            interpreter='nodejs',
            allow_brotli=True,
            doubleDown=True,
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

    def _fetch_with_cloudscraper(self, url: str) -> Tuple[Optional[str], str]:
        response = self.scraper.get(
            url,
            headers=self.headers,
            timeout=(ds.TimeoutConfig.CONNECTION, ds.TimeoutConfig.READ)
        )
        if response.status_code != 200:
            return None, 'failed to fetch'
        
        response.encoding = response.apparent_encoding

        if response.text:
            return response.text, 'success'
        
        return None, 'empty article'

    def _fetch_with_webdriver(self, url: str) -> Tuple[Optional[str], str]:
        """Fetch webpage content using Selenium WebDriver with bot detection handling."""
        
        CONTENT_SELECTORS = 'article, .article-content, .post-content, main, body'
        
        def wait_for_content(driver: uc.Chrome) -> None:
            WebDriverWait(driver, ds.TimeoutConfig.PAGE_LOAD).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, CONTENT_SELECTORS))
            )
        
        def handle_bot_detection(driver: uc.Chrome) -> Optional[str]:
            page_source = driver.page_source.lower()
            if any(r.search(page_source) for r in self.RETRY_PROMPTS):
                time.sleep(ds.TimeoutConfig.VERIFICATION_SLEEP)
                driver.refresh()
                wait_for_content(driver)
                return driver.page_source.lower()
            return None
        
        def extract_content(driver: uc.Chrome) -> str:
            try:
                article = driver.find_element(By.CSS_SELECTOR, CONTENT_SELECTORS)
                return article.get_attribute('outerHTML')
            except:
                return driver.page_source
        
        with self.driver_pool.driver_context() as driver:
            try:
                driver.get(url)
                wait_for_content(driver)
                
                if updated_source := handle_bot_detection(driver):
                    if any(r.search(updated_source) for r in self.RETRY_PROMPTS):
                        return None, 'retries failed'
                
                return extract_content(driver), 'success'
                
            except (TimeoutException, Exception) as e:
                logger.error(f"Scraping failed for {url}: {str(e)}")
                return None, 'timeout'
        
    
    def fetch(self, url: str, reliable: bool = False) -> Tuple[Optional[str], str]:
        """Fetch content using Cloudscraper or WebDriver."""
        try:
            if reliable:
                return self._fetch_with_webdriver(url)
            return self._fetch_with_cloudscraper(url)
        except Exception as e:
            logger.error(f"Fetch error for {url} (reliable={reliable}): {e}")
            return None, str(e)


    def scrape_url(self, url: str) -> ScrapingResult:
        """
        Smart scraping with fast method fallback to reliable method.
        Returns a ScrapingResult containing the scraped text and metadata.
        """

        methods = [
            ("fast", False),
            # ("reliable", True)
        ]
        
        error_msg = None
        for method, reliable in methods:
            try:
                start_time = time.perf_counter()
                content, status = self.fetch(url, reliable=reliable)
                elapsed_time = time.perf_counter() - start_time
    
                if not content:
                    continue
    
                extracted_text = trafilatura.extract(
                    content,
                    include_comments=False,
                    include_tables=False,
                    include_links=False,
                    no_fallback=True,
                )
                
                if not extracted_text:
                    continue
                    
                word_count = len(TextBlob(extracted_text).words)
    
                if word_count < self.MIN_TEXT_LENGTH:
                    return ScrapingResult(
                        news_url=url,
                        full_text=None,
                        full_word_count=word_count,
                        scrape_method=method,
                        elapsed_time=elapsed_time,
                        scrape_status=status,
                        corrupted_text=extracted_text,
                        spam_score = None,
                    )
    
                spam_score = SpamDetector.get_score(extracted_text)
                
                if (
                    word_count < self.SPAM_CHECK_LENGTH_THRESHOLD and
                    (spam_score > self.MAX_SPAM_SCORE or
                    any(p.search(extracted_text.lower()) for p in self.EXIT_PROMPTS))
                ):
                    return ScrapingResult(
                        news_url=url,
                        full_text=None,
                        full_word_count=word_count,
                        scrape_method=method,
                        elapsed_time=elapsed_time,
                        scrape_status=status,
                        corrupted_text=extracted_text,
                        spam_score = spam_score,
                    )
                
                return ScrapingResult(
                    news_url=url,
                    full_text=extracted_text,
                    full_word_count=word_count,
                    scrape_method=method,
                    elapsed_time=elapsed_time,
                    scrape_status=status,
                    corrupted_text=None,
                    spam_score = spam_score,
                )
                            
            except Exception as e:
                error_type = type(e).__name__
                status = str(e)
                logger.error(f"Scraping error for {url} using {method}: {error_type} - {status}")
                continue
                
        return ScrapingResult(
            news_url=url,
            full_text=None,
            full_word_count=None,
            scrape_method=None,
            elapsed_time=None,
            scrape_status=status,
            corrupted_text=None,
            spam_score = None,
        )


    def scrape_urls(self, urls: List[str]) -> List[ScrapingResult]:
        
        with ThreadPoolExecutor(max_workers=self.driver_pool.size) as executor:
            results = list(executor.map(self.scrape_url, urls))
        
        return results
    
    def refresh_resources(self) -> None:
        """Safely refresh all scraping resources."""
        self.close()
        try:
            self._init_resources()
        except Exception as e:
            logger.error(f"Failed to refresh resources: {e}")
            raise


    def close(self) -> None:
        """Close all resources gracefully."""
        if self.scraper:
            try:
                self.scraper.close()
            except Exception as e:
                logger.debug(f"Error closing scraper: {e}")
            finally:
                self.scraper = None
                
        if self.driver_pool:
            try:
                self.driver_pool.cleanup()
            except Exception as e:
                logger.debug(f"Error cleaning up driver pool: {e}")
            finally:
                self.driver_pool = None
    
        gc.collect()
        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
