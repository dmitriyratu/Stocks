from playwright.async_api import async_playwright
from newspaper import Article
import trafilatura
from typing import Optional, Tuple
import asyncio
import nest_asyncio


class ContentScraper:

    MIN_WORD_COUNT = 50

    def __init__(self, logger):
        self.logger = logger
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
        }
        self.driver = None

    def _initialize_driver(self):
        """Initialize a WebDriver instance."""
        if not self.driver:
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--blink-settings=imagesEnabled=false')
            options.add_argument('window-size=1024x768')
            options.add_argument('--disable-extensions')
            options.add_argument(f'user-agent={self.headers["User-Agent"]}')
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)

    def _get_article_using_newspaper(self, url: str) -> Optional[str]:
        """
        Extract article content using the Newspaper library.
        """
        try:
            article = Article(url)
            article.download()
            article.parse()
            content = article.text.strip()
            return content if content else None
        except Exception as e:
            return None
        
    def _get_article_with_selenium(self, url: str) -> Optional[str]:
        """Extract article content using Selenium."""
        try:
            self._initialize_driver()
            self.driver.get(url)
            html = self.driver.page_source
            content = trafilatura.extract(html)
            return content if content else None
        except Exception as e:
            self.logger.error(f"Error fetching {url} with Selenium: {e}")
            return None

    def get_article_content(self, url: str) -> Optional[tuple]:
        """Extract content using fallback methods."""
        methods = [
            (self._get_article_using_newspaper, "newspaper"),
            (self._get_article_with_selenium, "selenium")
        ]
    
        for method, method_name in methods:
            content = method(url)
            if content and len(TextBlob(content).words) > self.MIN_WORD_COUNT:
                return method_name, content

        return None, None

    def close_driver(self):
        """Close the WebDriver instance."""
        if self.driver:
            self.driver.quit()

