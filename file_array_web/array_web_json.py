import requests
from bs4 import BeautifulSoup
import os
import json
import time
import logging
from typing import List, Dict, Set
from collections import defaultdict
import threading
from requests.exceptions import Timeout, RequestException
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil


class WebTextCrawlerWithCookies:
    def __init__(self, 
                 urls: List[str], 
                 cookies: List[Dict[str, str]], 
                 output_dir: str = "crawled_data", 
                 timeout: int = 60,
                 max_workers: int = None,
                 max_retries: int = 5):
        
        self.urls = urls
        self.cookies = cookies
        self.output_dir = output_dir
        self.timeout = timeout
        self.max_workers = max_workers or psutil.cpu_count(logical=True)
        self.max_retries = max_retries
        self.visited_urls: Set[str] = set()
        self.file_counter = 0
        self.error_stats = defaultdict(int)
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        os.makedirs(output_dir, exist_ok=True)
        self.setup_session()
        self.lock = threading.Lock()

    def setup_session(self):
        self.session = requests.Session()
        
        retry_strategy = Retry(total=self.max_retries, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        for cookie in self.cookies:
            self.session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])

    def extract_text(self, url: str) -> List[str]:
        try:
            response = self.session.get(url, headers={'User-Agent': 'Custom Web Crawler', 'Accept-Charset': 'utf-8'}, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            for tag in soup(['script', 'style', 'nav', 'footer']):
                tag.decompose()
            
            texts = [element.get_text().strip() for element in soup.find_all(['a', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div']) if element.get_text().strip()]
            return texts
            
        except (Timeout, RequestException) as e:
            self.logger.error(f"Error for {url}: {str(e)}")
            raise

    def save_text(self, url: str, texts: List[str]):
        with self.lock:
            filename = f"data{self.file_counter}.json"
            filepath = os.path.join(self.output_dir, filename)
            data = {'url': url, 'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'), 'status': 'success', **{f'text{i}': text for i, text in enumerate(texts)}}
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.file_counter += 1

    def process_url(self, url: str) -> Dict:
        try:
            texts = self.extract_text(url)
            if texts:
                self.save_text(url, texts)
                return {'url': url, 'success': True}
        except (Timeout, RequestException) as e:
            return {'url': url, 'success': False, 'error': str(e)}

    def crawl(self):
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.process_url, url): url for url in self.urls}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Crawling"):
                result = future.result()
                if not result['success']:
                    self.error_stats[result['error']] += 1
        self.logger.info(f"Crawling completed with error stats: {dict(self.error_stats)}")