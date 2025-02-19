import requests
from bs4 import BeautifulSoup
import os
import json
import time
import logging
from typing import List, Dict, Set
from collections import defaultdict
import concurrent.futures
import threading
from requests.exceptions import Timeout, RequestException
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

class WebTextCrawlerWithCookies:
    def __init__(self, 
                 urls: List[str], 
                 cookies: List[Dict[str, str]], 
                 output_dir: str = "crawled_data", 
                 delay: float = 2.0,
                 timeout: int = 60,
                 max_workers: int = 2,
                 max_retries: int = 5):
        
        self.urls = urls
        self.cookies = cookies
        self.output_dir = output_dir
        self.delay = delay
        self.timeout = timeout
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.visited_urls: Set[str] = set()
        self.file_counter = 0
        self.lock = threading.Lock()  # スレッドロックの初期化を__init__で行う
        self.error_stats = defaultdict(int)

        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)
        
        os.makedirs(output_dir, exist_ok=True)
        self.setup_session()

    def setup_session(self):
        """セッションの設定とリトライ戦略の実装"""
        self.session = requests.Session()
        
        # リトライ戦略の設定
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # クッキーの設定
        for cookie in self.cookies:
            self.session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])

    def extract_text(self, url: str) -> List[str]:
        """URLからテキストを抽出（タイムアウト付き）"""
        try:
            response = self.session.get(
                url,
                headers={
                    'User-Agent': 'Custom Web Crawler',
                    'Accept-Charset': 'utf-8'
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for tag in soup(['script', 'style', 'nav', 'footer']):
                tag.decompose()
            
            texts = []
            for element in soup.find_all(['a', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div']):
                text = element.get_text().strip()
                if text:
                    texts.append(text)
                    
            return texts
            
        except Timeout:
            self.logger.error(f"Timeout occurred while processing {url}")
            raise
        except RequestException as e:
            self.logger.error(f"Request failed for {url}: {str(e)}")
            raise

    def save_text(self, url: str, texts: List[str]):
        """テキストを指定フォーマットでJSON形式で保存（スレッドセーフ）"""
        with self.lock:  # ロックを使用して排他制御
            filename = f"data{self.file_counter}.json"
            filepath = os.path.join(self.output_dir, filename)
            print(f"data{self.file_counter}")

            data = {
                'url': url,
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'success'
            }
            
            for i, text in enumerate(texts):
                data[f'text{i}'] = text
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Saved {len(texts)} texts to {filepath}")
            self.file_counter += 1

    def process_url(self, url: str) -> Dict:
        result = {
            'url': url,
            'success': False,
            'error': None,
            'error_type': None
        }
        
        try:
            self.logger.info(f"Processing: {url}")
            texts = self.extract_text(url)
            
            if texts:
                self.save_text(url, texts)
                result['success'] = True
                
            time.sleep(self.delay)
            
        except Timeout as e:
            error_msg = f"Timeout error for {url}: {str(e)}"
            self.logger.error(error_msg)
            result['error'] = error_msg
            result['error_type'] = 'timeout'
            with self.lock:
                self.error_stats['timeout'] += 1
                
        except RequestException as e:
            error_msg = f"Request error for {url}: {str(e)}"
            self.logger.error(error_msg)
            result['error'] = error_msg
            result['error_type'] = f'http_{e.response.status_code}' if hasattr(e, 'response') else 'request_error'
            with self.lock:
                self.error_stats[result['error_type']] += 1
                
        except Exception as e:
            error_msg = f"Unexpected error for {url}: {str(e)}"
            self.logger.error(error_msg)
            result['error'] = error_msg
            result['error_type'] = type(e).__name__
            with self.lock:
                self.error_stats['unexpected'] += 1
            
        return result

    def crawl(self):
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self.process_url, url): url for url in self.urls}
            
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    self.logger.error(f"Unexpected error for {url}: {str(e)}")
                    results.append({
                        'url': url,
                        'success': False,
                        'error': str(e),
                        'error_type': type(e).__name__
                    })
        
        # 詳細なクロール結果のサマリーを作成
        successful = sum(1 for r in results if r['success'])
        self.logger.info(f"Crawling completed. Success: {successful}/{len(self.urls)}")
        self.logger.info("Error statistics:")
        for error_type, count in self.error_stats.items():
            self.logger.info(f"  {error_type}: {count}")
        
        return results




if __name__ == "__main__":
    array_web_url = [
        "https://www.numazu-ct.ac.jp/",
        #"https://www.example.com",
    ]

    cookies = [
        {'name': 'example_cookie', 'value': 'example_value', 'domain': '.example.com'},
    ]

    crawler = WebTextCrawlerWithCookies(
        urls=array_web_url,
        cookies=cookies,
        output_dir="crawled_data",
        delay=0.001,
        timeout=30,
        max_workers=3,
        max_retries=3
    )

    results = crawler.crawl()