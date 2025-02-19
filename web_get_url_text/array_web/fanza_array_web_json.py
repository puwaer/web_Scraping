import requests
from bs4 import BeautifulSoup
import os
import time
import logging
from typing import Set, List, Optional, Dict
import json
import hashlib
from collections import defaultdict

class WebTextCrawler:
    def __init__(self, 
                 urls: List[str],
                 cushion_urls: Optional[Dict[str, str]] = None,
                 output_dir: str = "crawled_data",
                 delay: float = 1.0):
        self.urls = urls
        self.cushion_urls = cushion_urls or {}
        self.output_dir = output_dir
        self.delay = delay
        self.session = requests.Session()
        self.file_counter = 0  # ファイル名用のカウンター
        
        self.headers = {
            'User-Agent': 'Custom Web Crawler',
            'Accept-Charset': 'utf-8'
        }
        self.session.headers.update(self.headers)
        
        self.visited_urls: Set[str] = set()
        self.text_hashes: Dict[str, Set[str]] = defaultdict(set)
        self.text_chunks: Dict[str, defaultdict] = defaultdict(lambda: defaultdict(int))
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)
        
        os.makedirs(output_dir, exist_ok=True)

    def compute_text_hash(self, text: str) -> str:
        return hashlib.md5(text.encode('utf-8')).hexdigest()

    def is_duplicate_content(self, url: str, texts: List[str]) -> bool:
        combined_text = ' '.join(texts)
        text_hash = self.compute_text_hash(combined_text)
        
        if text_hash in self.text_hashes[url]:
            return True
            
        words = combined_text.split()
        chunk_size = 5
        
        for i in range(len(words) - chunk_size + 1):
            chunk = ' '.join(words[i:i + chunk_size])
            chunk_hash = self.compute_text_hash(chunk)
            if self.text_chunks[url][chunk_hash] > 2:
                return True
            
        for i in range(len(words) - chunk_size + 1):
            chunk = ' '.join(words[i:i + chunk_size])
            chunk_hash = self.compute_text_hash(chunk)
            self.text_chunks[url][chunk_hash] += 1
            
        self.text_hashes[url].add(text_hash)
        return False

    def handle_cushion_page(self, url: str):
        if url in self.cushion_urls:
            try:
                cushion_url = self.cushion_urls[url]
                self.logger.info(f"Accessing cushion page: {cushion_url}")
                
                response = self.session.get(cushion_url)
                response.raise_for_status()
                
                time.sleep(self.delay)
                
            except Exception as e:
                self.logger.error(f"Error handling cushion page for {url}: {str(e)}")
                raise

    def extract_text(self, url: str) -> List[str]:
        self.handle_cushion_page(url)
        
        response = self.session.get(url)
        response.encoding = 'utf-8'
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        for tag in soup(['script', 'style', 'nav', 'footer']):
            tag.decompose()
        
        texts = []
        for element in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div']):
            text = element.get_text().strip()
            text = text.encode('utf-8', errors='ignore').decode('utf-8')
            if text:
                text = ' '.join(text.split())
                if len(text) > 0:
                    texts.append(text)
                
        return texts

    def save_text(self, url: str, texts: List[str]) -> bool:
        """テキストをJSON形式で保存（シンプルな連番ファイル名を使用）"""
        try:
            filename = f"data{self.file_counter}.json"
            filepath = os.path.join(self.output_dir, filename)
            
            data = {
                'url': url,
                'texts': {},
                'crawled_at': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            for i, text in enumerate(texts):
                data['texts'][f'text{i}'] = text
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Saved {len(texts)} texts from {url} to {filename}")
            self.file_counter += 1  # カウンターをインクリメント
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving text for {url}: {str(e)}")
            return False

    def process_url(self, url: str) -> bool:
        """個別URLの処理"""
        try:
            self.logger.info(f"Processing: {url}")
            texts = self.extract_text(url)
            
            if texts and not self.is_duplicate_content(url, texts):
                return self.save_text(url, texts)
                
            time.sleep(self.delay)
            return False
                
        except Exception as e:
            self.logger.error(f"Error processing {url}: {str(e)}")
            return False

    def crawl(self):
        """URLリストの処理"""
        successful_urls = 0
        for url in self.urls:
            if url not in self.visited_urls:
                if self.process_url(url):
                    successful_urls += 1
                self.visited_urls.add(url)
            
        self.logger.info(f"Crawling completed. Successfully processed {successful_urls} out of {len(self.urls)} URLs.")



# 使用例
if __name__ == "__main__":
    array_web_url = [
        "https://www.dmm.co.jp/digital/",
        "https://www.dmm.co.jp/digital/videoa/-/detail/=/cid=h_1350kamef00032/?i3_ref=list&i3_ord=2"
    ]

    cushion_pages = {
        "https://www.dmm.co.jp/digital/": "https://www.dmm.co.jp/age_check/=/declared=yes/?rurl=https%3A%2F%2Fwww.dmm.co.jp%2Ftop%2F",
        "https://www.dmm.co.jp/digital/videoa/-/detail/=/cid=h_1350kamef00032/?i3_ref=list&i3_ord=2": "https://www.dmm.co.jp/age_check/=/declared=yes/?rurl=https%3A%2F%2Fwww.dmm.co.jp%2Ftop%2F",
    }

    crawler = WebTextCrawler(
        urls=array_web_url,
        cushion_urls=cushion_pages,
        output_dir="crawled_data",
        delay=0.1
    )

    crawler.crawl()