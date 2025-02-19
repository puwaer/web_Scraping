import requests
from bs4 import BeautifulSoup
import os
import json
import time
import logging
from typing import List, Dict, Set
from collections import defaultdict

class WebTextCrawlerWithCookies:
    def __init__(self, 
                 urls: List[str], 
                 cookies: List[Dict[str, str]], 
                 output_dir: str = "crawled_data", 
                 delay: float = 1.0):
        self.urls = urls
        self.cookies = cookies
        self.output_dir = output_dir
        self.delay = delay
        self.visited_urls: Set[str] = set()
        self.file_counter = 0
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)
        
        os.makedirs(output_dir, exist_ok=True)
        self.session = requests.Session()
        
        for cookie in cookies:
            self.session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])

    def extract_text(self, url: str) -> List[str]:
        """URLからテキストを抽出"""
        response = self.session.get(
            url,
            headers={
                'User-Agent': 'Custom Web Crawler',
                'Accept-Charset': 'utf-8'
            }
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

    def save_text(self, url: str, texts: List[str]):
        """テキストを指定フォーマットでJSON形式で保存"""
        filename = f"data{self.file_counter}.json"
        filepath = os.path.join(self.output_dir, filename)
        
        # 新しいフォーマットでデータを構造化
        data = {'url': url}
        for i, text in enumerate(texts):
            data[f'text{i}'] = text
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Saved {len(texts)} texts to {filepath}")
        self.file_counter += 1

    def process_url(self, url: str):
        """個別URLの処理"""
        try:
            self.logger.info(f"Processing: {url}")
            texts = self.extract_text(url)
            
            if texts:
                self.save_text(url, texts)
                
            time.sleep(self.delay)
                
        except Exception as e:
            self.logger.error(f"Error processing {url}: {str(e)}")

    def crawl(self):
        """URLリストの処理"""
        for url in self.urls:
            self.process_url(url)
            
        self.logger.info("Crawling completed.")

# URLと対応するクッキー情報
array_web_url = [
    "https://www.dlsite.com/index.html",
    "https://www.example.com",
]

cookies = [
    {'name': 'age_check', 'value': 'confirmed', 'domain': '.dlsite.com'},
    {'name': 'example_cookie', 'value': 'example_value', 'domain': '.example.com'},
]

if __name__ == "__main__":
    crawler = WebTextCrawlerWithCookies(
        urls=array_web_url,
        cookies=cookies,
        output_dir="crawled_data",
        delay=0.5
    )

    crawler.crawl()