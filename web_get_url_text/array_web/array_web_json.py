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
                 output_dir: str = "crawled_data",
                 delay: float = 1.0):
        self.urls = urls
        self.output_dir = output_dir
        self.delay = delay
        
        self.visited_urls: Set[str] = set()
        self.text_hashes: Set[str] = set()
        self.text_chunks: defaultdict = defaultdict(int)
        self.file_counter = 0
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)
        
        os.makedirs(output_dir, exist_ok=True)

    def compute_text_hash(self, text: str) -> str:
        return hashlib.md5(text.encode('utf-8')).hexdigest()

    def is_duplicate_content(self, texts: List[str]) -> bool:
        combined_text = ' '.join(texts)
        text_hash = self.compute_text_hash(combined_text)
        if text_hash in self.text_hashes:
            return True
            
        words = combined_text.split()
        chunk_size = 5
        
        for i in range(len(words) - chunk_size + 1):
            chunk = ' '.join(words[i:i + chunk_size])
            chunk_hash = self.compute_text_hash(chunk)
            if self.text_chunks[chunk_hash] > 2:
                return True
            
        for i in range(len(words) - chunk_size + 1):
            chunk = ' '.join(words[i:i + chunk_size])
            chunk_hash = self.compute_text_hash(chunk)
            self.text_chunks[chunk_hash] += 1
            
        self.text_hashes.add(text_hash)
        return False

    def extract_text(self, url: str) -> List[str]:
        """URLからテキストを抽出"""
        response = requests.get(
            url, 
            headers={
                'User-Agent': 'Custom Web Crawler',
                'Accept-Charset': 'utf-8'
            }
        )
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

    def save_text(self, url: str, texts: List[str]):
        """テキストをJSON形式で保存"""
        # 連番でファイル名を生成
        filename = f"data{self.file_counter}.json"
        filepath = os.path.join(self.output_dir, filename)
        
        # 指定された形式でデータを構築
        data = {'url': url}
        for i, text in enumerate(texts):
            data[f'text{i}'] = text
        
        # JSONファイルとして保存
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Saved {len(texts)} texts to {filepath}")
        self.file_counter += 1

    def process_url(self, url: str):
        """個別URLの処理"""
        try:
            self.logger.info(f"Processing: {url}")
            texts = self.extract_text(url)
            
            if texts and not self.is_duplicate_content(texts):
                self.save_text(url, texts)
                
            time.sleep(self.delay)
                
        except Exception as e:
            self.logger.error(f"Error processing {url}: {str(e)}")

    def crawl(self):
        """URLリストの処理"""
        for url in self.urls:
            self.process_url(url)
            
        self.logger.info(f"Crawling completed. Processed {len(self.urls)} URLs.")


array_web_url = [
    "https://www.numazu-ct.ac.jp/",
    #"https://www.numazu-ct.ac.jp/for-student/",
    #"https://www.numazu-ct.ac.jp/for-admission/",
    #"https://www.dlsite.com/index.html",
]


# 使用方法
if __name__ == "__main__":
    urls_to_crawl = array_web_url
    
    crawler = WebTextCrawler(
        urls=urls_to_crawl,
        output_dir="crawled_data",
        delay=0.5
    )

    crawler.crawl()