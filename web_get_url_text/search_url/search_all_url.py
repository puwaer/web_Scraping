import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin, urlparse
import time
from pathlib import Path
from datetime import datetime
import sys

class URLScraper:
    def __init__(self, base_url, file_name, delay_time=0.5, batch_size=5000, max_pages=None, 
                 progress_interval=60, stall_time=300):
        self.base_url = base_url
        self.file_name = file_name
        self.delay_time = delay_time
        self.batch_size = batch_size
        self.max_pages = max_pages
        self.progress_interval = progress_interval
        self.stall_time = stall_time
        self.base_domain = urlparse(base_url).netloc
        self.batch_count = 0
        
        self.stats = {
            'start_time': None,
            'last_progress_time': None,
            'last_url_increase_time': None,
            'last_url_count': 0,
            'total_urls': 0,
            'processed_pages': 0,
            'error_count': 0
        }
        
        self.data_dir = Path('data_url')
        self.data_dir.mkdir(exist_ok=True)

    def format_time_elapsed(self, seconds):
        if seconds is None:
            return "無限"
        
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        return f"{int(hours)}時間{int(minutes)}分{int(seconds)}秒"

    def check_stall_condition(self):
        current_time = time.time()
        if self.stats['total_urls'] > self.stats['last_url_count']:
            self.stats['last_url_increase_time'] = current_time
            self.stats['last_url_count'] = self.stats['total_urls']
            return False
        if self.stall_time and (current_time - self.stats['last_url_increase_time'] >= self.stall_time):
            stall_duration = self.format_time_elapsed(current_time - self.stats['last_url_increase_time'])
            print(f"\nURL数が{stall_duration}間増加していないため、収集を終了します。")
            return True
        return False

    def print_progress(self, force=False):
        current_time = time.time()
        if force or not self.stats['last_progress_time'] or \
           (current_time - self.stats['last_progress_time']) >= self.progress_interval:
            
            elapsed_time = current_time - self.stats['start_time']
            elapsed_str = self.format_time_elapsed(elapsed_time)
            urls_per_hour = self.stats['total_urls'] / (elapsed_time / 3600) if elapsed_time > 0 else 0
            time_since_last_increase = self.format_time_elapsed(
                current_time - self.stats['last_url_increase_time']
            )
            
            print("\n" + "="*50)
            print(f"進捗状況 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
            print(f"収集したURL数: {self.stats['total_urls']:,}")
            print(f"処理したページ数: {self.stats['processed_pages']:,}")
            print(f"エラー数: {self.stats['error_count']}")
            print(f"経過時間: {elapsed_str}")
            print(f"収集速度: {urls_per_hour:.1f} URLs/時")
            print(f"最後のURL増加から: {time_since_last_increase}")
            
            if self.max_pages:
                progress = (self.stats['processed_pages'] / self.max_pages) * 100
                print(f"全体の進捗: {progress:.1f}% ({self.stats['processed_pages']:,}/{self.max_pages:,})")
            
            print("="*50 + "\n")
            self.stats['last_progress_time'] = current_time
            sys.stdout.flush()

    def collect_urls(self):
        collected_urls = []
        urls_to_visit = [self.base_url]
        visited_urls = set()
        
        self.stats['start_time'] = time.time()
        self.stats['last_progress_time'] = time.time()
        self.stats['last_url_increase_time'] = time.time()
        
        print(f"収集を開始します: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"対象ドメイン: {self.base_domain}")
        if self.max_pages:
            print(f"最大ページ数: {self.max_pages:,}")
        else:
            print("ページ数制限: なし")
        print(f"バッチサイズ: {self.batch_size:,}")
        print(f"リクエスト間隔: {self.delay_time}秒")
        print(f"停滞判定時間: {self.format_time_elapsed(self.stall_time) if self.stall_time else '無限'}")
        
        while urls_to_visit:
            if self.max_pages and self.stats['processed_pages'] >= self.max_pages:
                print("\n最大ページ数に到達しました。収集を終了します。")
                break
            if self.check_stall_condition():
                break
            
            current_url = urls_to_visit.pop(0)
            if current_url in visited_urls:
                continue
            
            try:
                time.sleep(self.delay_time)
                
                response = requests.get(current_url)
                visited_urls.add(current_url)
                self.stats['processed_pages'] += 1
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        absolute_url = urljoin(current_url, href)
                        
                        if urlparse(absolute_url).netloc == self.base_domain:
                            if absolute_url not in visited_urls and absolute_url not in urls_to_visit:
                                urls_to_visit.append(absolute_url)
                                collected_urls.append(absolute_url)
                                self.stats['total_urls'] += 1
                                
                                if len(collected_urls) >= self.batch_size:
                                    self.save_to_json(collected_urls)
                                    self.batch_count += 1
                                    print(f"\nバッチ{self.batch_count}を保存しました。({len(collected_urls):,}個のURL)")
                                    collected_urls = []
                
                self.print_progress()

                if self.stats['total_urls'] == self.stats['processed_pages']:
                    print("\n収集したURL数と処理したページ数が同じため、収集を終了します。")
                    break
                                  
            except Exception as e:
                self.stats['error_count'] += 1
                print(f"\nError processing {current_url}: {e}")
            
        if collected_urls:
            self.save_to_json(collected_urls)
            self.batch_count += 1
            print(f"\nバッチ{self.batch_count}を保存しました。({len(collected_urls):,}個のURL)")
        
        self.merge_json_files()

    def save_to_json(self, urls):
        file_path = self.data_dir / f"{self.file_name}_{self.batch_count + 1}.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(urls, f, ensure_ascii=False, indent=4)
        print(f"ファイルに保存しました: {file_path}")
    
    def merge_json_files(self):
        total_urls = []
        for json_file in self.data_dir.glob("*.json"):
            with open(json_file, 'r', encoding='utf-8') as f:
                urls = json.load(f)
                total_urls.extend(urls)
        
        total_urls = list(set(total_urls))
        
        merged_file_path = self.data_dir / f"{self.file_name}_merged.json"
        with open(merged_file_path, 'w', encoding='utf-8') as f:
            json.dump(total_urls, f, ensure_ascii=False, indent=4)
        print(f"マージされたファイルに保存しました: {merged_file_path}")
        
        return len(total_urls)

    def run(self):
        """URL収集を開始するためのメソッド"""
        self.collect_urls()
        

def main():
    # スクレイピングの設定
    base_url='https://example.com',  # スクレイピングを開始するURL
    file_name='collected_urls',  # 出力するJSONファイル名
    delay_time=0.5,  # リクエスト間隔
    batch_size=5000,  # バッチサイズ
    max_pages=10000,  # 最大ページ数（Noneの場合は無制限）
    progress_interval=60,  # 進捗表示の間隔
    stall_time=300  # 停滞判定時間(Noneで無制限)

    # スクレイパーの作成と実行
    scraper = URLScraper(
        base_url=base_url,
        file_name=file_name,
        delay_time=delay_time,
        batch_size=batch_size,
        max_pages=max_pages,
        progress_interval=progress_interval,
        stall_time=stall_time
    )
    scraper.run()

if __name__ == "__main__":
    main()
