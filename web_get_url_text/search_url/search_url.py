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
        """
        URLスクレイパーの初期化
        
        Args:
            base_url: スクレイピングを開始するURL
            file_name: 出力するJSONファイルの名前（拡張子なし）
            delay_time: リクエスト間の待機時間（秒）
            batch_size: 一度に出力するURLの数
            max_pages: 収集する最大ページ数（Noneの場合は無制限）
            progress_interval: 進捗状況を表示する間隔（秒）
            stall_time: URL数が変化しない場合に終了するまでの時間（秒）
        """
        self.base_url = base_url
        self.file_name = file_name
        self.delay_time = delay_time
        self.batch_size = batch_size
        self.max_pages = max_pages
        self.progress_interval = progress_interval
        self.stall_time = stall_time
        self.base_domain = urlparse(base_url).netloc
        self.batch_count = 0
        
        # 統計情報の初期化
        self.stats = {
            'start_time': None,
            'last_progress_time': None,
            'last_url_increase_time': None,
            'last_url_count': 0,
            'total_urls': 0,
            'processed_pages': 0,
            'error_count': 0
        }
        
        # data_urlディレクトリの作成
        self.data_dir = Path('data_url')
        self.data_dir.mkdir(exist_ok=True)

    def format_time_elapsed(self, seconds):
        """経過時間を見やすい形式にフォーマット"""
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        return f"{int(hours)}時間{int(minutes)}分{int(seconds)}秒"

    def check_stall_condition(self):
        """URL収集が停滞しているかチェック"""
        current_time = time.time()
        
        # URL数が増加した場合、最終増加時刻を更新
        if self.stats['total_urls'] > self.stats['last_url_count']:
            self.stats['last_url_increase_time'] = current_time
            self.stats['last_url_count'] = self.stats['total_urls']
            return False
        
        # URL数が一定時間変化していない場合
        if current_time - self.stats['last_url_increase_time'] >= self.stall_time:
            stall_duration = self.format_time_elapsed(current_time - self.stats['last_url_increase_time'])
            print(f"\nURL数が{stall_duration}間増加していないため、収集を終了します。")
            return True
            
        return False

    def print_progress(self, force=False):
        """進捗状況を表示"""
        current_time = time.time()
        
        # 強制表示または最後の表示から指定時間が経過している場合に表示
        if force or not self.stats['last_progress_time'] or \
           (current_time - self.stats['last_progress_time']) >= self.progress_interval:
            
            elapsed_time = current_time - self.stats['start_time']
            elapsed_str = self.format_time_elapsed(elapsed_time)
            
            # URLの収集速度を計算（1時間あたり）
            urls_per_hour = self.stats['total_urls'] / (elapsed_time / 3600) if elapsed_time > 0 else 0
            
            # 最後のURL増加からの経過時間
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
        """指定されたベースURLから始まるURLを収集する"""
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
        print(f"停滞判定時間: {self.format_time_elapsed(self.stall_time)}")
        
        while urls_to_visit:
            # 最大ページ数チェック
            if self.max_pages and self.stats['processed_pages'] >= self.max_pages:
                print("\n最大ページ数に到達しました。収集を終了します。")
                break
                
            # URL増加停滞チェック
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
                                
                                # バッチサイズに達したら保存
                                if len(collected_urls) >= self.batch_size:
                                    self.save_to_json(collected_urls)
                                    self.batch_count += 1
                                    print(f"\nバッチ{self.batch_count}を保存しました。({len(collected_urls):,}個のURL)")
                                    collected_urls = []
                
                self.print_progress()
                                
            except Exception as e:
                self.stats['error_count'] += 1
                print(f"\nError processing {current_url}: {str(e)}")
                continue
        
        # 残りのURLを保存
        if collected_urls:
            self.save_to_json(collected_urls)
            self.batch_count += 1
            print(f"\n最終バッチ{self.batch_count}を保存しました。({len(collected_urls):,}個のURL)")

    
    def run(self):
        """スクレイピングプロセスを実行"""
        # URL収集
        self.collect_urls()
        
        # 最終進捗状況の表示
        self.print_progress(force=True)
        
        # 結果のマージと集計
        total_urls = self.merge_json_files()
        
        # 実行結果のサマリーを表示
        elapsed_time = time.time() - self.stats['start_time']
        print("\n" + "="*50)
        print("収集完了")
        print(f"総URL数: {total_urls:,}")
        print(f"処理したページ数: {self.stats['processed_pages']:,}")
        print(f"エラー数: {self.stats['error_count']}")
        print(f"合計実行時間: {self.format_time_elapsed(elapsed_time)}")
        print(f"最終ファイル: {self.data_dir / f'{self.file_name}_merged.json'}")
        print("="*50)

    def save_to_json(self, urls):
        """URLリストをJSONファイルとして保存"""
        output_file = self.data_dir / f"{self.file_name}_batch_{self.batch_count}.json"
        
        # URLリストを辞書形式に変換
        url_dict = {f"url{i}": url for i, url in enumerate(urls)}
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(url_dict, f, ensure_ascii=False, indent=2)


    def merge_json_files(self):
        """バッチファイルをマージして1つのJSONファイルにする"""
        all_urls = []
        
        # すべてのバッチファイルを読み込み
        for i in range(self.batch_count):
            batch_file = self.data_dir / f"{self.file_name}_batch_{i}.json"
            if batch_file.exists():
                with open(batch_file, 'r', encoding='utf-8') as f:
                    urls = json.load(f)
                    all_urls.extend(urls.values())
        
        # 重複を削除
        unique_urls = list(set(all_urls))
        
        # 辞書形式に変換
        url_dict = {f"url{i}": url for i, url in enumerate(unique_urls)}
        
        # マージされたファイルを保存
        output_file = self.data_dir / f"{self.file_name}_merged.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(url_dict, f, ensure_ascii=False, indent=2)
            
        return len(unique_urls)



def main():
    # スクレイピングの設定
    base_url = "https://www.numazu-ct.ac.jp/"
    file_name = "kosen"
    delay_time = 0.01
    batch_size = 500
    max_pages = None  # None for unlimited, or set a number like 1000
    progress_interval = 10  # 進捗表示の間隔（秒）
    stall_time = 60  # URL増加が止まってから終了するまでの時間（秒）
    
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