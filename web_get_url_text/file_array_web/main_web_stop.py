from array_web_json import WebTextCrawlerWithCookies
import os
import json
import psutil
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from tqdm import tqdm
import glob
import logging
import time
import sys
import pickle
from datetime import datetime

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class CrawlerState:
    def __init__(self, input_directory, output_base_dir):
        self.input_directory = input_directory
        self.output_base_dir = output_base_dir
        self.completed_files = set()
        self.failed_files = set()
        self.state_file = os.path.join(output_base_dir, 'crawler_state.pkl')
        self.load_state()

    def load_state(self):
        """状態ファイルから進捗を読み込む"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'rb') as f:
                    state = pickle.load(f)
                    self.completed_files = state.get('completed_files', set())
                    self.failed_files = state.get('failed_files', set())
                logging.info(f"既存の進捗を読み込みました: 完了 {len(self.completed_files)}件, 失敗 {len(self.failed_files)}件")
            except Exception as e:
                logging.error(f"進捗ファイルの読み込みに失敗: {str(e)}")

    def save_state(self):
        """現在の進捗を状態ファイルに保存"""
        try:
            state = {
                'completed_files': self.completed_files,
                'failed_files': self.failed_files,
                'last_update': datetime.now().isoformat()
            }
            with open(self.state_file, 'wb') as f:
                pickle.dump(state, f)
            logging.info("進捗を保存しました")
        except Exception as e:
            logging.error(f"進捗の保存に失敗: {str(e)}")

    def mark_completed(self, file_path):
        """ファイルを完了としてマーク"""
        self.completed_files.add(file_path)
        self.failed_files.discard(file_path)
        self.save_state()

    def mark_failed(self, file_path):
        """ファイルを失敗としてマーク"""
        self.failed_files.add(file_path)
        self.completed_files.discard(file_path)
        self.save_state()

def load_json(file_path):
    """JSONファイルを読み込む関数"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        logging.error(f"JSONファイルの読み込みに失敗: {file_path}, エラー: {str(e)}")
        return []

def process_single_json(json_path, output_base_dir):
    try:
        base_name = os.path.splitext(os.path.basename(json_path))[0]
        output_dir = os.path.join(output_base_dir, base_name)
        os.makedirs(output_dir, exist_ok=True)
        
        logging.info(f"処理開始: {json_path}")
        array_web_url = load_json(json_path)
        
        if not array_web_url:
            logging.warning(f"URLが見つかりません: {json_path}")
            return
            
        filtered_urls = [url for url in array_web_url if not url.lower().endswith('.pdf')]
        
        if not filtered_urls:
            logging.warning(f"処理可能なURLが見つかりません: {json_path}")
            return

        cookies = [
            {'name': 'OptanonConsent', 
             'value': 'isGpcEnabled=0&datestamp=Fri+Oct+25+2024+22%3A19%3A15+GMT%2B0900+(%E6%97%A5%E6%9C%AC%E6%A8%99%E6%BA%96%E6%99%82)&version=6.23.0&isIABGlobal=false&hosts=&consentId=88957a0d-9fc8-4ddf-b6be-b107a12edb47&interactionCount=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0003%3A1%2CC0002%3A1%2CC0001%3A1&AwaitingReconsent=false',
             'domain': '.dlsite.com'},
            {'name': 'age_check_done', 'value': '1', 'domain': '.dmm.co.jp'},
        ]
     
        crawler = WebTextCrawlerWithCookies(
            urls=filtered_urls,
            cookies=cookies,
            output_dir=output_dir,
            timeout=5,
            max_workers=min(psutil.cpu_count(logical=True), 4),
            max_retries=4
        )
        
        crawler.crawl()
        logging.info(f"処理完了: {json_path}")
        return True
        
    except Exception as e:
        logging.error(f"ファイル処理中にエラーが発生: {json_path}, エラー: {str(e)}")
        raise

def process_all_json_files(input_directory: str, output_base_dir: str, num_processes: int = None, resume: bool = True):
    if not os.path.exists(input_directory):
        raise FileNotFoundError(f"入力ディレクトリが見つかりません: {input_directory}")
    
    os.makedirs(output_base_dir, exist_ok=True)
    json_files = glob.glob(os.path.join(input_directory, "*.json"))
    
    if not json_files:
        logging.warning(f"警告: {input_directory} にJSONファイルが見つかりませんでした。")
        return

    # 状態管理オブジェクトの初期化
    state = CrawlerState(input_directory, output_base_dir)
    
    # 処理するファイルの選択
    if resume:
        # 未完了または失敗したファイルのみを処理
        files_to_process = [f for f in json_files if f not in state.completed_files]
        if state.failed_files:
            logging.info(f"失敗したファイル {len(state.failed_files)}件 を再処理します")
    else:
        files_to_process = json_files

    if not files_to_process:
        logging.info("処理すべきファイルがありません")
        return
    
    # プロセス数を制限
    max_workers = min(num_processes or psutil.cpu_count(logical=True), 4)
    logging.info(f"並行処理数: {max_workers}")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_single_json, json_file, output_base_dir): json_file 
            for json_file in files_to_process
        }
        
        try:
            with tqdm(total=len(files_to_process), desc="Processing files") as pbar:
                for future in as_completed(future_to_file):
                    json_file = future_to_file[future]
                    try:
                        future.result(timeout=3600)  # 1時間のタイムアウト
                        state.mark_completed(json_file)
                    except TimeoutError:
                        logging.error(f"タイムアウト: {json_file}")
                        state.mark_failed(json_file)
                    except Exception as e:
                        logging.error(f"エラー発生 {json_file}: {str(e)}")
                        state.mark_failed(json_file)
                    finally:
                        pbar.update(1)
        except KeyboardInterrupt:
            logging.info("\n処理を中断します。進捗は保存されています。")
            executor.shutdown(wait=False)
            sys.exit(1)
    
    # 処理結果のサマリーを表示
    logging.info(f"\n処理完了サマリー:")
    logging.info(f"成功: {len(state.completed_files)}")
    logging.info(f"失敗: {len(state.failed_files)}")
    
    if state.failed_files:
        logging.info("\n失敗したファイル:")
        for file in state.failed_files:
            logging.info(f"- {file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='High Performance Data Crawler')
    parser.add_argument('--input', '-i', default="./class_kosen_url", help='Input JSON files directory path')
    parser.add_argument('--output', '-o', default="kosen_data", help='Output base directory path')
    parser.add_argument('--single', '-s', help='Process single JSON file path')
    parser.add_argument('--processes', '-p', type=int, default=psutil.cpu_count(logical=True), help='Number of processes to use')
    parser.add_argument('--no-resume', action='store_true', help='Do not resume from previous state')
    
    args = parser.parse_args()
    
    start_time = time.time()
    
    try:
        if args.single:
            process_single_json(args.single, args.output)
        else:
            process_all_json_files(args.input, args.output, args.processes, not args.no_resume)
            
        elapsed_time = time.time() - start_time
        logging.info(f"\n総処理時間: {elapsed_time:.2f} 秒")
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.exit(1)