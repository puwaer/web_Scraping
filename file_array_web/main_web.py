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

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

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
            {'name': 'example_name', 'value': 'example_value','domain': '.example.com'},
            {'name': 'example_name', 'value': 'example_value','domain': '.example.com'},
        ]
     
        crawler = WebTextCrawlerWithCookies(
            urls=filtered_urls,
            cookies=cookies,
            output_dir=output_dir,
            timeout=5,
            max_workers=min(psutil.cpu_count(logical=True), 4),  # ワーカー数を制限
            max_retries=4
        )
        
        crawler.crawl()
        logging.info(f"処理完了: {json_path}")
        
    except Exception as e:
        logging.error(f"ファイル処理中にエラーが発生: {json_path}, エラー: {str(e)}")
        raise

def process_all_json_files(input_directory: str, output_base_dir: str, num_processes: int = None):
    if not os.path.exists(input_directory):
        raise FileNotFoundError(f"入力ディレクトリが見つかりません: {input_directory}")
    
    os.makedirs(output_base_dir, exist_ok=True)
    json_files = glob.glob(os.path.join(input_directory, "*.json"))
    
    if not json_files:
        logging.warning(f"警告: {input_directory} にJSONファイルが見つかりませんでした。")
        return
    
    # プロセス数を制限
    max_workers = min(num_processes or psutil.cpu_count(logical=True), 4)
    logging.info(f"並行処理数: {max_workers}")
    
    failed_files = []
    successful_files = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_single_json, json_file, output_base_dir): json_file 
            for json_file in json_files
        }
        
        with tqdm(total=len(json_files), desc="Processing files") as pbar:
            for future in as_completed(future_to_file):
                json_file = future_to_file[future]
                try:
                    future.result(timeout=3600)  # 1時間のタイムアウト
                    successful_files.append(json_file)
                except TimeoutError:
                    logging.error(f"タイムアウト: {json_file}")
                    failed_files.append(json_file)
                except Exception as e:
                    logging.error(f"エラー発生 {json_file}: {str(e)}")
                    failed_files.append(json_file)
                finally:
                    pbar.update(1)
    
    # 処理結果のサマリーを表示
    logging.info(f"\n処理完了サマリー:")
    logging.info(f"成功: {len(successful_files)}")
    logging.info(f"失敗: {len(failed_files)}")
    
    if failed_files:
        logging.info("\n失敗したファイル:")
        for file in failed_files:
            logging.info(f"- {file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='High Performance Data Crawler')
    parser.add_argument('--input', '-i', default="./class_example_url", help='Input JSON files directory path')
    parser.add_argument('--output', '-o', default="example_data", help='Output base directory path')
    parser.add_argument('--single', '-s', help='Process single JSON file path')
    parser.add_argument('--processes', '-p', type=int, default=psutil.cpu_count(logical=True), help='Number of processes to use')
    
    args = parser.parse_args()
    
    start_time = time.time()
    
    try:
        if args.single:
            process_single_json(args.single, args.output)
        else:
            process_all_json_files(args.input, args.output, args.processes)
            
        elapsed_time = time.time() - start_time
        logging.info(f"\n総処理時間: {elapsed_time:.2f} 秒")
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.exit(1)